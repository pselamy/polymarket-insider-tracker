"""Assemble a real ``Pipeline`` with only its external boundaries replaced."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Sequence

from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.alerter.dispatcher import AlertChannel, AlertDispatcher
from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.alerter.history import AlertHistory
from polymarket_insider_tracker.config import (
    DatabaseSettings,
    DetectorSettings,
    DiscordSettings,
    PolygonSettings,
    PolymarketSettings,
    RedisSettings,
    Settings,
    TelegramSettings,
)
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.scorer import RiskScorer
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.metadata_sync import (
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_REDIS_KEY_PREFIX,
    MarketMetadataSync,
    SyncState,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState, TradePoller
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.funding import FundingTracer
from polymarket_insider_tracker.storage.database import DatabaseManager
from tests.fakes.clob import FakeBaseClobClient
from tests.fakes.gamma import FakeGammaClient
from tests.fakes.trades import FakeClock, FakeTradesServer
from tests.fakes.web3 import FakeAsyncWeb3, FakeEth


def make_test_settings(
    *,
    dry_run: bool = False,
    persist_assessments: bool = True,
    alert_threshold: float = 0.8,
    discord_enabled: bool = False,
    telegram_enabled: bool = False,
    discord_webhook_url: str | None = None,
    telegram_bot_token: str | None = None,
    telegram_chat_id: str | None = None,
    trades_url: str = "https://data-api.polymarket.com/trades",
    trades_coverage: str = "all",
    trades_poll_interval_seconds: int = 5,
    trades_recovery_horizon_seconds: int = 600,
    health_port: int = 8080,
) -> Settings:
    """Create a fully validated ``Settings`` value without reading the environment."""
    return Settings(
        _env_file=None,
        redis=RedisSettings(_env_file=None, REDIS_URL="redis://localhost:6379/0"),
        database=DatabaseSettings(
            _env_file=None, DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/db"
        ),
        polygon=PolygonSettings(
            _env_file=None,
            POLYGON_RPC_URL="https://polygon-rpc.com",
            POLYGON_FALLBACK_RPC_URL=None,
        ),
        polymarket=PolymarketSettings(
            _env_file=None,
            POLYMARKET_TRADES_URL=trades_url,
            POLYMARKET_TRADES_COVERAGE=trades_coverage,
            POLYMARKET_TRADES_POLL_INTERVAL_SECONDS=trades_poll_interval_seconds,
            POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS=trades_recovery_horizon_seconds,
            POLYMARKET_WS_URL=None,
            POLYMARKET_API_KEY=None,
        ),
        discord=DiscordSettings(
            _env_file=None,
            DISCORD_WEBHOOK_URL=discord_webhook_url if discord_enabled else None,
        ),
        telegram=TelegramSettings(
            _env_file=None,
            TELEGRAM_BOT_TOKEN=telegram_bot_token if telegram_enabled else None,
            TELEGRAM_CHAT_ID=telegram_chat_id if telegram_enabled else None,
        ),
        # Detector fields are declared by alias with ``extra="ignore"``; the field names
        # themselves would be silently dropped, so the aliases must be used here.
        detector=DetectorSettings(
            _env_file=None,
            DETECTOR_PERSIST_ASSESSMENTS=persist_assessments,
            DETECTOR_ALERT_THRESHOLD=alert_threshold,
            DETECTOR_DEDUP_WINDOW_SECONDS=3600,
        ),
        LOG_LEVEL="INFO",
        HEALTH_PORT=health_port,
        DRY_RUN=dry_run,
    )


async def wire_pipeline(
    settings: Settings,
    *,
    redis: FakeAsyncRedis,
    eth: FakeEth,
    market: MarketMetadata | None = None,
    db_manager: DatabaseManager | None = None,
    channels: Sequence[AlertChannel] = (),
    trades: FakeTradesServer | None = None,
    poll_clock: FakeClock | None = None,
    crawl_gate: threading.Event | None = None,
    state_log: list[str] | None = None,
) -> Pipeline:
    """Build a ``Pipeline`` the way ``Pipeline._initialize_components`` does.

    Every product component is real: the Polygon client, wallet analyzer, funding tracer,
    metadata sync, both detectors, the risk scorer, the alert formatter, the dispatcher, and,
    when ``trades`` is given, the trade poller. Only the boundaries differ: Redis is ``fakeredis``,
    the Polygon RPC is ``eth``, the CLOB SDK is ``FakeBaseClobClient`` (held behind ``crawl_gate``
    when given), gamma statistics are ``FakeGammaClient``, the trades query is ``trades`` on
    ``poll_clock``, market metadata is pre-cached in
    Redis the way a completed sync leaves it, and delivery goes to ``channels`` instead of Discord
    or Telegram. ``state_log`` records poller and metadata state transitions in order.
    """
    pipeline = Pipeline(settings)
    pipeline._redis = redis
    pipeline._db_manager = db_manager
    log = state_log if state_log is not None else []

    polygon_client = PolygonClient(
        settings.polygon.rpc_url,
        redis=redis,
        max_requests_per_second=10_000.0,
        retry_delay_seconds=0.0,
    )
    polygon_client._w3 = FakeAsyncWeb3(eth)
    wallet_analyzer = WalletAnalyzer(polygon_client, redis=redis)
    pipeline._funding_tracer = FundingTracer(polygon_client)

    clob_client = ClobClient()
    base_client = FakeBaseClobClient()
    base_client.crawl_gate = crawl_gate
    clob_client._client = base_client
    pipeline._metadata_sync = MarketMetadataSync(
        redis=redis,
        clob_client=clob_client,
        gamma_client=FakeGammaClient(),
        on_state_change=lambda state: log.append(f"metadata:{state.value}"),
    )
    if market is not None:
        await redis.setex(
            f"{DEFAULT_REDIS_KEY_PREFIX}{market.condition_id}",
            DEFAULT_CACHE_TTL_SECONDS,
            json.dumps(market.to_dict()),
        )

    pipeline._fresh_wallet_detector = FreshWalletDetector(wallet_analyzer)
    pipeline._size_anomaly_detector = SizeAnomalyDetector(pipeline._metadata_sync)
    pipeline._risk_scorer = RiskScorer(
        redis,
        alert_threshold=settings.detector.alert_threshold,
        dedup_window_seconds=settings.detector.dedup_window_seconds,
    )
    pipeline._alert_formatter = AlertFormatter(verbosity="detailed")
    pipeline._alert_history = AlertHistory(
        redis, dedup_window_seconds=settings.detector.dedup_window_seconds
    )
    pipeline._alert_dispatcher = AlertDispatcher(
        list(channels), history=pipeline._alert_history, dry_run=pipeline._dry_run
    )
    if trades is not None:
        pipeline._trade_poller = _wire_poller(pipeline, redis, trades, poll_clock, log)
    return pipeline


def _wire_poller(
    pipeline: Pipeline,
    redis: FakeAsyncRedis,
    trades: FakeTradesServer,
    poll_clock: FakeClock | None,
    log: list[str],
) -> TradePoller:
    clock = poll_clock or FakeClock(0.0)

    def on_state(state: IngestionState) -> None:
        log.append(f"poller:{state.value}")
        pipeline._on_ingestion_state(state)

    return TradePoller(
        pipeline._on_trade,
        redis=redis,
        settings=pipeline._settings.polymarket,
        metadata=pipeline._metadata_sync,
        on_state_change=on_state,
        http_client=trades.client(),
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )


def metadata_state(pipeline: Pipeline) -> SyncState:
    """The current metadata-sync state of a wired pipeline."""
    assert pipeline._metadata_sync is not None
    return pipeline._metadata_sync.state


class FailingDetector:
    """A detector whose analysis raises.

    Real detectors swallow their collaborator failures, so this is the only way to reach the
    pipeline's own detector-error handling.
    """

    def __init__(self, error: Exception) -> None:
        self.error = error

    async def analyze(self, trade: TradeEvent) -> None:
        _ = trade
        raise self.error


class BarrierDetector:
    """A detector that finishes only once every detector sharing its barrier has started.

    Sequential execution deadlocks at the barrier, so completing within a timeout proves the
    pipeline runs detectors concurrently without measuring wall-clock time.
    """

    def __init__(self, barrier: asyncio.Barrier) -> None:
        self._barrier = barrier

    async def analyze(self, trade: TradeEvent) -> None:
        _ = trade
        await self._barrier.wait()
