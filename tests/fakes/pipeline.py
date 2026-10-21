"""Assemble a real ``Pipeline`` with only its external boundaries replaced."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Generic, NoReturn, TypeVar

import pytest
from redis.asyncio import Redis

import polymarket_insider_tracker.ingestor.clob_client as clob_module
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
    TradesCoverage,
)
from polymarket_insider_tracker.detector.detector_config import default_detector_config
from polymarket_insider_tracker.detector.evaluation import SizeEvaluation, WalletEvaluation
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.scorer import RiskScorer
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.metadata_sync import (
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_REDIS_KEY_PREFIX,
    MarketMetadataSync,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import (
    IngestionState,
    TradePoller,
)
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.pipeline_components import Evaluator, PipelineComponents
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.funding import FundingTracer
from polymarket_insider_tracker.storage.database import DatabaseManager
from tests.fakes.clob import FakeBaseClobClient
from tests.fakes.gamma import FakeGammaClient
from tests.fakes.trades import FakeClock, FakeTradesServer
from tests.fakes.trades_v2 import FakeV2TradesServer
from tests.fakes.web3 import FakeAsyncWeb3, FakeEth

TradesServer = FakeTradesServer | FakeV2TradesServer
EvaluationT = TypeVar("EvaluationT")


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
    trades_url: str | None = "https://trades.invalid/trades",
    trades_coverage: str = "all",
    trades_poll_interval_seconds: int = 5,
    trades_recovery_horizon_seconds: int = 600,
    health_port: int = 8080,
) -> Settings:
    """Create a fully validated ``Settings`` value without reading the environment.

    ``trades_url=None`` selects the configuration default (``OFFICIAL_V2_TRADES_URL``) explicitly,
    so neither the environment nor a ``.env`` file can supply it.
    """
    return Settings.model_validate(
        {
            "redis": RedisSettings.model_validate({"REDIS_URL": "redis://localhost:6379/0"}),
            "database": DatabaseSettings.model_validate(
                {"DATABASE_URL": "postgresql+psycopg://user:pass@localhost:5432/db"}
            ),
            "polygon": PolygonSettings.model_validate(
                {
                    "POLYGON_RPC_URL": "https://polygon-rpc.com",
                    "POLYGON_FALLBACK_RPC_URL": None,
                }
            ),
            "polymarket": PolymarketSettings.model_validate(
                {
                    "POLYMARKET_TRADES_URL": trades_url or OFFICIAL_V2_TRADES_URL,
                    "POLYMARKET_TRADES_COVERAGE": TradesCoverage(trades_coverage),
                    "POLYMARKET_TRADES_POLL_INTERVAL_SECONDS": trades_poll_interval_seconds,
                    "POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS": trades_recovery_horizon_seconds,
                    "POLYMARKET_WS_URL": None,
                    "POLYMARKET_API_KEY": None,
                }
            ),
            "discord": DiscordSettings.model_validate(
                {"DISCORD_WEBHOOK_URL": discord_webhook_url if discord_enabled else None}
            ),
            "telegram": TelegramSettings.model_validate(
                {
                    "TELEGRAM_BOT_TOKEN": telegram_bot_token if telegram_enabled else None,
                    "TELEGRAM_CHAT_ID": telegram_chat_id if telegram_enabled else None,
                }
            ),
            # Detector fields are declared by alias with ``extra="ignore"``; the field names
            # themselves would be silently dropped, so the aliases must be used here.
            "detector": DetectorSettings.model_validate(
                {
                    "DETECTOR_PERSIST_ASSESSMENTS": persist_assessments,
                    "DETECTOR_ALERT_THRESHOLD": alert_threshold,
                    "DETECTOR_DEDUP_WINDOW_SECONDS": 3600,
                }
            ),
            "LOG_LEVEL": "INFO",
            "HEALTH_PORT": health_port,
            "DRY_RUN": dry_run,
        }
    )


@dataclass(frozen=True)
class Substitutes:
    """Collaborators a test replaces; every unset one stays the real, wired component."""

    formatter: AlertFormatter | None = None
    fresh_detector: Evaluator[WalletEvaluation] | None = None
    size_detector: Evaluator[SizeEvaluation] | None = None
    poller: TradePoller | None = None
    detectors_together: bool = False


class WiredPipeline(Pipeline):
    """A real ``Pipeline`` that keeps the collaborators it was wired with for tests to drive."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.poller: TradePoller | None = None
        self.metadata_sync: MarketMetadataSync | None = None


def _polygon_client(settings: Settings, redis: Redis, eth: FakeEth) -> PolygonClient:
    return PolygonClient(
        settings.polygon.rpc_url,
        redis=redis,
        max_requests_per_second=10_000.0,
        retry_delay_seconds=0.0,
        web3=FakeAsyncWeb3(eth),
    )


def _sdk_boundary(base_client: FakeBaseClobClient) -> ClobClient:
    """A real ``ClobClient`` whose SDK handle is ``base_client``."""

    def build(host: str) -> FakeBaseClobClient:
        _ = host
        return base_client

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(clob_module, "BaseClobClient", build)
        return ClobClient()


def _metadata_sync(
    redis: Redis, crawl_gate: threading.Event | None, log: list[str]
) -> MarketMetadataSync:
    base_client = FakeBaseClobClient()
    base_client.crawl_gate = crawl_gate
    return MarketMetadataSync(
        redis=redis,
        clob_client=_sdk_boundary(base_client),
        gamma_client=FakeGammaClient(),
        on_state_change=lambda state: log.append(f"metadata:{state.value}"),
    )


async def _cache_market(redis: Redis, market: MarketMetadata | None) -> None:
    """Leave ``market`` in Redis the way a completed metadata sync does."""
    if market is None:
        return
    await redis.setex(
        f"{DEFAULT_REDIS_KEY_PREFIX}{market.condition_id}",
        DEFAULT_CACHE_TTL_SECONDS,
        json.dumps(market.to_dict()),
    )


def _detectors(
    polygon_client: PolygonClient,
    metadata_sync: MarketMetadataSync,
    redis: Redis,
    substitutes: Substitutes,
) -> tuple[Evaluator[WalletEvaluation], Evaluator[SizeEvaluation]]:
    fresh = substitutes.fresh_detector or FreshWalletDetector(
        WalletAnalyzer(polygon_client, redis=redis)
    )
    size = substitutes.size_detector or SizeAnomalyDetector(metadata_sync)
    if not substitutes.detectors_together:
        return fresh, size
    barrier = asyncio.Barrier(2)
    return BarrierDetector(barrier, fresh), BarrierDetector(barrier, size)


def _dispatcher(
    settings: Settings, redis: Redis, channels: Sequence[AlertChannel], dry_run: bool
) -> AlertDispatcher:
    history = AlertHistory(redis, dedup_window_seconds=settings.detector.dedup_window_seconds)
    return AlertDispatcher(list(channels), history=history, dry_run=dry_run)


def _scorer(settings: Settings, redis: Redis) -> RiskScorer:
    return RiskScorer(
        redis,
        alert_threshold=settings.detector.alert_threshold,
        dedup_window_seconds=settings.detector.dedup_window_seconds,
    )


async def wire_pipeline(
    settings: Settings,
    *,
    redis: Redis,
    eth: FakeEth,
    market: MarketMetadata | None = None,
    db_manager: DatabaseManager | None = None,
    channels: Sequence[AlertChannel] = (),
    trades: TradesServer | None = None,
    poll_clock: FakeClock | None = None,
    crawl_gate: threading.Event | None = None,
    state_log: list[str] | None = None,
    trace_funding: bool = True,
    substitutes: Substitutes | None = None,
    pipeline_type: type[WiredPipeline] = WiredPipeline,
) -> WiredPipeline:
    """Assemble a real ``Pipeline`` with only its external boundaries replaced.

    Every product component is real: the Polygon client, wallet analyzer, funding tracer,
    metadata sync, both detectors, the risk scorer, the alert formatter, the dispatcher, and,
    when ``trades`` is given, the trade poller. Only the boundaries differ: Redis is ``fakeredis``,
    the Polygon RPC is ``eth``, the CLOB SDK is ``FakeBaseClobClient`` (held behind ``crawl_gate``
    when given), gamma statistics are ``FakeGammaClient``, the trades query is ``trades`` on
    ``poll_clock``, market metadata is pre-cached in
    Redis the way a completed sync leaves it, and delivery goes to ``channels`` instead of Discord
    or Telegram. ``state_log`` records poller and metadata state transitions in order.
    ``trace_funding=False`` leaves the pipeline without a funding tracer.

    ``substitutes`` replaces named collaborators (a failing detector, a stand-in trade source).
    The caller owns ``redis`` and ``db_manager``: stopping the pipeline leaves them open.
    ``pipeline_type`` selects a ``WiredPipeline`` subclass for lifecycle failure injection.
    """
    substitutes = substitutes or Substitutes()
    log = state_log if state_log is not None else []
    pipeline = pipeline_type(settings)
    polygon_client = _polygon_client(settings, redis, eth)
    metadata_sync = _metadata_sync(redis, crawl_gate, log)
    await _cache_market(redis, market)
    fresh_detector, size_detector = _detectors(polygon_client, metadata_sync, redis, substitutes)
    if trades is not None:
        pipeline.poller = _wire_poller(pipeline, redis, trades, poll_clock, metadata_sync, log)
    pipeline.metadata_sync = metadata_sync
    pipeline.install_components(
        PipelineComponents(
            redis=redis,
            db_manager=db_manager,
            metadata_sync=metadata_sync,
            funding_tracer=FundingTracer(polygon_client) if trace_funding else None,
            fresh_wallet_detector=fresh_detector,
            size_anomaly_detector=size_detector,
            detector_config=default_detector_config(),
            risk_scorer=_scorer(settings, redis),
            alert_formatter=substitutes.formatter or AlertFormatter(verbosity="detailed"),
            alert_dispatcher=_dispatcher(settings, redis, channels, pipeline.dry_run),
            trade_poller=substitutes.poller or pipeline.poller,
            owns_connections=False,
        )
    )
    return pipeline


def _wire_poller(
    pipeline: Pipeline,
    redis: Redis,
    trades: TradesServer,
    poll_clock: FakeClock | None,
    metadata_sync: MarketMetadataSync,
    log: list[str],
) -> TradePoller:
    clock = poll_clock or FakeClock(0.0)

    def on_state(state: IngestionState) -> None:
        log.append(f"poller:{state.value}")
        pipeline.on_ingestion_state(state)

    return TradePoller(
        pipeline.process_trade,
        redis=redis,
        settings=pipeline.settings.polymarket,
        metadata=metadata_sync,
        on_state_change=on_state,
        http_client=trades.client(),
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )


class FailingDetector:
    """A detector whose evaluation raises, exercising the pipeline's detector-failure
    counting and skip-disposition handling deterministically."""

    def __init__(self, error: Exception) -> None:
        self.error = error

    async def evaluate(self, trade: TradeEvent, *, evaluated_at: datetime) -> NoReturn:
        _ = trade, evaluated_at
        raise self.error


class BarrierDetector(Generic[EvaluationT]):
    """A detector that finishes only once every detector sharing its barrier has started.

    Sequential execution deadlocks at the barrier, so completing within a timeout proves the
    pipeline runs detectors concurrently without measuring wall-clock time. After the barrier
    the wrapped real detector evaluates the trade.
    """

    def __init__(self, barrier: asyncio.Barrier, inner: Evaluator[EvaluationT]) -> None:
        self._barrier = barrier
        self._inner = inner

    async def evaluate(self, trade: TradeEvent, *, evaluated_at: datetime) -> EvaluationT:
        await self._barrier.wait()
        return await self._inner.evaluate(trade, evaluated_at=evaluated_at)


def detector_trade() -> TradeEvent:
    """A real trade value for exercising the detector-failure path."""
    return TradeEvent(
        trade_id="0x" + "e" * 64,
        wallet_address="0x" + "f" * 40,
        market_id="0x" + "d" * 64,
        asset_id="asset_redact_r7",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.65"),
        size=Decimal("5000"),
        timestamp=datetime.now(UTC),
    )
