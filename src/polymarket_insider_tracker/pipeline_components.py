"""The collaborators a ``Pipeline`` runs on, and how the production set is assembled.

``Pipeline`` orchestrates; this module owns what it orchestrates. A caller that embeds the
pipeline over its own connections (or substitutes a collaborator) builds a ``PipelineComponents``
and installs it with ``Pipeline.install_components``; ``Pipeline.start`` builds the production
set from ``Settings`` with ``build_components`` when nothing was installed.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, TypeVar, cast

from redis.asyncio import Redis

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.channels.telegram import TelegramChannel
from polymarket_insider_tracker.alerter.dispatcher import AlertChannel, AlertDispatcher
from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.alerter.history import AlertHistory
from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.detector_config import capture_detector_config
from polymarket_insider_tracker.detector.evaluation import SizeEvaluation, WalletEvaluation
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.scorer import RiskScorer
from polymarket_insider_tracker.detector.scoring_config import DetectorConfig
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState, TradePoller
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.funding import FundingTracer
from polymarket_insider_tracker.storage.database import DatabaseManager

logger = logging.getLogger("polymarket_insider_tracker.pipeline")

_EvaluationT = TypeVar("_EvaluationT", covariant=True)


class Evaluator(Protocol[_EvaluationT]):
    """A detector the pipeline evaluates a trade with (satisfied by the real detectors)."""

    async def evaluate(self, trade: TradeEvent, *, evaluated_at: datetime) -> _EvaluationT: ...


class RedisFactory(Protocol):
    """Concrete Redis construction surface used by the pipeline."""

    @classmethod
    def from_url(cls, url: str) -> Redis: ...


@dataclass(frozen=True)
class PipelineComponents:
    """The collaborators a pipeline processes trades with.

    ``owns_connections`` says whether the pipeline created ``redis`` and ``db_manager`` and so
    closes them when it stops; a caller that supplies its own connections keeps them open.
    """

    redis: Redis
    db_manager: DatabaseManager | None
    metadata_sync: MarketMetadataSync
    funding_tracer: FundingTracer | None
    fresh_wallet_detector: Evaluator[WalletEvaluation]
    size_anomaly_detector: Evaluator[SizeEvaluation]
    detector_config: DetectorConfig
    risk_scorer: RiskScorer
    alert_formatter: AlertFormatter
    alert_dispatcher: AlertDispatcher
    trade_poller: TradePoller | None
    owns_connections: bool = True


def _build_discord_channel(settings: Settings) -> DiscordChannel | None:
    discord = settings.discord
    if not discord.enabled or not discord.webhook_url:
        return None
    logger.info("Discord channel enabled")
    return DiscordChannel(discord.webhook_url.get_secret_value())


def _build_telegram_channel(settings: Settings) -> TelegramChannel | None:
    telegram = settings.telegram
    if not telegram.enabled or not telegram.bot_token or not telegram.chat_id:
        return None
    logger.info("Telegram channel enabled")
    return TelegramChannel(telegram.bot_token.get_secret_value(), telegram.chat_id)


def build_alert_channels(settings: Settings) -> list[AlertChannel]:
    """Build the list of alert channels the settings enable."""
    channels: list[AlertChannel] = []
    discord_chan = _build_discord_channel(settings)
    if discord_chan:
        channels.append(discord_chan)

    telegram_chan = _build_telegram_channel(settings)
    if telegram_chan:
        channels.append(telegram_chan)

    if not channels:
        logger.warning("No alert channels configured")

    return channels


async def build_components(
    settings: Settings,
    *,
    dry_run: bool,
    on_trade: Callable[[TradeEvent], Awaitable[None]],
    on_ingestion_state: Callable[[IngestionState], None],
) -> PipelineComponents:
    """Assemble the production collaborators from ``settings``.

    A failure part-way through closes the connections already opened before it propagates.
    """
    logger.debug("Initializing Redis connection...")
    redis_factory = cast(type[RedisFactory], Redis)
    redis = redis_factory.from_url(settings.redis.url)
    db_manager: DatabaseManager | None = None
    try:
        logger.debug("Initializing database manager...")
        db_manager = DatabaseManager(settings.database.url, async_mode=True)
        return _assemble(settings, redis, db_manager, dry_run, on_trade, on_ingestion_state)
    except Exception:
        if db_manager is not None:
            await db_manager.dispose_async()
        await redis.aclose()
        raise


def _assemble(
    settings: Settings,
    redis: Redis,
    db_manager: DatabaseManager,
    dry_run: bool,
    on_trade: Callable[[TradeEvent], Awaitable[None]],
    on_ingestion_state: Callable[[IngestionState], None],
) -> PipelineComponents:
    logger.debug("Initializing Polygon client...")
    polygon_client = PolygonClient(
        settings.polygon.rpc_url,
        fallback_rpc_url=settings.polygon.fallback_rpc_url,
        redis=redis,
    )

    logger.debug("Initializing CLOB client...")
    clob_client = ClobClient()

    logger.debug("Initializing market metadata sync...")
    metadata_sync = MarketMetadataSync(redis=redis, clob_client=clob_client)

    logger.debug("Initializing wallet analyzer...")
    wallet_analyzer = WalletAnalyzer(polygon_client, redis=redis)

    logger.debug("Initializing funding tracer...")
    funding_tracer = FundingTracer(polygon_client)

    logger.debug("Initializing detectors...")
    fresh_detector = FreshWalletDetector(wallet_analyzer)
    size_detector = SizeAnomalyDetector(metadata_sync)

    logger.debug("Initializing risk scorer...")
    risk_scorer = RiskScorer(redis, alert_threshold=settings.detector.alert_threshold)
    logger.info(
        "RiskScorer threshold=%.2f persist=%s",
        settings.detector.alert_threshold,
        settings.detector.persist_assessments,
    )

    logger.debug("Initializing alerting components...")
    alert_formatter = AlertFormatter(verbosity="detailed")
    channels = build_alert_channels(settings)
    alert_history = AlertHistory(redis, dedup_window_seconds=settings.detector.dedup_window_seconds)
    logger.info("AlertHistory dedup_window=%ds", settings.detector.dedup_window_seconds)
    alert_dispatcher = AlertDispatcher(channels, history=alert_history, dry_run=dry_run)

    logger.debug("Initializing trade poller...")
    trade_poller = TradePoller(
        on_trade,
        redis=redis,
        settings=settings.polymarket,
        metadata=metadata_sync,
        on_state_change=on_ingestion_state,
    )

    return PipelineComponents(
        redis=redis,
        db_manager=db_manager,
        metadata_sync=metadata_sync,
        funding_tracer=funding_tracer,
        fresh_wallet_detector=fresh_detector,
        size_anomaly_detector=size_detector,
        detector_config=capture_detector_config(fresh_detector, size_detector),
        risk_scorer=risk_scorer,
        alert_formatter=alert_formatter,
        alert_dispatcher=alert_dispatcher,
        trade_poller=trade_poller,
    )
