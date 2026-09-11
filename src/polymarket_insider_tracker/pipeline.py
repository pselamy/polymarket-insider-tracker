"""Main pipeline orchestrator for Polymarket Insider Tracker.

This module provides the Pipeline class that wires together all detection
components and manages the event flow from ingestion to alerting.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, cast

from redis.asyncio import Redis

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.channels.telegram import TelegramChannel
from polymarket_insider_tracker.alerter.dispatcher import (
    AlertChannel,
    AlertDispatcher,
    DispatchResult,
)
from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.alerter.history import AlertHistory
from polymarket_insider_tracker.config import Settings, get_settings
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.health import ComponentStatus, HealthMonitor
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState, TradePoller
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.funding import FundingTracer
from polymarket_insider_tracker.redaction import redact_text
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.repos import (
    FundingRepository,
    FundingTransferDTO,
    RiskAssessmentDTO,
    RiskAssessmentRepository,
    WalletProfileDTO,
    WalletRepository,
)

if TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.ext.asyncio import AsyncSession

    from polymarket_insider_tracker.alerter.models import FormattedAlert
    from polymarket_insider_tracker.detector.models import (
        FreshWalletSignal,
        SizeAnomalySignal,
    )
    from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)


class RedisFactory(Protocol):
    """Concrete Redis construction surface used by the pipeline."""

    @classmethod
    def from_url(cls, url: str) -> Redis: ...


class Pingable(Protocol):
    """Protocol for health ping check on Redis."""

    def ping(self) -> Awaitable[bool]: ...


class PipelineState(StrEnum):
    """Pipeline lifecycle states."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class PipelineStats:
    """Statistics for the pipeline."""

    started_at: datetime | None = None
    trades_processed: int = 0
    signals_generated: int = 0
    alerts_sent: int = 0
    errors: int = 0
    last_trade_time: datetime | None = None
    last_error: str | None = None


class Pipeline:
    """Main pipeline orchestrator for the Polymarket Insider Tracker.

    This class wires together all detection components and manages the
    event flow from trade ingestion through profiling, detection, and alerting.

    Pipeline flow:
        Public trades query (TradePoller) → Wallet Profiler → Detectors → Risk Scorer → Alerter

    The poller starts before the market-metadata crawl completes; metadata is enrichment and
    never holds ingestion back.

    Example:
        ```python
        from polymarket_insider_tracker.config import get_settings
        from polymarket_insider_tracker.pipeline import Pipeline

        settings = get_settings()
        pipeline = Pipeline(settings)

        await pipeline.start()
        # Pipeline runs until stop() is called
        await pipeline.stop()
        ```
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        dry_run: bool | None = None,
    ) -> None:
        """Initialize the pipeline.

        Args:
            settings: Application settings. If not provided, uses get_settings().
            dry_run: If True, skip sending alerts. Overrides settings.dry_run.
        """
        self._settings = settings or get_settings()
        self._dry_run = dry_run if dry_run is not None else self._settings.dry_run

        self._state = PipelineState.STOPPED
        self._stats = PipelineStats()

        # Components (initialized in start())
        self._redis: Redis | None = None
        self._db_manager: DatabaseManager | None = None
        self._polygon_client: PolygonClient | None = None
        self._clob_client: ClobClient | None = None
        self._metadata_sync: MarketMetadataSync | None = None
        self._wallet_analyzer: WalletAnalyzer | None = None
        self._fresh_wallet_detector: FreshWalletDetector | None = None
        self._size_anomaly_detector: SizeAnomalyDetector | None = None
        self._risk_scorer: RiskScorer | None = None
        self._alert_formatter: AlertFormatter | None = None
        self._alert_history: AlertHistory | None = None
        self._alert_dispatcher: AlertDispatcher | None = None
        self._trade_poller: TradePoller | None = None
        self._funding_tracer: FundingTracer | None = None
        self._health_monitor: HealthMonitor = HealthMonitor()
        self._wire_health_monitor()

        # Synchronization
        self._stop_event: asyncio.Event | None = None
        self._poller_task: asyncio.Task[None] | None = None
        self._metadata_task: asyncio.Task[None] | None = None

    @property
    def state(self) -> PipelineState:
        """Current pipeline state."""
        return self._state

    @property
    def stats(self) -> PipelineStats:
        """Current pipeline statistics."""
        return self._stats

    @property
    def is_running(self) -> bool:
        """Check if pipeline is running."""
        return self._state == PipelineState.RUNNING

    @property
    def health_monitor(self) -> HealthMonitor:
        """Return the pipeline's health monitor."""
        return self._health_monitor

    @property
    def stop_event(self) -> asyncio.Event | None:
        """Event signaled when the pipeline stops or encounters an unrecoverable worker failure."""
        return self._stop_event

    def _wire_health_monitor(self) -> None:
        self._health_monitor.set_component_checker("database", self._check_database)
        self._health_monitor.set_component_checker("redis", self._check_redis)
        self._health_monitor.set_component_checker("ingestion", self._check_ingestion)
        self._health_monitor.set_last_error_provider(lambda: self._stats.last_error)

    async def _check_database(self) -> ComponentStatus:
        if not self._db_manager:
            return ComponentStatus(status="down", last_error="Database manager not initialized")
        start = time.perf_counter()
        try:
            import sqlalchemy as sa

            async with self._db_manager.get_async_session() as session:
                await session.execute(sa.text("SELECT 1"))
            latency = (time.perf_counter() - start) * 1000.0
            return ComponentStatus(status="up", latency_ms=round(latency, 2))
        except Exception as exc:
            return ComponentStatus(status="down", last_error=redact_text(str(exc)))

    async def _check_redis(self) -> ComponentStatus:
        if not self._redis:
            return ComponentStatus(status="down", last_error="Redis client not initialized")
        start = time.perf_counter()
        try:
            ping_client = cast(Pingable, self._redis)
            await ping_client.ping()
            latency = (time.perf_counter() - start) * 1000.0
            return ComponentStatus(status="up", latency_ms=round(latency, 2))
        except Exception as exc:
            return ComponentStatus(status="down", last_error=redact_text(str(exc)))

    def _sync_poller_timestamps(self) -> None:
        if not self._trade_poller or not self._health_monitor:
            return
        status = self._trade_poller.status
        # Only a completed fetch proves the source was reached; the request-start
        # timestamp (last_acquisition_at) must never masquerade as acquisition
        # freshness when the request itself failed.
        if status.last_success_at is not None:
            self._health_monitor.record_acquisition(status.last_success_at.timestamp())
        if status.last_trade_at is not None:
            self._health_monitor.record_trade_arrival(status.last_trade_at.timestamp())

    # Recoverable poller states that must surface as a degraded component with their
    # error instead of being hidden behind "up" (FR-002, FR-003).
    _DEGRADED_INGESTION_STATES = (IngestionState.DEGRADED, IngestionState.POSSIBLE_DATA_LOSS)

    async def _check_ingestion(self) -> ComponentStatus:
        self._sync_poller_timestamps()
        if not self._trade_poller:
            return ComponentStatus(status="down", last_error="Trade poller not initialized")
        if self._state == PipelineState.ERROR or self._trade_poller.state is IngestionState.FAILED:
            error = (
                self._trade_poller.status.last_error
                or self._stats.last_error
                or "ingestion_worker_failed"
            )
            return ComponentStatus(status="down", last_error=error)
        if not self._trade_poller.is_running:
            return ComponentStatus(status="down", last_error="Trade poller stopped")
        return self._running_ingestion_status()

    def _running_ingestion_status(self) -> ComponentStatus:
        """Readiness needs proof of reachability: a recent successful acquisition.

        A poller that is merely running (STARTING, or DEGRADED because every request
        failed) has not acquired anything; reporting it "up" would make a
        never-connected or unreachable source look ready. A progressing
        POSSIBLE_DATA_LOSS or transiently DEGRADED source keeps a fresh success and
        stays a visible, ready-compatible "degraded" component.
        """
        assert self._trade_poller is not None
        poller = self._trade_poller
        success_age = poller.seconds_since_last_success
        if success_age is None:
            return ComponentStatus(status="down", last_error=self._no_acquisition_error())
        if success_age > self._health_monitor.stale_threshold_seconds:
            return ComponentStatus(status="down", last_error=self._stale_success_error(success_age))
        state = poller.state
        if state in self._DEGRADED_INGESTION_STATES:
            error = poller.status.last_error or f"ingestion state: {state.value}"
            return ComponentStatus(status="degraded", last_error=error)
        return ComponentStatus(status="up")

    def _no_acquisition_error(self) -> str:
        assert self._trade_poller is not None
        last_error = self._trade_poller.status.last_error
        message = "trade source has not completed a successful acquisition"
        return f"{message}: {last_error}" if last_error else message

    def _stale_success_error(self, success_age: float) -> str:
        assert self._trade_poller is not None
        last_error = self._trade_poller.status.last_error
        threshold = self._health_monitor.stale_threshold_seconds
        message = (
            f"last successful acquisition was {success_age:.0f}s ago"
            f" (staleness bound {threshold:.0f}s)"
        )
        return f"{message}: {last_error}" if last_error else message

    async def check_readiness(self) -> tuple[bool, str | None, dict[str, str]]:
        """Evaluate current readiness across all dependencies."""
        components = await self._health_monitor.evaluate_components()
        all_up = HealthMonitor.all_components_up(components)
        summary = HealthMonitor.components_summary(components)
        reason = None
        if not all_up:
            reason = HealthMonitor.first_failure_reason(components)
        return all_up, reason, summary

    async def start(self) -> None:
        """Start the pipeline.

        Initializes all components and begins processing trades.

        Raises:
            RuntimeError: If pipeline is already running.
            Exception: If any component fails to initialize.
        """
        if self._state != PipelineState.STOPPED:
            raise RuntimeError(f"Cannot start pipeline in state {self._state}")

        self._state = PipelineState.STARTING
        self._stop_event = asyncio.Event()
        logger.info("Starting pipeline...")

        try:
            await self._initialize_components()
            await self._start_background_services()
            self._stats.started_at = datetime.now(UTC)
            # A worker may already have failed terminally during startup; never
            # overwrite that ERROR state with RUNNING.
            if self._state == PipelineState.STARTING:
                self._state = PipelineState.RUNNING
                logger.info("Pipeline started successfully")
        except Exception as e:
            self._state = PipelineState.ERROR
            self._stats.last_error = redact_text(str(e))
            logger.error("Failed to start pipeline: %s", self._stats.last_error)
            await self._cleanup()
            raise

    async def stop(self) -> None:
        """Stop the pipeline gracefully.

        Stops all background services and cleans up resources.
        """
        if self._state == PipelineState.STOPPED:
            return

        was_error = self._state == PipelineState.ERROR
        self._state = PipelineState.STOPPING
        logger.info("Stopping pipeline...")

        if self._stop_event:
            self._stop_event.set()

        await self._stop_background_services()
        await self._cleanup()

        self._state = PipelineState.ERROR if was_error else PipelineState.STOPPED
        logger.info("Pipeline stopped")

    async def _initialize_components(self) -> None:
        """Initialize all pipeline components."""
        settings = self._settings

        # Initialize Redis
        logger.debug("Initializing Redis connection...")
        redis_factory = cast(type[RedisFactory], Redis)
        self._redis = redis_factory.from_url(settings.redis.url)

        # Initialize Database Manager
        logger.debug("Initializing database manager...")
        self._db_manager = DatabaseManager(
            settings.database.url,
            async_mode=True,
        )

        # Initialize Polygon client
        logger.debug("Initializing Polygon client...")
        self._polygon_client = PolygonClient(
            settings.polygon.rpc_url,
            fallback_rpc_url=settings.polygon.fallback_rpc_url,
            redis=self._redis,
        )

        # Initialize CLOB client
        logger.debug("Initializing CLOB client...")
        api_key = (
            settings.polymarket.api_key.get_secret_value() if settings.polymarket.api_key else None
        )
        self._clob_client = ClobClient(api_key=api_key)

        # Initialize Market Metadata Sync
        logger.debug("Initializing market metadata sync...")
        self._metadata_sync = MarketMetadataSync(
            redis=self._redis,
            clob_client=self._clob_client,
        )

        # Initialize Wallet Analyzer
        logger.debug("Initializing wallet analyzer...")
        self._wallet_analyzer = WalletAnalyzer(
            self._polygon_client,
            redis=self._redis,
        )

        # Initialize Funding Tracer
        logger.debug("Initializing funding tracer...")
        self._funding_tracer = FundingTracer(self._polygon_client)

        # Initialize Detectors
        logger.debug("Initializing detectors...")
        self._fresh_wallet_detector = FreshWalletDetector(self._wallet_analyzer)
        self._size_anomaly_detector = SizeAnomalyDetector(self._metadata_sync)

        # Initialize Risk Scorer
        logger.debug("Initializing risk scorer...")
        self._risk_scorer = RiskScorer(
            self._redis,
            alert_threshold=settings.detector.alert_threshold,
            dedup_window_seconds=settings.detector.dedup_window_seconds,
        )
        logger.info(
            "RiskScorer threshold=%.2f dedup_window=%ds persist=%s",
            settings.detector.alert_threshold,
            settings.detector.dedup_window_seconds,
            settings.detector.persist_assessments,
        )

        # Initialize Alerting
        logger.debug("Initializing alerting components...")
        self._alert_formatter = AlertFormatter(verbosity="detailed")
        channels = self._build_alert_channels()
        self._alert_history = AlertHistory(
            self._redis,
            dedup_window_seconds=settings.detector.dedup_window_seconds,
        )
        self._alert_dispatcher = AlertDispatcher(
            channels,
            history=self._alert_history,
            dry_run=self._dry_run,
        )

        # Initialize the trade poller over the documented public trades query
        logger.debug("Initializing trade poller...")
        self._trade_poller = TradePoller(
            self._on_trade,
            redis=self._redis,
            settings=settings.polymarket,
            metadata=self._metadata_sync,
            on_state_change=self._on_ingestion_state,
        )

        logger.info("All components initialized")

    def _build_discord_channel(self) -> DiscordChannel | None:
        discord = self._settings.discord
        if not discord.enabled or not discord.webhook_url:
            return None
        logger.info("Discord channel enabled")
        return DiscordChannel(discord.webhook_url.get_secret_value())

    def _build_telegram_channel(self) -> TelegramChannel | None:
        telegram = self._settings.telegram
        if not telegram.enabled or not telegram.bot_token or not telegram.chat_id:
            return None
        logger.info("Telegram channel enabled")
        return TelegramChannel(telegram.bot_token.get_secret_value(), telegram.chat_id)

    def _build_alert_channels(self) -> list[AlertChannel]:
        """Build list of enabled alert channels."""
        channels: list[AlertChannel] = []
        discord_chan = self._build_discord_channel()
        if discord_chan:
            channels.append(discord_chan)

        telegram_chan = self._build_telegram_channel()
        if telegram_chan:
            channels.append(telegram_chan)

        if not channels:
            logger.warning("No alert channels configured")

        return channels

    async def _start_health_server(self) -> None:
        self._wire_health_monitor()
        assert self._health_monitor is not None
        await self._health_monitor.start()
        try:
            await self._health_monitor.start_http_server(port=self._settings.health_port)
        except OSError as exc:
            logger.error(
                "Failed to bind health server on port %d: %s",
                self._settings.health_port,
                exc,
            )
            raise

    async def _start_background_services(self) -> None:
        """Start the poller first, then the metadata crawl as a tracked background task."""
        await self._start_health_server()

        if self._trade_poller:
            logger.debug("Starting trade poller...")
            self._poller_task = asyncio.create_task(self._run_trade_poller())

        if self._metadata_sync:
            logger.debug("Starting metadata sync service in the background...")
            self._metadata_task = asyncio.create_task(self._run_metadata_sync())

    def _record_processing_error(self, message: str) -> None:
        """Count a recoverable per-trade failure and surface it at /health (lifecycle §5)."""
        redacted = redact_text(message)
        logger.warning(redacted)
        self._stats.errors += 1
        self._stats.last_error = redacted

    def _handle_worker_failure(self, error: str) -> None:
        """Handle background worker crash or terminal failure."""
        if self._state in (PipelineState.STARTING, PipelineState.RUNNING):
            self._state = PipelineState.ERROR
        self._stats.errors += 1
        self._stats.last_error = redact_text(error)
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()

    async def _run_trade_poller(self) -> None:
        """Run the poller in a task; a terminal failure is reported through the state callback."""
        if not self._trade_poller:
            return
        try:
            await self._trade_poller.start()
        except asyncio.CancelledError:
            logger.debug("Trade poller task cancelled")
        except Exception as e:
            logger.error("Trade poller error: %s", redact_text(str(e)))
            self._handle_worker_failure(str(e))

    async def _run_metadata_sync(self) -> None:
        """Run the initial metadata crawl without delaying acquisition."""
        if not self._metadata_sync:
            return
        try:
            await self._metadata_sync.start()
        except asyncio.CancelledError:
            logger.debug("Metadata sync task cancelled")
        except Exception as e:
            logger.error("Metadata sync error: %s", redact_text(str(e)))
            self._stats.last_error = redact_text(str(e))
            self._stats.errors += 1

    def _on_ingestion_state(self, state: IngestionState) -> None:
        """Record terminal ingestion failures on the pipeline statistics."""
        if state is not IngestionState.FAILED or not self._trade_poller:
            return
        error = self._trade_poller.status.last_error or "ingestion_worker_failed"
        self._handle_worker_failure(error)

    async def _stop_background_services(self) -> None:
        """Stop the poller and its task before the metadata task and sync."""
        if self._trade_poller:
            logger.debug("Stopping trade poller...")
            await self._trade_poller.stop()
        await self._cancel_task(self._poller_task)
        await self._cancel_task(self._metadata_task)
        self._poller_task = None
        self._metadata_task = None

        if self._metadata_sync:
            logger.debug("Stopping metadata sync...")
            await self._metadata_sync.stop()

        if self._health_monitor:
            await self._health_monitor.stop()

    @staticmethod
    async def _cancel_task(task: asyncio.Task[None] | None) -> None:
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _cleanup(self) -> None:
        """Clean up resources."""
        await self._health_monitor.stop()

        # Close database connections
        if self._db_manager:
            await self._db_manager.dispose_async()
            self._db_manager = None

        # Close Redis connection
        if self._redis:
            await self._redis.aclose()
            self._redis = None

        logger.debug("Resources cleaned up")

    async def _on_trade(self, trade: TradeEvent) -> None:
        """Process a single trade event.

        This is the main event handler that runs the detection pipeline:
        1. Run fresh wallet detection
        2. Run size anomaly detection
        3. Score the combined signals
        4. Send alert if threshold exceeded

        Args:
            trade: The trade observation delivered by the poller.
        """
        self._stats.trades_processed += 1
        self._stats.last_trade_time = datetime.now(UTC)
        if self._health_monitor:
            self._health_monitor.record_event("trades")

        try:
            await self._detect_score_and_alert(trade)
        except Exception as e:
            self._record_processing_error(f"Error processing trade {trade.trade_id}: {e}")

    async def _detect_score_and_alert(self, trade: TradeEvent) -> None:
        """Run detectors, count their failures, then score/alert or persist a skip row."""
        # Run detectors in parallel; each returns its signal and any failure message
        (fresh_signal, fresh_error), (size_signal, size_error) = await asyncio.gather(
            self._detect_fresh_wallet(trade),
            self._detect_size_anomaly(trade),
        )
        had_detector_failure = self._record_detector_failures(fresh_error, size_error)

        # Persist wallet profile and funding data when a fresh wallet is detected
        if fresh_signal is not None:
            await self._persist_wallet_and_funding(fresh_signal)

        bundle = SignalBundle(
            trade_event=trade,
            fresh_wallet_signal=fresh_signal,
            size_anomaly_signal=size_signal,
        )
        if fresh_signal or size_signal:
            self._stats.signals_generated += 1
            await self._score_and_alert(bundle)
        elif had_detector_failure:
            await self._persist_detector_failure_skip(bundle)

    def _record_detector_failures(self, *errors: str | None) -> bool:
        """Count each failed detector and expose the failure in operational state."""
        failures = [error for error in errors if error]
        for error in failures:
            self._stats.errors += 1
            self._stats.last_error = redact_text(error)
        return bool(failures)

    async def _persist_detector_failure_skip(self, bundle: SignalBundle) -> None:
        """Persist why no evidence exists when detector failure is the only outcome.

        Without this row, a trade whose detectors all failed (or whose only working
        detector found nothing) would leave no durable explanation for the absent
        evidence. The skip row is never dispatched.
        """
        if not self._settings.detector.persist_assessments:
            return
        assessment = RiskAssessment(
            trade_event=bundle.trade_event,
            wallet_address=bundle.wallet_address,
            market_id=bundle.market_id,
            delivery_disposition="detector_failure",
            dry_run=self._dry_run,
            # The skip row was produced under the active configuration even though
            # no score was computed; record it so the row stays self-describing.
            scoring_config=(self._risk_scorer.scoring_config if self._risk_scorer else None),
        )
        await self._persist_assessment(assessment)

    async def _persist_wallet_and_funding(self, signal: FreshWalletSignal) -> None:
        """Persist wallet profile and funding transfers to Postgres.

        Called when a fresh wallet signal is detected. Upserts the wallet
        profile and traces/inserts any funding transfers found on-chain.

        Args:
            signal: The fresh wallet signal containing the wallet profile.
        """
        if not self._db_manager:
            return

        profile = signal.wallet_profile
        address = profile.address

        try:
            async with self._db_manager.get_async_session() as session:
                wallet_repo = WalletRepository(session)
                dto = WalletProfileDTO(
                    address=address,
                    nonce=profile.nonce,
                    first_seen_at=profile.first_seen,
                    is_fresh=profile.is_fresh,
                    matic_balance=profile.matic_balance,
                    usdc_balance=profile.usdc_balance,
                    analyzed_at=profile.analyzed_at,
                )
                await wallet_repo.upsert(dto)
                funding_transfer_count = await self._persist_funding_transfers(session, address)
                logger.debug(
                    "Persisted wallet profile and %d funding transfers for %s",
                    funding_transfer_count,
                    address[:10] + "...",
                )
        except Exception as e:
            logger.warning(
                "Failed to persist wallet/funding data for %s: %s", address, redact_text(str(e))
            )

    async def _persist_funding_transfers(self, session: AsyncSession, address: str) -> int:
        if not self._funding_tracer:
            return 0
        chain = await self._funding_tracer.trace(address)
        if not chain.chain:
            return 0
        funding_repo = FundingRepository(session)
        funding_dtos = [
            FundingTransferDTO(
                from_address=t.from_address,
                to_address=t.to_address,
                amount=t.amount,
                token=t.token,
                tx_hash=t.tx_hash,
                block_number=t.block_number,
                timestamp=t.timestamp,
            )
            for t in chain.chain
        ]
        await funding_repo.insert_many(funding_dtos)
        return len(chain.chain)

    async def _detect_fresh_wallet(
        self, trade: TradeEvent
    ) -> tuple[FreshWalletSignal | None, str | None]:
        """Run fresh wallet detection; a failure is returned so it can be counted."""
        if not self._fresh_wallet_detector:
            return None, None
        try:
            return await self._fresh_wallet_detector.analyze(trade), None
        except Exception as e:
            message = redact_text(f"fresh wallet detection failed for trade {trade.trade_id}: {e}")
            logger.warning("%s", message)
            return None, message

    async def _detect_size_anomaly(
        self, trade: TradeEvent
    ) -> tuple[SizeAnomalySignal | None, str | None]:
        """Run size anomaly detection; a failure is returned so it can be counted."""
        if not self._size_anomaly_detector:
            return None, None
        try:
            return await self._size_anomaly_detector.analyze(trade), None
        except Exception as e:
            message = redact_text(f"size anomaly detection failed for trade {trade.trade_id}: {e}")
            logger.warning("%s", message)
            return None, message

    def _can_alert(self) -> bool:
        return (
            self._risk_scorer is not None
            and self._alert_formatter is not None
            and self._alert_dispatcher is not None
        )

    async def _dispatch_alert(
        self, formatted_alert: FormattedAlert, assessment: RiskAssessment
    ) -> DispatchResult:
        assert self._alert_dispatcher is not None
        result = await self._alert_dispatcher.dispatch(formatted_alert, assessment=assessment)
        if result.all_succeeded:
            self._stats.alerts_sent += 1
            logger.info(
                "Alert sent successfully: wallet=%s, score=%.2f",
                assessment.wallet_address[:10] + "...",
                assessment.weighted_score,
            )
        elif not self._dry_run and result.disposition != "duplicate":
            logger.warning(
                "Alert dispatch disposition=%s (%d/%d succeeded)",
                result.disposition,
                result.success_count,
                result.success_count + result.failure_count,
            )
        return result

    async def _send_or_dry_run_alert(self, assessment: RiskAssessment) -> DispatchResult:
        assert self._alert_formatter is not None
        formatted_alert = self._alert_formatter.format(assessment)
        if self._dry_run:
            logger.info(
                "[DRY RUN] Would send alert: wallet=%s, score=%.2f",
                assessment.wallet_address[:10] + "...",
                assessment.weighted_score,
            )
        return await self._dispatch_alert(formatted_alert, assessment)

    async def _handle_below_threshold(
        self, bundle: SignalBundle, assessment: RiskAssessment
    ) -> None:
        assessment = dataclasses.replace(
            assessment,
            delivery_disposition="below_threshold",
            dry_run=self._dry_run,
        )
        if self._settings.detector.persist_assessments:
            await self._persist_assessment(assessment)
        logger.debug(
            "Trade %s below alert threshold (score=%.2f)",
            bundle.trade_event.trade_id,
            assessment.weighted_score,
        )

    async def _handle_above_threshold(self, assessment: RiskAssessment) -> None:
        """Persist the qualifying assessment before any delivery work, then record the outcome.

        A formatter or dispatch failure — or termination after an external delivery —
        between scoring and persistence must not lose the durable research record
        (FR-012). The pending row carries the ``unrecorded`` disposition until the
        delivery outcome is known; an initial persistence failure stays observable
        and never blocks the authorized delivery attempt (FR-013).
        """
        persist = self._settings.detector.persist_assessments
        pending = dataclasses.replace(assessment, dry_run=self._dry_run)
        pending_written = persist and await self._persist_assessment(pending)
        result = await self._send_or_dry_run_alert(assessment)
        channels_str = json.dumps(result.channel_statuses) if result.channel_statuses else None
        final = dataclasses.replace(
            pending,
            delivery_disposition=result.disposition,
            delivery_channels=channels_str,
        )
        if persist:
            await self._record_final_disposition(final, already_written=pending_written)

    async def _record_final_disposition(
        self, assessment: RiskAssessment, *, already_written: bool
    ) -> None:
        """Update the pending row with the delivery outcome, inserting it if it is missing."""
        if already_written and await self._update_assessment_delivery(assessment):
            return
        await self._persist_assessment(assessment)

    async def _score_and_alert(self, bundle: SignalBundle) -> None:
        """Score signals, persist the assessment, and send alert if above threshold."""
        if not self._can_alert():
            return
        assert self._risk_scorer is not None

        assessment = await self._risk_scorer.assess(bundle)
        if not assessment.should_alert:
            await self._handle_below_threshold(bundle, assessment)
            return

        await self._handle_above_threshold(assessment)

    async def _update_assessment_delivery(self, assessment: RiskAssessment) -> bool:
        """Record the delivery outcome on the pending row. Best-effort; never raises."""
        if not self._db_manager:
            return False
        try:
            async with self._db_manager.get_async_session() as session:
                repo = RiskAssessmentRepository(session)
                return await repo.update_delivery(
                    assessment.assessment_id,
                    delivery_disposition=assessment.delivery_disposition,
                    delivery_channels=assessment.delivery_channels,
                    dry_run=assessment.dry_run,
                )
        except Exception as e:
            self._record_processing_error(
                f"Failed to record delivery outcome for assessment {assessment.assessment_id}: {e}"
            )
            return False

    async def _persist_assessment(self, assessment: RiskAssessment) -> bool:
        """Write the assessment row. Best-effort; never raises.

        Returns True when the row is durably written. A failure is observable
        (counted, surfaced at /health) without blocking delivery (FR-013).
        """
        if not self._db_manager:
            return False
        from decimal import Decimal as _D

        from polymarket_insider_tracker.detector.scorer import quantize_score_value

        trade = assessment.trade_event
        fresh = assessment.fresh_wallet_signal
        size_sig = assessment.size_anomaly_signal
        wallet_age: _D | None = None
        if fresh is not None and fresh.wallet_profile.age_hours is not None:
            wallet_age = _D(str(round(float(fresh.wallet_profile.age_hours), 2)))
        dto = RiskAssessmentDTO(
            assessment_id=assessment.assessment_id,
            trade_id=trade.trade_id,
            wallet_address=assessment.wallet_address.lower(),
            market_id=assessment.market_id,
            asset_id=getattr(trade, "asset_id", None) or None,
            side=trade.side,
            outcome=getattr(trade, "outcome", None) or None,
            outcome_index=getattr(trade, "outcome_index", None),
            price=trade.price,
            size=trade.size,
            notional_usdc=trade.notional_value,
            trade_timestamp=trade.timestamp,
            # The scorer quantizes with the same function, so the stored score,
            # confidences, and threshold replay the decision exactly (FR-012).
            weighted_score=quantize_score_value(assessment.weighted_score),
            signals_triggered=assessment.signals_triggered,
            fresh_wallet_confidence=(
                quantize_score_value(fresh.confidence) if fresh is not None else None
            ),
            size_anomaly_confidence=(
                quantize_score_value(size_sig.confidence) if size_sig is not None else None
            ),
            is_niche_market=size_sig.is_niche_market if size_sig is not None else None,
            volume_impact=(
                _D(str(round(size_sig.volume_impact, 4))) if size_sig is not None else None
            ),
            book_impact=(_D(str(round(size_sig.book_impact, 4))) if size_sig is not None else None),
            wallet_age_hours=wallet_age,
            should_alert=assessment.should_alert,
            threshold_at_eval=quantize_score_value(self._settings.detector.alert_threshold),
            scoring_algorithm_version=assessment.scoring_algorithm_version,
            scoring_config=assessment.scoring_config,
            delivery_disposition=assessment.delivery_disposition,
            delivery_channels=assessment.delivery_channels,
            dry_run=assessment.dry_run,
            volume_available=assessment.volume_available,
            market_daily_volume=assessment.market_daily_volume,
            book_depth_available=assessment.book_depth_available,
            wallet_tx_count=assessment.wallet_tx_count,
            wallet_age_known=assessment.wallet_age_known,
        )
        try:
            async with self._db_manager.get_async_session() as session:
                repo = RiskAssessmentRepository(session)
                await repo.insert(dto)
        except Exception as e:
            self._record_processing_error(
                f"Failed to persist risk assessment {assessment.assessment_id}: {e}"
            )
            return False
        return True

    async def run(self) -> None:
        """Start the pipeline and run until interrupted.

        This is a convenience method that starts the pipeline and
        blocks until a stop signal is received.

        Example:
            ```python
            pipeline = Pipeline()
            try:
                await pipeline.run()
            except KeyboardInterrupt:
                pass
            ```
        """
        await self.start()

        try:
            if self._stop_event:
                await self._stop_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def __aenter__(self) -> Pipeline:
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.stop()
