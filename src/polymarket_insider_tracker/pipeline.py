"""Main pipeline orchestrator for Polymarket Insider Tracker.

This module provides the Pipeline class that wires together all detection
components and manages the event flow from ingestion to alerting.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, TypeVar, cast

from redis.asyncio import Redis

from polymarket_insider_tracker import terminal_errors as terminal
from polymarket_insider_tracker.alerter.dispatcher import AlertDispatcher, DispatchResult
from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.assessment_row import assessment_dto
from polymarket_insider_tracker.config import Settings, get_settings
from polymarket_insider_tracker.detector.detector_config import default_detector_config
from polymarket_insider_tracker.detector.evaluation import (
    DetectorError,
    SizeEvaluation,
    TradeEvidence,
    WalletEvaluation,
)
from polymarket_insider_tracker.detector.evaluation_clock import (
    Clock,
    read_evaluation_instant,
    system_clock,
)
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.ingestor.alert_path_health import AlertPathHealthMonitor
from polymarket_insider_tracker.ingestor.health import ComponentStatus, HealthMonitor
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.readiness import running_ingestion_status
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState, TradePoller
from polymarket_insider_tracker.pipeline_components import (
    Evaluator,
    PipelineComponents,
    build_components,
)
from polymarket_insider_tracker.profiler.funding import FundingTracer
from polymarket_insider_tracker.redaction import redact_exception_message, redact_text
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.repos import (
    FundingRepository,
    FundingTransferDTO,
    RiskAssessmentRepository,
    WalletProfileDTO,
    WalletRepository,
)

if TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.ext.asyncio import AsyncSession

    from polymarket_insider_tracker.alerter.models import FormattedAlert
    from polymarket_insider_tracker.detector.models import FreshWalletSignal
    from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)

_EvaluationT = TypeVar("_EvaluationT", covariant=True)


async def _run_detector(
    detector: str, trade: TradeEvent, evaluation: Awaitable[_EvaluationT]
) -> tuple[_EvaluationT | None, DetectorError | None]:
    """Await one detector's evaluation; a failure is logged redacted and returned, not raised."""
    try:
        return await evaluation, None
    except Exception as e:
        message = redact_text(
            f"{detector.replace('_', ' ')} detection failed for trade {trade.trade_id}:"
            f" {redact_exception_message(e)}"
        )
        logger.warning("%s", message)
        return None, DetectorError(detector, type(e).__name__, message)


def _signal_bundle(trade: TradeEvent, evidence: TradeEvidence) -> SignalBundle:
    """The bundle the scorer reads: each evaluation's signal, with the evidence behind them."""
    wallet, size = evidence.wallet, evidence.size
    fresh_signal = wallet.signal if wallet is not None else None
    return SignalBundle(trade, fresh_signal, size.signal if size is not None else None, evidence)


def _failures_only(evidence: TradeEvidence | None) -> TradeEvidence | None:
    """A skip row records its detector failures and no evaluation evidence."""
    if evidence is None:
        return None
    return TradeEvidence(
        evidence.evaluated_at, evidence.detector_config, failures=evidence.failures
    )


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
        self._owns_connections = True
        self._components_installed = False
        self._metadata_sync: MarketMetadataSync | None = None
        self._fresh_wallet_detector: Evaluator[WalletEvaluation] | None = None
        self._size_anomaly_detector: Evaluator[SizeEvaluation] | None = None
        self._detector_config = default_detector_config()
        self._clock: Clock = system_clock
        self._risk_scorer: RiskScorer | None = None
        self._alert_formatter: AlertFormatter | None = None
        self._alert_dispatcher: AlertDispatcher | None = None
        self._trade_poller: TradePoller | None = None
        self._funding_tracer: FundingTracer | None = None
        self._health_monitor = AlertPathHealthMonitor(self._settings.detector.alert_threshold)
        self._wire_health_monitor()

        # Synchronization
        self._stop_event: asyncio.Event | None = None
        self._poller_task: asyncio.Task[None] | None = None
        self._metadata_task: asyncio.Task[None] | None = None
        self._terminal_writes: set[asyncio.Task[None]] = set()

    @property
    def settings(self) -> Settings:
        """The application settings this pipeline runs with."""
        return self._settings

    @property
    def dry_run(self) -> bool:
        """Whether alerts are only logged: the constructor override, else the settings flag."""
        return self._dry_run

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

    @property
    def db_manager(self) -> DatabaseManager | None:
        """Database manager used for durable terminal rows."""
        return self._db_manager

    @property
    def terminal_writes(self) -> set[asyncio.Task[None]]:
        """Retained durable writes drained across stop/shutdown."""
        return self._terminal_writes

    def record_processing_error(self, message: str) -> None:
        """Count a recoverable per-trade failure and surface it at /health (lifecycle §5)."""
        self._record_processing_error(message)

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
            return ComponentStatus(status="down", last_error=redact_exception_message(exc))

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
            return ComponentStatus(status="down", last_error=redact_exception_message(exc))

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

    async def _check_ingestion(self) -> ComponentStatus:
        self._sync_poller_timestamps()
        if not self._trade_poller:
            return ComponentStatus(status="down", last_error="Trade poller not initialized")
        if self._state == PipelineState.ERROR or self._trade_poller.state is IngestionState.FAILED:
            reason = await terminal.resolve_terminal_reason_live(
                self.db_manager,
                self._trade_poller.status.last_error,
                self._state == PipelineState.ERROR,
                self._stats.last_error,
            )
            return ComponentStatus(status="down", last_error=reason)
        if not self._trade_poller.is_running:
            return ComponentStatus(status="down", last_error="Trade poller stopped")
        return running_ingestion_status(self._trade_poller, self._health_monitor)

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
            if not self._components_installed:
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
            self._stats.last_error = redact_exception_message(e)
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
        """Build the production components from the settings and install them."""
        components = await build_components(
            self._settings,
            dry_run=self._dry_run,
            on_trade=self.process_trade,
            on_ingestion_state=self.on_ingestion_state,
        )
        self.install_components(components)
        logger.info("All components initialized")

    def install_components(self, components: PipelineComponents) -> None:
        """Run on ``components`` instead of building them from the settings.

        ``start`` builds and installs the production set unless one is already installed, so an
        embedding caller can supply its own connections and collaborators. Connections the
        pipeline did not create (``owns_connections=False``) stay open when it stops.
        """
        self._redis = components.redis
        self._db_manager = components.db_manager
        self._owns_connections = components.owns_connections
        self._metadata_sync = components.metadata_sync
        self._funding_tracer = components.funding_tracer
        self._fresh_wallet_detector = components.fresh_wallet_detector
        self._size_anomaly_detector = components.size_anomaly_detector
        self._detector_config = components.detector_config
        self._risk_scorer = components.risk_scorer
        self._alert_formatter = components.alert_formatter
        self._alert_dispatcher = components.alert_dispatcher
        self._trade_poller = components.trade_poller
        self._health_monitor.bind_scorer(components.risk_scorer, logger)
        self._components_installed = True

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

    def _handle_worker_failure(
        self, error: str, worker: str = terminal.DEFAULT_TERMINAL_WORKER
    ) -> None:
        """Handle background worker crash or terminal failure."""
        terminal.record_terminal_failure(self, error, worker)

    def enter_terminal_state(self) -> None:
        """Move to ERROR; bookkeeping lives in terminal_errors."""
        self._state = PipelineState.ERROR

    async def _drain_terminal_writes(self) -> None:
        """Await retained failure writes across stop/shutdown. Never raises."""
        await terminal.drain_terminal_writes(self.terminal_writes, self.record_processing_error)

    async def _run_trade_poller(self) -> None:
        """Run the poller in a task; a terminal failure is reported through the state callback."""
        if not self._trade_poller:
            return
        try:
            await self._trade_poller.start()
        except asyncio.CancelledError:
            logger.debug("Trade poller task cancelled")
        except Exception as e:
            logger.error("Trade poller error: %s", redact_exception_message(e))
            self._handle_worker_failure(redact_exception_message(e))

    async def _run_metadata_sync(self) -> None:
        """Run the initial metadata crawl without delaying acquisition."""
        if not self._metadata_sync:
            return
        try:
            await self._metadata_sync.start()
        except asyncio.CancelledError:
            logger.debug("Metadata sync task cancelled")
        except Exception as e:
            logger.error("Metadata sync error: %s", redact_exception_message(e))
            self._stats.last_error = redact_exception_message(e)
            self._stats.errors += 1

    def on_ingestion_state(self, state: IngestionState) -> None:
        """Record terminal ingestion failures on the pipeline statistics."""
        if state is not IngestionState.FAILED or not self._trade_poller:
            return
        terminal.note_poller_failure(self, self._trade_poller.status.last_error)

    async def _stop_background_services(self) -> None:
        """Stop the poller and its task before the metadata task and sync."""
        if self._trade_poller:
            logger.debug("Stopping trade poller...")
            await self._trade_poller.stop()
        await terminal.cancel_task(self._poller_task)
        await terminal.cancel_task(self._metadata_task)
        self._poller_task = None
        self._metadata_task = None
        await self._drain_terminal_writes()
        if self._metadata_sync:
            logger.debug("Stopping metadata sync...")
            await self._metadata_sync.stop()
        if self._health_monitor:
            await self._health_monitor.stop()

    async def _cleanup(self) -> None:
        """Clean up resources."""
        await self._drain_terminal_writes()
        await self._health_monitor.stop()

        if self._owns_connections:
            await self._close_connections()

        logger.debug("Resources cleaned up")

    async def _close_connections(self) -> None:
        """Close the connections the pipeline created; the next start builds them afresh."""
        if self._db_manager:
            await self._db_manager.dispose_async()
            self._db_manager = None

        if self._redis:
            await self._redis.aclose()
            self._redis = None

        self._components_installed = False

    async def process_trade(self, trade: TradeEvent) -> None:
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
            self._record_processing_error(
                f"Error processing trade {trade.trade_id}: {redact_exception_message(e)}"
            )

    async def _detect_score_and_alert(self, trade: TradeEvent) -> None:
        """Run detectors, count their failures, then score/alert or persist a skip row."""
        evidence = await self._evaluate(trade)
        bundle = _signal_bundle(trade, evidence)
        fresh_signal = bundle.fresh_wallet_signal

        # Persist wallet profile and funding data when a fresh wallet is detected
        if fresh_signal is not None:
            await self._persist_wallet_and_funding(fresh_signal)

        if fresh_signal or bundle.size_anomaly_signal:
            self._stats.signals_generated += 1
            await self._score_and_alert(bundle)
        elif evidence.failures:
            await self._persist_detector_failure_skip(bundle)

    async def _evaluate(self, trade: TradeEvent) -> TradeEvidence:
        """Run both detectors in parallel at one evaluation instant; count their failures."""
        # One evaluation instant per trade, shared by both detectors (detector-api.md section 2)
        evaluated_at = read_evaluation_instant(self._clock)
        (wallet, fresh_error), (size, size_error) = await asyncio.gather(
            self._detect_fresh_wallet(trade, evaluated_at),
            self._detect_size_anomaly(trade, evaluated_at),
        )
        errors = self._record_detector_failures(fresh_error, size_error)
        failures = tuple(error.recorded() for error in errors)
        return TradeEvidence(evaluated_at, self._detector_config, wallet, size, failures)

    def _record_detector_failures(self, *errors: DetectorError | None) -> tuple[DetectorError, ...]:
        """Count each failed detector and expose the failure in operational state."""
        failed = tuple(error for error in errors if error is not None)
        for error in failed:
            self._stats.errors += 1
            self._stats.last_error = redact_text(error.message)
        return failed

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
            # No evaluation evidence (contracts/scoring-config.md section 4): failures only.
            evidence=_failures_only(bundle.evidence),
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
                "Failed to persist wallet/funding data for %s: %s",
                address,
                redact_exception_message(e),
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
        self, trade: TradeEvent, evaluated_at: datetime
    ) -> tuple[WalletEvaluation | None, DetectorError | None]:
        """Run fresh wallet detection; a failure is returned so it can be counted."""
        if not self._fresh_wallet_detector:
            return None, None
        evaluation = self._fresh_wallet_detector.evaluate(trade, evaluated_at=evaluated_at)
        return await _run_detector("fresh_wallet", trade, evaluation)

    async def _detect_size_anomaly(
        self, trade: TradeEvent, evaluated_at: datetime
    ) -> tuple[SizeEvaluation | None, DetectorError | None]:
        """Run size anomaly detection; a failure is returned so it can be counted."""
        if not self._size_anomaly_detector:
            return None, None
        evaluation = self._size_anomaly_detector.evaluate(trade, evaluated_at=evaluated_at)
        return await _run_detector("size_anomaly", trade, evaluation)

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
                f"Failed to record delivery outcome for assessment {assessment.assessment_id}:"
                f" {redact_exception_message(e)}"
            )
            return False

    async def _persist_assessment(self, assessment: RiskAssessment) -> bool:
        """Write the assessment row. Best-effort; never raises.

        Returns True when the row is durably written. A failure is observable
        (counted, surfaced at /health) without blocking delivery (FR-013).
        """
        if not self._db_manager:
            return False
        dto = assessment_dto(
            assessment,
            alert_threshold=self._settings.detector.alert_threshold,
            detector_config=self._detector_config,
        )
        try:
            async with self._db_manager.get_async_session() as session:
                repo = RiskAssessmentRepository(session)
                await repo.insert(dto)
        except Exception as e:
            self._record_processing_error(
                f"Failed to persist risk assessment {assessment.assessment_id}:"
                f" {redact_exception_message(e)}"
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
