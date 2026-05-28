"""Main pipeline orchestrator for Polymarket Insider Tracker.

This module provides the Pipeline class that wires together all detection
components and manages the event flow from ingestion to alerting.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from redis.asyncio import Redis

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.channels.telegram import TelegramChannel
from polymarket_insider_tracker.alerter.dispatcher import AlertChannel, AlertDispatcher
from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.config import Settings, get_settings
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.detector.tail_bet import TailBetDetector
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.websocket import TradeStreamHandler
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
from polymarket_insider_tracker.profiler.ankr_client import AnkrClient
from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.funding import FundingTracer
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

    from polymarket_insider_tracker.detector.models import (
        FreshWalletSignal,
        RiskAssessment,
        SizeAnomalySignal,
        TailBetSignal,
    )
    from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)


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
        WebSocket Trade Stream → Wallet Profiler → Detectors → Risk Scorer → Alerter

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
        self._ankr_client: AnkrClient | None = None
        self._clob_client: ClobClient | None = None
        self._metadata_sync: MarketMetadataSync | None = None
        self._wallet_analyzer: WalletAnalyzer | None = None
        self._fresh_wallet_detector: FreshWalletDetector | None = None
        self._size_anomaly_detector: SizeAnomalyDetector | None = None
        self._tail_bet_detector: TailBetDetector | None = None
        self._risk_scorer: RiskScorer | None = None
        self._alert_formatter: AlertFormatter | None = None
        self._alert_dispatcher: AlertDispatcher | None = None
        self._trade_stream: TradeStreamHandler | None = None
        self._funding_tracer: FundingTracer | None = None

        # Synchronization
        self._stop_event: asyncio.Event | None = None
        self._stream_task: asyncio.Task[None] | None = None

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
            self._state = PipelineState.RUNNING
            logger.info("Pipeline started successfully")
        except Exception as e:
            self._state = PipelineState.ERROR
            self._stats.last_error = str(e)
            logger.error("Failed to start pipeline: %s", e)
            await self._cleanup()
            raise

    async def stop(self) -> None:
        """Stop the pipeline gracefully.

        Stops all background services and cleans up resources.
        """
        if self._state == PipelineState.STOPPED:
            return

        self._state = PipelineState.STOPPING
        logger.info("Stopping pipeline...")

        if self._stop_event:
            self._stop_event.set()

        await self._stop_background_services()
        await self._cleanup()

        self._state = PipelineState.STOPPED
        logger.info("Pipeline stopped")

    async def _initialize_components(self) -> None:
        """Initialize all pipeline components."""
        settings = self._settings

        # Initialize Redis
        logger.debug("Initializing Redis connection...")
        self._redis = Redis.from_url(settings.redis.url)

        # Initialize Database Manager
        logger.debug("Initializing database manager...")
        self._db_manager = DatabaseManager(
            settings.database.url,
            async_mode=True,
        )

        # Initialize Polygon client
        logger.debug("Initializing Polygon client...")
        # Optional Ankr Advanced API client for first-transaction lookups
        # (used to populate WalletProfile.age_hours). Silently disabled when
        # ANKR_API_KEY is not set.
        if settings.ankr.enabled and settings.ankr.api_key is not None:
            self._ankr_client = AnkrClient(
                api_key=settings.ankr.api_key.get_secret_value(),
                endpoint=settings.ankr.endpoint,
                blockchain=settings.ankr.blockchain,
            )
            logger.info(
                "Ankr Advanced API enabled for first-tx lookups (chain=%s)",
                settings.ankr.blockchain,
            )
        else:
            logger.info("Ankr Advanced API not configured; wallet_age_hours will be None")

        self._polygon_client = PolygonClient(
            settings.polygon.rpc_url,
            fallback_rpc_url=settings.polygon.fallback_rpc_url,
            redis=self._redis,
            ankr_client=self._ankr_client,
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
        if settings.detector.tail_bet_enabled:
            from decimal import Decimal as _D

            self._tail_bet_detector = TailBetDetector(
                self._metadata_sync,
                max_price=_D(str(settings.detector.tail_bet_max_price)),
                min_payout_usdc=_D(str(settings.detector.tail_bet_min_payout_usdc)),
            )
            logger.info(
                "TailBetDetector enabled: max_price=%.4f, min_payout=%.0f",
                settings.detector.tail_bet_max_price,
                settings.detector.tail_bet_min_payout_usdc,
            )
        else:
            logger.info("TailBetDetector disabled by config")

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
        self._alert_dispatcher = AlertDispatcher(channels)

        # Initialize Trade Stream
        logger.debug("Initializing trade stream handler...")
        self._trade_stream = TradeStreamHandler(
            on_trade=self._on_trade,
            host=settings.polymarket.ws_url,
        )

        logger.info("All components initialized")

    def _build_alert_channels(self) -> list[AlertChannel]:
        """Build list of enabled alert channels."""
        channels: list[AlertChannel] = []
        settings = self._settings

        if settings.discord.enabled and settings.discord.webhook_url:
            webhook_url = settings.discord.webhook_url.get_secret_value()
            channels.append(DiscordChannel(webhook_url))
            logger.info("Discord channel enabled")

        if settings.telegram.enabled:
            bot_token = settings.telegram.bot_token
            chat_id = settings.telegram.chat_id
            if bot_token and chat_id:
                channels.append(
                    TelegramChannel(
                        bot_token.get_secret_value(),
                        chat_id,
                    )
                )
                logger.info("Telegram channel enabled")

        if not channels:
            logger.warning("No alert channels configured")

        return channels

    async def _start_background_services(self) -> None:
        """Start background services."""
        # Start metadata sync
        if self._metadata_sync:
            logger.debug("Starting metadata sync service...")
            await self._metadata_sync.start()

        # Start trade stream in background task
        if self._trade_stream:
            logger.debug("Starting trade stream...")
            self._stream_task = asyncio.create_task(self._run_trade_stream())

    async def _run_trade_stream(self) -> None:
        """Run the trade stream in a task."""
        if not self._trade_stream:
            return

        try:
            await self._trade_stream.start()
        except asyncio.CancelledError:
            logger.debug("Trade stream task cancelled")
        except Exception as e:
            logger.error("Trade stream error: %s", e)
            self._stats.last_error = str(e)
            self._stats.errors += 1

    async def _stop_background_services(self) -> None:
        """Stop background services."""
        # Stop trade stream
        if self._trade_stream:
            logger.debug("Stopping trade stream...")
            await self._trade_stream.stop()

        # Cancel stream task
        if self._stream_task:
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task
            self._stream_task = None

        # Stop metadata sync
        if self._metadata_sync:
            logger.debug("Stopping metadata sync...")
            await self._metadata_sync.stop()

    async def _cleanup(self) -> None:
        """Clean up resources."""
        # Close Ankr HTTP client
        if self._ankr_client:
            await self._ankr_client.aclose()
            self._ankr_client = None

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
            trade: The trade event from the WebSocket stream.
        """
        self._stats.trades_processed += 1
        self._stats.last_trade_time = datetime.now(UTC)

        try:
            # Run detectors in parallel
            fresh_signal, size_signal, tail_signal = await asyncio.gather(
                self._detect_fresh_wallet(trade),
                self._detect_size_anomaly(trade),
                self._detect_tail_bet(trade),
            )

            # Persist wallet profile and funding data when a fresh wallet is detected
            if fresh_signal is not None:
                await self._persist_wallet_and_funding(fresh_signal)

            # Bundle signals
            bundle = SignalBundle(
                trade_event=trade,
                fresh_wallet_signal=fresh_signal,
                size_anomaly_signal=size_signal,
                tail_bet_signal=tail_signal,
            )

            # Score and potentially alert
            if fresh_signal or size_signal or tail_signal:
                self._stats.signals_generated += 1
                await self._score_and_alert(bundle)

        except Exception as e:
            logger.error("Error processing trade %s: %s", trade.trade_id, e)
            self._stats.errors += 1
            self._stats.last_error = str(e)

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
                # Persist wallet profile
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

                # Trace and persist funding transfers
                if self._funding_tracer:
                    chain = await self._funding_tracer.trace(address)
                    if chain.chain:
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

                logger.debug(
                    "Persisted wallet profile and %d funding transfers for %s",
                    len(chain.chain) if self._funding_tracer and chain.chain else 0,
                    address[:10] + "...",
                )
        except Exception as e:
            logger.warning("Failed to persist wallet/funding data for %s: %s", address, e)

    async def _detect_fresh_wallet(self, trade: TradeEvent) -> FreshWalletSignal | None:
        """Run fresh wallet detection."""
        if not self._fresh_wallet_detector:
            return None
        try:
            return await self._fresh_wallet_detector.analyze(trade)
        except Exception as e:
            logger.warning("Fresh wallet detection failed for %s: %s", trade.trade_id, e)
            return None

    async def _detect_size_anomaly(self, trade: TradeEvent) -> SizeAnomalySignal | None:
        """Run size anomaly detection."""
        if not self._size_anomaly_detector:
            return None
        try:
            return await self._size_anomaly_detector.analyze(trade)
        except Exception as e:
            logger.warning("Size anomaly detection failed for %s: %s", trade.trade_id, e)
            return None

    async def _detect_tail_bet(self, trade: TradeEvent) -> TailBetSignal | None:
        """Run tail-bet detection."""
        if not self._tail_bet_detector:
            return None
        try:
            return await self._tail_bet_detector.analyze(trade)
        except Exception as e:
            logger.warning("Tail-bet detection failed for %s: %s", trade.trade_id, e)
            return None

    async def _score_and_alert(self, bundle: SignalBundle) -> None:
        """Score signals, persist the assessment, and send alert if above threshold."""
        if not self._risk_scorer or not self._alert_formatter or not self._alert_dispatcher:
            return

        # Get risk assessment
        assessment = await self._risk_scorer.assess(bundle)

        # Persist every signal-bearing assessment (not just delivered alerts).
        # This is the ground-truth log future backtests will read instead of
        # grepping systemd. Failure here must never block alerting.
        # Defensive guard: caller already filters on (fresh_signal or size_signal),
        # but we double-check so a future refactor can't quietly fill the table
        # with zero-signal noise.
        if self._settings.detector.persist_assessments and assessment.signals_triggered > 0:
            await self._persist_assessment(assessment)

        if not assessment.should_alert:
            logger.debug(
                "Trade %s below alert threshold (score=%.2f)",
                bundle.trade_event.trade_id,
                assessment.weighted_score,
            )
            return

        # Format and dispatch alert
        formatted_alert = self._alert_formatter.format(assessment)

        if self._dry_run:
            logger.info(
                "[DRY RUN] Would send alert: wallet=%s, score=%.2f",
                assessment.wallet_address[:10] + "...",
                assessment.weighted_score,
            )
            return

        result = await self._alert_dispatcher.dispatch(formatted_alert)

        if result.all_succeeded:
            self._stats.alerts_sent += 1
            logger.info(
                "Alert sent successfully: wallet=%s, score=%.2f",
                assessment.wallet_address[:10] + "...",
                assessment.weighted_score,
            )
        else:
            logger.warning(
                "Alert partially failed: %d/%d channels succeeded",
                result.success_count,
                result.success_count + result.failure_count,
            )

    async def _persist_assessment(self, assessment: "RiskAssessment") -> None:
        """Write the assessment row. Best-effort; never raises."""
        if not self._db_manager:
            return
        from decimal import Decimal as _D

        trade = assessment.trade_event
        fresh = assessment.fresh_wallet_signal
        size_sig = assessment.size_anomaly_signal
        tail_sig = assessment.tail_bet_signal
        wallet_age: _D | None = None
        if fresh is not None and fresh.wallet_profile.age_hours is not None:
            wallet_age = _D(str(round(float(fresh.wallet_profile.age_hours), 2)))

        # Niche flag survives if either size_anomaly or tail_bet flagged the
        # market as niche — they share the same threshold so this is just an
        # OR over the two views, not a per-detector field.
        is_niche: bool | None = None
        if size_sig is not None and tail_sig is not None:
            is_niche = bool(size_sig.is_niche_market or tail_sig.is_niche_market)
        elif size_sig is not None:
            is_niche = size_sig.is_niche_market
        elif tail_sig is not None:
            is_niche = tail_sig.is_niche_market

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
            weighted_score=_D(str(round(assessment.weighted_score, 3))),
            signals_triggered=assessment.signals_triggered,
            fresh_wallet_confidence=(
                _D(str(round(fresh.confidence, 3))) if fresh is not None else None
            ),
            size_anomaly_confidence=(
                _D(str(round(size_sig.confidence, 3))) if size_sig is not None else None
            ),
            tail_bet_confidence=(
                _D(str(round(tail_sig.confidence, 3))) if tail_sig is not None else None
            ),
            is_niche_market=is_niche,
            volume_impact=(
                _D(str(round(size_sig.volume_impact, 4))) if size_sig is not None else None
            ),
            book_impact=(
                _D(str(round(size_sig.book_impact, 4))) if size_sig is not None else None
            ),
            wallet_age_hours=wallet_age,
            potential_payout_usdc=(
                tail_sig.potential_payout_usdc if tail_sig is not None else None
            ),
            payout_to_volume_ratio=(
                _D(str(round(tail_sig.payout_to_volume_ratio, 6)))
                if tail_sig is not None
                else None
            ),
            payout_to_notional_ratio=(
                _D(str(round(tail_sig.payout_to_notional_ratio, 4)))
                if tail_sig is not None
                else None
            ),
            should_alert=assessment.should_alert,
            threshold_at_eval=_D(str(round(self._settings.detector.alert_threshold, 3))),
        )
        try:
            async with self._db_manager.get_async_session() as session:
                repo = RiskAssessmentRepository(session)
                await repo.insert(dto)
        except Exception as e:
            logger.warning(
                "Failed to persist risk assessment %s: %s", assessment.assessment_id, e
            )

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
