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
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.websocket import TradeStreamHandler
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.storage.database import DatabaseManager

if TYPE_CHECKING:
    from typing import Any

    from polymarket_insider_tracker.detector.models import (
        FreshWalletSignal,
        SizeAnomalySignal,
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
        self._clob_client: ClobClient | None = None
        self._metadata_sync: MarketMetadataSync | None = None
        self._wallet_analyzer: WalletAnalyzer | None = None
        self._fresh_wallet_detector: FreshWalletDetector | None = None
        self._size_anomaly_detector: SizeAnomalyDetector | None = None
        self._risk_scorer: RiskScorer | None = None
        self._alert_formatter: AlertFormatter | None = None
        self._alert_dispatcher: AlertDispatcher | None = None
        self._trade_stream: TradeStreamHandler | None = None

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

        # Initialize Detectors
        logger.debug("Initializing detectors...")
        self._fresh_wallet_detector = FreshWalletDetector(self._wallet_analyzer)
        self._size_anomaly_detector = SizeAnomalyDetector(self._metadata_sync)

        # Initialize Risk Scorer
        logger.debug("Initializing risk scorer...")
        self._risk_scorer = RiskScorer(self._redis)

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
            fresh_signal, size_signal = await asyncio.gather(
                self._detect_fresh_wallet(trade),
                self._detect_size_anomaly(trade),
            )

            # Bundle signals
            bundle = SignalBundle(
                trade_event=trade,
                fresh_wallet_signal=fresh_signal,
                size_anomaly_signal=size_signal,
            )

            # Score and potentially alert
            if fresh_signal or size_signal:
                self._stats.signals_generated += 1
                await self._score_and_alert(bundle)

        except Exception as e:
            logger.error("Error processing trade %s: %s", trade.trade_id, e)
            self._stats.errors += 1
            self._stats.last_error = str(e)

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

    async def _score_and_alert(self, bundle: SignalBundle) -> None:
        """Score signals and send alert if threshold exceeded."""
        if not self._risk_scorer or not self._alert_formatter or not self._alert_dispatcher:
            return

        # Get risk assessment
        assessment = await self._risk_scorer.assess(bundle)

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
