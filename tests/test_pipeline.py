"""Tests for the main pipeline orchestrator."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.models import FreshWalletSignal
from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from polymarket_insider_tracker.profiler.models import WalletProfile


@pytest.fixture
def mock_settings():
    """Create mock settings for testing."""
    # Create nested mock objects
    redis = MagicMock()
    redis.url = "redis://localhost:6379"

    database = MagicMock()
    database.url = "postgresql+asyncpg://user:pass@localhost/db"

    polygon = MagicMock()
    polygon.rpc_url = "https://polygon-rpc.com"
    polygon.fallback_rpc_url = None

    ankr = MagicMock()
    ankr.enabled = False
    ankr.api_key = None
    ankr.endpoint = "https://rpc.ankr.com/multichain"
    ankr.blockchain = "polygon"

    polymarket = MagicMock()
    polymarket.ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    polymarket.api_key = None

    discord = MagicMock()
    discord.enabled = False
    discord.webhook_url = None

    telegram = MagicMock()
    telegram.enabled = False
    telegram.bot_token = None
    telegram.chat_id = None

    detector = MagicMock()
    detector.persist_assessments = False
    detector.alert_threshold = 0.8
    detector.tail_bet_enabled = False

    settings = MagicMock(spec=Settings)
    settings.redis = redis
    settings.database = database
    settings.polygon = polygon
    settings.ankr = ankr
    settings.polymarket = polymarket
    settings.discord = discord
    settings.telegram = telegram
    settings.detector = detector
    settings.dry_run = True
    return settings


@pytest.fixture
def sample_trade_event():
    """Create a sample trade event for testing."""
    return TradeEvent(
        trade_id="0x" + "a" * 64,
        wallet_address="0x" + "b" * 40,
        market_id="0x" + "c" * 64,
        asset_id="asset_123",
        side="BUY",
        price=Decimal("0.65"),
        size=Decimal("5000"),
        timestamp=datetime.now(UTC),
        outcome="Yes",
        outcome_index=0,
        event_title="Test Market",
        market_slug="test-market",
    )


@pytest.fixture
def sample_wallet_profile():
    """Create a sample wallet profile for testing."""
    return WalletProfile(
        address="0x" + "b" * 40,
        nonce=2,
        first_seen=datetime.now(UTC),
        age_hours=1.5,
        is_fresh=True,
        total_tx_count=2,
        matic_balance=Decimal("100"),
        usdc_balance=Decimal("5000"),
        fresh_threshold=5,
    )


class TestPipelineState:
    """Tests for pipeline state management."""

    def test_initial_state_is_stopped(self, mock_settings):
        """Pipeline should start in stopped state."""
        pipeline = Pipeline(mock_settings)
        assert pipeline.state == PipelineState.STOPPED

    def test_is_running_property(self, mock_settings):
        """is_running property should reflect state."""
        pipeline = Pipeline(mock_settings)
        assert not pipeline.is_running

        pipeline._state = PipelineState.RUNNING
        assert pipeline.is_running


class TestPipelineStats:
    """Tests for pipeline statistics."""

    def test_initial_stats(self, mock_settings):
        """Pipeline should have zero stats initially."""
        pipeline = Pipeline(mock_settings)
        stats = pipeline.stats

        assert stats.started_at is None
        assert stats.trades_processed == 0
        assert stats.signals_generated == 0
        assert stats.alerts_sent == 0
        assert stats.errors == 0


class TestPipelineInitialization:
    """Tests for pipeline initialization."""

    def test_dry_run_from_settings(self, mock_settings):
        """Pipeline should use dry_run from settings by default."""
        mock_settings.dry_run = True
        pipeline = Pipeline(mock_settings)
        assert pipeline._dry_run is True

        mock_settings.dry_run = False
        pipeline = Pipeline(mock_settings)
        assert pipeline._dry_run is False

    def test_dry_run_override(self, mock_settings):
        """Pipeline should allow overriding dry_run."""
        mock_settings.dry_run = False
        pipeline = Pipeline(mock_settings, dry_run=True)
        assert pipeline._dry_run is True

    def test_uses_get_settings_when_none_provided(self):
        """Pipeline should call get_settings if no settings provided."""
        with patch("polymarket_insider_tracker.pipeline.get_settings") as mock_get:
            mock_get.return_value = MagicMock(spec=Settings)
            mock_get.return_value.dry_run = False
            Pipeline()
            mock_get.assert_called_once()


class TestBuildAlertChannels:
    """Tests for alert channel building."""

    def test_no_channels_when_none_enabled(self, mock_settings):
        """Should return empty list when no channels enabled."""
        mock_settings.discord.enabled = False
        mock_settings.telegram.enabled = False

        pipeline = Pipeline(mock_settings)
        channels = pipeline._build_alert_channels()

        assert channels == []

    def test_discord_channel_when_enabled(self, mock_settings):
        """Should add Discord channel when enabled."""
        mock_settings.discord.enabled = True
        mock_settings.discord.webhook_url = MagicMock()
        mock_settings.discord.webhook_url.get_secret_value.return_value = (
            "https://discord.com/webhook"
        )

        pipeline = Pipeline(mock_settings)
        channels = pipeline._build_alert_channels()

        assert len(channels) == 1
        assert channels[0].name == "discord"

    def test_telegram_channel_when_enabled(self, mock_settings):
        """Should add Telegram channel when enabled."""
        mock_settings.telegram.enabled = True
        mock_settings.telegram.bot_token = MagicMock()
        mock_settings.telegram.bot_token.get_secret_value.return_value = "bot_token"
        mock_settings.telegram.chat_id = "chat_123"

        pipeline = Pipeline(mock_settings)
        channels = pipeline._build_alert_channels()

        assert len(channels) == 1
        assert channels[0].name == "telegram"

    def test_both_channels_when_both_enabled(self, mock_settings):
        """Should add both channels when both enabled."""
        mock_settings.discord.enabled = True
        mock_settings.discord.webhook_url = MagicMock()
        mock_settings.discord.webhook_url.get_secret_value.return_value = (
            "https://discord.com/webhook"
        )
        mock_settings.telegram.enabled = True
        mock_settings.telegram.bot_token = MagicMock()
        mock_settings.telegram.bot_token.get_secret_value.return_value = "bot_token"
        mock_settings.telegram.chat_id = "chat_123"

        pipeline = Pipeline(mock_settings)
        channels = pipeline._build_alert_channels()

        assert len(channels) == 2


class TestOnTrade:
    """Tests for trade event processing."""

    @pytest.mark.asyncio
    async def test_on_trade_increments_stats(self, mock_settings, sample_trade_event):
        """Processing a trade should increment stats."""
        pipeline = Pipeline(mock_settings)
        pipeline._fresh_wallet_detector = AsyncMock(return_value=None)
        pipeline._size_anomaly_detector = AsyncMock(return_value=None)

        await pipeline._on_trade(sample_trade_event)

        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.last_trade_time is not None

    @pytest.mark.asyncio
    async def test_on_trade_runs_detectors_in_parallel(self, mock_settings, sample_trade_event):
        """Detectors should run in parallel."""
        pipeline = Pipeline(mock_settings)
        pipeline._fresh_wallet_detector = AsyncMock()
        pipeline._size_anomaly_detector = AsyncMock()

        # Make detectors take some time
        async def slow_detect(*_args):
            await asyncio.sleep(0.1)
            return None

        pipeline._fresh_wallet_detector.analyze = slow_detect
        pipeline._size_anomaly_detector.analyze = slow_detect

        start = asyncio.get_event_loop().time()
        await pipeline._on_trade(sample_trade_event)
        elapsed = asyncio.get_event_loop().time() - start

        # Should complete in ~0.1s not ~0.2s
        assert elapsed < 0.15

    @pytest.mark.asyncio
    async def test_on_trade_handles_detector_errors(self, mock_settings, sample_trade_event):
        """Should handle detector errors gracefully."""
        pipeline = Pipeline(mock_settings)
        pipeline._fresh_wallet_detector = MagicMock()
        pipeline._fresh_wallet_detector.analyze = AsyncMock(side_effect=Exception("Detector error"))
        pipeline._size_anomaly_detector = MagicMock()
        pipeline._size_anomaly_detector.analyze = AsyncMock(return_value=None)

        # Should not raise
        await pipeline._on_trade(sample_trade_event)

        # Should still increment trades processed
        assert pipeline.stats.trades_processed == 1

    @pytest.mark.asyncio
    async def test_on_trade_calls_score_and_alert_when_signals(
        self, mock_settings, sample_trade_event, sample_wallet_profile
    ):
        """Should call score_and_alert when signals are detected."""
        pipeline = Pipeline(mock_settings)

        # Create a signal
        fresh_signal = FreshWalletSignal(
            trade_event=sample_trade_event,
            wallet_profile=sample_wallet_profile,
            confidence=0.8,
            factors={"base": 0.5, "brand_new": 0.2},
        )

        pipeline._fresh_wallet_detector = MagicMock()
        pipeline._fresh_wallet_detector.analyze = AsyncMock(return_value=fresh_signal)
        pipeline._size_anomaly_detector = MagicMock()
        pipeline._size_anomaly_detector.analyze = AsyncMock(return_value=None)

        # Mock score_and_alert
        pipeline._score_and_alert = AsyncMock()

        await pipeline._on_trade(sample_trade_event)

        # Should call score_and_alert with the bundle
        pipeline._score_and_alert.assert_called_once()
        bundle = pipeline._score_and_alert.call_args[0][0]
        assert bundle.fresh_wallet_signal == fresh_signal
        assert pipeline.stats.signals_generated == 1


class TestScoreAndAlert:
    """Tests for scoring and alerting."""

    @pytest.mark.asyncio
    async def test_dry_run_skips_dispatch(
        self, mock_settings, sample_trade_event, sample_wallet_profile
    ):
        """Dry run should skip actual alert dispatch."""
        mock_settings.dry_run = True
        pipeline = Pipeline(mock_settings)

        # Create mock components
        pipeline._risk_scorer = MagicMock()
        pipeline._risk_scorer.assess = AsyncMock(
            return_value=MagicMock(
                should_alert=True,
                wallet_address="0x" + "b" * 40,
                weighted_score=0.85,
            )
        )
        pipeline._alert_formatter = MagicMock()
        pipeline._alert_dispatcher = MagicMock()
        pipeline._alert_dispatcher.dispatch = AsyncMock()

        bundle = SignalBundle(
            trade_event=sample_trade_event,
            fresh_wallet_signal=FreshWalletSignal(
                trade_event=sample_trade_event,
                wallet_profile=sample_wallet_profile,
                confidence=0.8,
                factors={},
            ),
        )

        await pipeline._score_and_alert(bundle)

        # Dispatcher should NOT be called in dry run
        pipeline._alert_dispatcher.dispatch.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_when_below_threshold(self, mock_settings, sample_trade_event):
        """Should not alert when below threshold."""
        pipeline = Pipeline(mock_settings)

        # Create mock components
        pipeline._risk_scorer = MagicMock()
        pipeline._risk_scorer.assess = AsyncMock(
            return_value=MagicMock(
                should_alert=False,
                weighted_score=0.4,
            )
        )
        pipeline._alert_formatter = MagicMock()
        pipeline._alert_dispatcher = MagicMock()

        bundle = SignalBundle(trade_event=sample_trade_event)

        await pipeline._score_and_alert(bundle)

        # Formatter should NOT be called
        pipeline._alert_formatter.format.assert_not_called()


class TestPipelineLifecycle:
    """Tests for pipeline lifecycle methods."""

    @pytest.mark.asyncio
    async def test_cannot_start_when_not_stopped(self, mock_settings):
        """Should raise error when starting non-stopped pipeline."""
        pipeline = Pipeline(mock_settings)
        pipeline._state = PipelineState.RUNNING

        with pytest.raises(RuntimeError, match="Cannot start pipeline"):
            await pipeline.start()

    @pytest.mark.asyncio
    async def test_stop_when_already_stopped(self, mock_settings):
        """Stop should be no-op when already stopped."""
        pipeline = Pipeline(mock_settings)
        assert pipeline.state == PipelineState.STOPPED

        # Should not raise
        await pipeline.stop()
        assert pipeline.state == PipelineState.STOPPED


class TestPipelineContextManager:
    """Tests for async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager_calls_start_and_stop(self, mock_settings):
        """Context manager should call start and stop."""
        pipeline = Pipeline(mock_settings)
        pipeline.start = AsyncMock()
        pipeline.stop = AsyncMock()

        async with pipeline:
            pipeline.start.assert_called_once()

        pipeline.stop.assert_called_once()
