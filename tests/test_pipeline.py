"""Tests for the main pipeline orchestrator."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.models import FreshWalletSignal, SizeAnomalySignal
from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from polymarket_insider_tracker.profiler.models import WalletProfile
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel
from tests.fakes import (
    BarrierDetector,
    FailingDetector,
    FakeAlertChannel,
    FakeEth,
    make_test_settings,
    wire_pipeline,
)


@pytest.fixture
def test_settings() -> Settings:
    """Create concrete test settings."""
    return make_test_settings()


@pytest.fixture
def sample_trade_event() -> TradeEvent:
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
def sample_wallet_profile() -> WalletProfile:
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


def _market(trade: TradeEvent, category: str) -> MarketMetadata:
    return MarketMetadata(
        condition_id=trade.market_id,
        question="Test market?",
        description="",
        tokens=(Token(token_id=trade.asset_id, outcome=trade.outcome, price=trade.price),),
        category=category,
    )


@pytest.fixture
def niche_market(sample_trade_event: TradeEvent) -> MarketMetadata:
    """A science market with unknown volume, which the size detector treats as niche."""
    return _market(sample_trade_event, "science")


@pytest.fixture
def mainstream_market(sample_trade_event: TradeEvent) -> MarketMetadata:
    """A politics market with unknown volume, which never produces a size signal."""
    return _market(sample_trade_event, "politics")


async def _persisted_assessments(engine: AsyncEngine) -> list[RiskAssessmentModel]:
    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(RiskAssessmentModel))).scalars().all())


class TestPipelineState:
    """Tests for pipeline state management."""

    def test_initial_state_is_stopped(self, test_settings: Settings) -> None:
        """Pipeline should start in stopped state."""
        pipeline = Pipeline(test_settings)
        assert pipeline.state == PipelineState.STOPPED

    def test_is_running_property(self, test_settings: Settings) -> None:
        """is_running property should reflect state."""
        pipeline = Pipeline(test_settings)
        assert not pipeline.is_running

        pipeline._state = PipelineState.RUNNING
        assert pipeline.is_running


class TestPipelineStats:
    """Tests for pipeline statistics."""

    def test_initial_stats(self, test_settings: Settings) -> None:
        """Pipeline should have zero stats initially."""
        pipeline = Pipeline(test_settings)
        stats = pipeline.stats

        assert stats.started_at is None
        assert stats.trades_processed == 0
        assert stats.signals_generated == 0
        assert stats.alerts_sent == 0
        assert stats.errors == 0


class TestPipelineInitialization:
    """Tests for pipeline initialization."""

    def test_dry_run_from_settings(self) -> None:
        """Pipeline should use dry_run from settings by default."""
        assert Pipeline(make_test_settings(dry_run=True))._dry_run is True
        assert Pipeline(make_test_settings(dry_run=False))._dry_run is False

    def test_dry_run_override(self) -> None:
        """Pipeline should allow overriding dry_run."""
        pipeline = Pipeline(make_test_settings(dry_run=False), dry_run=True)
        assert pipeline._dry_run is True

    def test_settings_helper_applies_detector_options(self) -> None:
        """The test settings helper must really change the aliased detector fields."""
        settings = make_test_settings(alert_threshold=0.4, persist_assessments=False)

        assert settings.detector.alert_threshold == 0.4
        assert settings.detector.persist_assessments is False

    def test_uses_get_settings_when_none_provided(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Pipeline should load settings through get_settings when none are provided."""
        loaded = make_test_settings(dry_run=True)
        monkeypatch.setattr("polymarket_insider_tracker.pipeline.get_settings", lambda: loaded)

        pipeline = Pipeline()

        assert pipeline._settings is loaded
        assert pipeline._dry_run is True


class TestBuildAlertChannels:
    """Tests for alert channel building."""

    def test_no_channels_when_none_enabled(self) -> None:
        """Should return empty list when no channels enabled."""
        settings = make_test_settings(discord_enabled=False, telegram_enabled=False)
        pipeline = Pipeline(settings)
        channels = pipeline._build_alert_channels()

        assert channels == []

    def test_discord_channel_when_enabled(self) -> None:
        """Should add Discord channel when enabled."""
        settings = make_test_settings(
            discord_enabled=True,
            discord_webhook_url="https://discord.com/webhook",
        )
        pipeline = Pipeline(settings)
        channels = pipeline._build_alert_channels()

        assert len(channels) == 1
        assert channels[0].name == "discord"

    def test_telegram_channel_when_enabled(self) -> None:
        """Should add Telegram channel when enabled."""
        settings = make_test_settings(
            telegram_enabled=True,
            telegram_bot_token="bot_token",
            telegram_chat_id="chat_123",
        )
        pipeline = Pipeline(settings)
        channels = pipeline._build_alert_channels()

        assert len(channels) == 1
        assert channels[0].name == "telegram"

    def test_both_channels_when_both_enabled(self) -> None:
        """Should add both channels when both enabled."""
        settings = make_test_settings(
            discord_enabled=True,
            discord_webhook_url="https://discord.com/webhook",
            telegram_enabled=True,
            telegram_bot_token="bot_token",
            telegram_chat_id="chat_123",
        )
        pipeline = Pipeline(settings)
        channels = pipeline._build_alert_channels()

        assert len(channels) == 2


class TestOnTrade:
    """Tests for trade event processing through real detectors."""

    async def test_on_trade_increments_stats(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        sample_trade_event: TradeEvent,
        mainstream_market: MarketMetadata,
    ) -> None:
        """A trade from an established wallet in a mainstream market yields no signal."""
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=FakeEth(transaction_count=100),
            market=mainstream_market,
        )

        await pipeline._on_trade(sample_trade_event)

        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.last_trade_time is not None
        assert pipeline.stats.signals_generated == 0
        assert pipeline.stats.errors == 0

    async def test_on_trade_runs_detectors_in_parallel(
        self, test_settings: Settings, sample_trade_event: TradeEvent
    ) -> None:
        """Both detectors must be in flight at once; sequential execution would deadlock."""
        pipeline = Pipeline(test_settings)
        barrier = asyncio.Barrier(2)
        pipeline._fresh_wallet_detector = BarrierDetector(barrier)
        pipeline._size_anomaly_detector = BarrierDetector(barrier)

        await asyncio.wait_for(pipeline._on_trade(sample_trade_event), timeout=1.0)

        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.errors == 0

    async def test_on_trade_handles_detector_errors(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A failing detector is logged and the other detector's signal still counts."""
        pipeline = await wire_pipeline(
            test_settings, redis=fake_redis, eth=FakeEth(), market=niche_market
        )
        pipeline._fresh_wallet_detector = FailingDetector(RuntimeError("Detector error"))

        with caplog.at_level(logging.WARNING):
            await pipeline._on_trade(sample_trade_event)

        assert "Fresh wallet detection failed" in caplog.text
        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.errors == 0
        assert pipeline.stats.signals_generated == 1

    async def test_on_trade_calls_score_and_alert_when_signals(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        """A brand-new wallet in a niche market is scored, persisted, and alerted."""
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            make_test_settings(alert_threshold=0.4),
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[channel],
        )

        await pipeline._on_trade(sample_trade_event)

        assert pipeline.stats.signals_generated == 1
        assert pipeline.stats.alerts_sent == 1
        rows = await _persisted_assessments(async_engine)
        assert [row.should_alert for row in rows] == [True]
        assert rows[0].signals_triggered == 2
        assert float(rows[0].weighted_score) >= 0.4
        assert [alert.links["wallet"] for alert in channel.deliveries] == [
            f"https://polygonscan.com/address/{sample_trade_event.wallet_address}"
        ]


class TestScoreAndAlert:
    """Tests for scoring and alerting with the real scorer, formatter, and dispatcher."""

    async def test_dry_run_skips_dispatch(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade_event: TradeEvent,
        sample_wallet_profile: WalletProfile,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Dry run formats the alert and logs it but never hands it to a channel."""
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            make_test_settings(dry_run=True, persist_assessments=False),
            redis=fake_redis,
            eth=FakeEth(),
            channels=[channel],
        )
        bundle = SignalBundle(
            trade_event=sample_trade_event,
            fresh_wallet_signal=FreshWalletSignal(
                trade_event=sample_trade_event,
                wallet_profile=sample_wallet_profile,
                confidence=1.0,
                factors={},
            ),
            size_anomaly_signal=SizeAnomalySignal(
                trade_event=sample_trade_event,
                market_metadata=_market(sample_trade_event, "science"),
                volume_impact=0.0,
                book_impact=0.0,
                is_niche_market=True,
                confidence=1.0,
                factors={},
            ),
        )

        with caplog.at_level(logging.INFO):
            await pipeline._score_and_alert(bundle)

        assert "[DRY RUN] Would send alert" in caplog.text
        assert channel.deliveries == []
        assert pipeline.stats.alerts_sent == 0

    async def test_no_alert_when_below_threshold(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        sample_wallet_profile: WalletProfile,
    ) -> None:
        """A sub-threshold assessment is persisted and nothing is delivered."""
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
            channels=[channel],
        )
        bundle = SignalBundle(
            trade_event=sample_trade_event,
            fresh_wallet_signal=FreshWalletSignal(
                trade_event=sample_trade_event,
                wallet_profile=sample_wallet_profile,
                confidence=0.5,
                factors={},
            ),
        )

        await pipeline._score_and_alert(bundle)

        rows = await _persisted_assessments(async_engine)
        assert [(row.should_alert, float(row.weighted_score)) for row in rows] == [(False, 0.2)]
        assert channel.deliveries == []
        assert pipeline.stats.alerts_sent == 0


class TestPipelineLifecycle:
    """Tests for pipeline lifecycle methods."""

    async def test_cannot_start_when_not_stopped(self, test_settings: Settings) -> None:
        """Should raise error when starting non-stopped pipeline."""
        pipeline = Pipeline(test_settings)
        pipeline._state = PipelineState.RUNNING

        with pytest.raises(RuntimeError, match="Cannot start pipeline"):
            await pipeline.start()

    async def test_stop_when_already_stopped(self, test_settings: Settings) -> None:
        """Stop should be no-op when already stopped."""
        pipeline = Pipeline(test_settings)
        assert pipeline.state == PipelineState.STOPPED

        # Should not raise
        await pipeline.stop()
        assert pipeline.state == PipelineState.STOPPED


class TestPipelineContextManager:
    """Tests for async context manager."""

    async def test_context_manager_calls_start_and_stop(
        self, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Entering the context starts the pipeline and leaving it stops the pipeline."""
        pipeline = Pipeline(test_settings)
        transitions: list[str] = []

        async def start() -> None:
            transitions.append("start")

        async def stop() -> None:
            transitions.append("stop")

        monkeypatch.setattr(pipeline, "start", start)
        monkeypatch.setattr(pipeline, "stop", stop)

        async with pipeline as entered:
            assert entered is pipeline
            assert transitions == ["start"]

        assert transitions == ["start", "stop"]
