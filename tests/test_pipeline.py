"""Tests for the main pipeline orchestrator."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.models import FreshWalletSignal, SizeAnomalySignal
from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.ingestor.metadata_sync import SyncState
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState
from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from polymarket_insider_tracker.profiler.models import WalletProfile
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel
from tests.fakes import (
    BarrierDetector,
    FailingDetector,
    FakeAlertChannel,
    FakeClock,
    FakeEth,
    FakeTradesServer,
    make_test_settings,
    metadata_state,
    trade_row,
    wire_pipeline,
)

POLL_START = 1_788_983_720.0


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

    def test_settings_helper_ignores_environment_and_dotenv(self, monkeypatch, tmp_path) -> None:
        expected = make_test_settings().model_dump()
        overrides = {
            "DATABASE_URL": "postgresql+psycopg://other:synthetic@untrusted.invalid/db",
            "REDIS_URL": "redis://untrusted.invalid:6379",
            "POLYGON_RPC_URL": "https://untrusted.invalid",
            "POLYGON_FALLBACK_RPC_URL": "https://fallback.invalid",
            "POLYMARKET_WS_URL": "wss://untrusted.invalid",
            "POLYMARKET_API_KEY": "synthetic-api-key",
            "DISCORD_WEBHOOK_URL": "https://untrusted.invalid/webhook",
            "TELEGRAM_BOT_TOKEN": "synthetic-bot-token",
            "TELEGRAM_CHAT_ID": "synthetic-chat",
            "DETECTOR_ALERT_THRESHOLD": "0.1",
            "DETECTOR_DEDUP_WINDOW_SECONDS": "17",
            "DETECTOR_PERSIST_ASSESSMENTS": "false",
            "LOG_LEVEL": "ERROR",
            "HEALTH_PORT": "9090",
            "DRY_RUN": "true",
        }
        for name, value in overrides.items():
            monkeypatch.setenv(name, value)
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".env").write_text("REDIS_URL=redis://dotenv.invalid:6379\n")

        settings = make_test_settings()

        assert settings.model_dump() == expected
        assert settings.discord.enabled is False
        assert settings.telegram.enabled is False

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

    async def test_on_trade_counts_detector_error_and_still_scores_other_signal(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A failing detector is counted and exposed while the other signal still scores."""
        pipeline = await wire_pipeline(
            test_settings, redis=fake_redis, eth=FakeEth(), market=niche_market
        )
        pipeline._fresh_wallet_detector = FailingDetector(RuntimeError("Detector error"))

        with caplog.at_level(logging.WARNING):
            await pipeline._on_trade(sample_trade_event)

        assert "fresh wallet detection failed" in caplog.text
        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.errors == 1
        assert pipeline.stats.last_error is not None
        assert "Detector error" in pipeline.stats.last_error
        assert pipeline.stats.signals_generated == 1

    async def test_all_detector_failures_persist_skip_disposition(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
    ) -> None:
        """When every detector fails, the absent evidence is durably explained (US3-3)."""
        pipeline = await wire_pipeline(
            make_test_settings(), redis=fake_redis, eth=FakeEth(), db_manager=db_manager
        )
        pipeline._fresh_wallet_detector = FailingDetector(RuntimeError("polygon rpc down"))
        pipeline._size_anomaly_detector = FailingDetector(RuntimeError("metadata down"))

        await pipeline._on_trade(sample_trade_event)

        assert pipeline.stats.errors == 2
        assert pipeline.stats.last_error is not None
        rows = await _persisted_assessments(async_engine)
        assert len(rows) == 1
        assert rows[0].delivery_disposition == "detector_failure"
        assert rows[0].should_alert is False
        assert rows[0].signals_triggered == 0
        assert rows[0].fresh_wallet_confidence is None
        assert rows[0].size_anomaly_confidence is None

    async def test_detector_failure_skip_not_persisted_when_persistence_disabled(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
    ) -> None:
        """The skip row honors the persist_assessments switch; the count still happens."""
        pipeline = await wire_pipeline(
            make_test_settings(persist_assessments=False),
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
        )
        pipeline._fresh_wallet_detector = FailingDetector(RuntimeError("polygon rpc down"))
        pipeline._size_anomaly_detector = FailingDetector(RuntimeError("metadata down"))

        await pipeline._on_trade(sample_trade_event)

        assert pipeline.stats.errors == 2
        assert await _persisted_assessments(async_engine) == []

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


async def _run_until(predicate: Callable[[], bool], *, cycles: int = 25000) -> None:
    for _ in range(cycles):
        if predicate():
            return
        await asyncio.sleep(0.0005)
    raise AssertionError("condition was not reached")


def _seeded_server(clock: FakeClock) -> FakeTradesServer:
    """A provider with two rows of history so the first cycle can anchor and later pages reach."""
    server = FakeTradesServer(clock=clock)
    server.publish(trade_row(timestamp=int(POLL_START) - 1, transaction=1, wallet=1))
    server.publish(trade_row(timestamp=int(POLL_START) - 61, transaction=2, wallet=2))
    return server


def _pipeline_row(sample_trade_event: TradeEvent, clock: FakeClock) -> dict[str, object]:
    """A wallet-bearing row for the sample market, timestamped just inside the next cutoff."""
    row = trade_row(
        timestamp=int(clock.now) + 1,
        transaction=9,
        market=sample_trade_event.market_id,
        asset=sample_trade_event.asset_id,
        price="0.65",
        size="5000",
    )
    row["proxyWallet"] = sample_trade_event.wallet_address
    return row


class TestIngestionWiring:
    """The poller is a background service that starts before the metadata crawl finishes."""

    async def test_poller_delivers_before_a_blocked_metadata_crawl_completes(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        sample_trade_event: TradeEvent,
        mainstream_market: MarketMetadata,
    ) -> None:
        clock = FakeClock(POLL_START)
        server = _seeded_server(clock)
        gate = threading.Event()
        log: list[str] = []
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=FakeEth(transaction_count=100),
            market=mainstream_market,
            trades=server,
            poll_clock=clock,
            crawl_gate=gate,
            state_log=log,
        )

        try:
            await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
            await _run_until(lambda: len(server.requests) >= 1)
            server.publish(_pipeline_row(sample_trade_event, clock))
            await _run_until(lambda: pipeline.stats.trades_processed >= 1)

            assert metadata_state(pipeline) in {SyncState.STARTING, SyncState.SYNCING}
            assert pipeline.stats.trades_processed == 1
            assert pipeline.stats.errors == 0
            assert "poller:running" in log
            assert "metadata:idle" not in log
        finally:
            gate.set()
            await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)

        poller_stopped = log.index("poller:stopped")
        metadata_stopping = log.index("metadata:stopping")
        assert poller_stopped < metadata_stopping
        assert pipeline._trade_poller is not None
        assert pipeline._trade_poller.state is IngestionState.STOPPED

    async def test_polled_observations_reach_detection_scoring_persistence_and_delivery(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        clock = FakeClock(POLL_START)
        server = _seeded_server(clock)
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            make_test_settings(alert_threshold=0.4),
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[channel],
            trades=server,
            poll_clock=clock,
        )

        try:
            await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
            await _run_until(lambda: len(server.requests) >= 1)
            server.publish(_pipeline_row(sample_trade_event, clock))
            await _run_until(lambda: len(channel.deliveries) >= 1)
            await _run_until(lambda: len(server.requests) >= 4)
        finally:
            await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)

        rows = await _persisted_assessments(async_engine)
        assert [row.should_alert for row in rows] == [True]
        assert rows[0].wallet_address == sample_trade_event.wallet_address.lower()
        assert rows[0].trade_id == trade_row(timestamp=0, transaction=9)["transactionHash"]
        assert [alert.links["wallet"] for alert in channel.deliveries] == [
            f"https://polygonscan.com/address/{sample_trade_event.wallet_address}"
        ]
        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.alerts_sent == 1

    async def test_terminal_ingestion_failure_is_recorded_on_pipeline_stats(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
    ) -> None:
        clock = FakeClock(POLL_START)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            test_settings, redis=fake_redis, eth=FakeEth(), trades=server, poll_clock=clock
        )
        assert pipeline._trade_poller is not None
        from tests.fakes import terminal

        server.fail_next(terminal(401))

        await pipeline._trade_poller.run_cycle()

        assert pipeline._trade_poller.state is IngestionState.FAILED
        assert pipeline.stats.errors == 1
        assert pipeline.stats.last_error is not None
        assert "401" in pipeline.stats.last_error
        assert pipeline.state is PipelineState.STOPPED


class TestIngestionRestart:
    """Slice 001's contribution to the end-to-end harness: a restart inside the horizon."""

    async def test_restart_delivers_every_missed_trade_once_and_replays_nothing(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        clock = FakeClock(POLL_START)
        server = _seeded_server(clock)
        first_channel = FakeAlertChannel("discord")
        first = await wire_pipeline(
            make_test_settings(alert_threshold=0.4),
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[first_channel],
            trades=server,
            poll_clock=clock,
        )
        try:
            await asyncio.wait_for(first._start_background_services(), timeout=2.0)
            await _run_until(lambda: len(server.requests) >= 1)
            server.publish(_pipeline_row(sample_trade_event, clock))
            await _run_until(lambda: len(first_channel.deliveries) >= 1)
        finally:
            await asyncio.wait_for(first._stop_background_services(), timeout=2.0)
        assert first._trade_poller is not None
        boundary_at_stop = first._trade_poller.status.boundary_time

        clock.advance(120)
        missed = [
            dict(_pipeline_row(sample_trade_event, clock), timestamp=int(clock.now) - 30 + n)
            for n in range(3)
        ]
        for index, row in enumerate(missed):
            row["transactionHash"] = "0x" + f"{100 + index:064x}"
        server.publish(missed[2])
        server.publish(missed[0])
        server.publish(missed[1])
        second_channel = FakeAlertChannel("discord")
        second = await wire_pipeline(
            make_test_settings(alert_threshold=0.4),
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[second_channel],
            trades=server,
            poll_clock=clock,
        )
        try:
            await asyncio.wait_for(second._start_background_services(), timeout=2.0)
            await _run_until(lambda: second.stats.trades_processed >= 3)
            await _run_until(lambda: len(server.requests) >= 8)
        finally:
            await asyncio.wait_for(second._stop_background_services(), timeout=2.0)

        assert second._trade_poller is not None
        status = second._trade_poller.status
        assert boundary_at_stop is not None
        assert status.boundary_time is not None and status.boundary_time > boundary_at_stop
        assert status.loss_events == ()
        assert second.stats.trades_processed == 3
        assert first.stats.trades_processed == 1
        rows = await _persisted_assessments(async_engine)
        assert sorted(row.trade_id for row in rows) == sorted(
            [trade_row(timestamp=0, transaction=9)["transactionHash"]]
            + [row["transactionHash"] for row in missed]
        )
        assert len(first_channel.deliveries) == 1
        assert status.counts["emitted"] == 3


class TestWorkerSupervision:
    """Tests for background worker failure propagation (G-016, FR-006, SC-002)."""

    async def test_worker_crash_transitions_pipeline_to_error_and_fails_readiness(
        self,
        fake_redis: FakeAsyncRedis,
    ) -> None:
        """When trade poller crashes, pipeline state becomes ERROR and readiness fails."""
        from tests.fakes import terminal

        clock = FakeClock(POLL_START)
        server = FakeTradesServer(clock=clock)
        server.fail_next(terminal(401))

        settings = make_test_settings(health_port=19201)
        pipeline = await wire_pipeline(
            settings,
            redis=fake_redis,
            eth=FakeEth(),
            trades=server,
            poll_clock=clock,
        )
        pipeline._state = PipelineState.RUNNING
        await pipeline._start_background_services()

        try:
            await _run_until(lambda: pipeline.state is PipelineState.ERROR)
            assert pipeline.state is PipelineState.ERROR
            assert pipeline.stats.errors >= 1
            assert pipeline.health_monitor is not None

            ready_status, reason, components = await pipeline.check_readiness()
            assert ready_status is False
            assert components.get("ingestion") == "down"
        finally:
            await pipeline._stop_background_services()

    async def test_worker_failure_during_startup_transitions_to_error(self) -> None:
        """A terminal worker failure while still STARTING must reach ERROR, not stay hidden."""
        pipeline = Pipeline(make_test_settings())
        pipeline._state = PipelineState.STARTING

        pipeline._handle_worker_failure("terminal poller failure during startup")

        assert pipeline.state is PipelineState.ERROR
        assert pipeline.stats.errors == 1
        assert pipeline.stats.last_error == "terminal poller failure during startup"


class _StubPollerStatus:
    def __init__(self, last_error: str | None, last_success_at: datetime | None) -> None:
        self.last_error = last_error
        self.last_acquisition_at = None
        self.last_success_at = last_success_at
        self.last_trade_at = None


class _StubPoller:
    """Just enough poller surface for `_check_ingestion` state-mapping tests."""

    def __init__(
        self,
        state: IngestionState,
        last_error: str | None = None,
        seconds_since_last_success: float | None = 1.0,
    ) -> None:
        self.state = state
        self.is_running = True
        self.seconds_since_last_success = seconds_since_last_success
        last_success = datetime.now(UTC) if seconds_since_last_success is not None else None
        self.status = _StubPollerStatus(last_error, last_success)


class TestIngestionComponentTruthfulness:
    """Readiness gates on proven, recent successful acquisition; degraded and
    possible-data-loss states stay visible instead of being reported "up" (FR-002/003)."""

    async def test_degraded_poller_with_recent_success_maps_to_degraded_with_error(self) -> None:
        pipeline = Pipeline(make_test_settings())
        pipeline._trade_poller = _StubPoller(  # type: ignore[assignment]
            IngestionState.DEGRADED, last_error="acquisition cycle failed: 503"
        )

        component = await pipeline._check_ingestion()

        assert component.status == "degraded"
        assert component.last_error == "acquisition cycle failed: 503"

    async def test_possible_data_loss_maps_to_degraded_with_state_fallback_error(self) -> None:
        pipeline = Pipeline(make_test_settings())
        pipeline._trade_poller = _StubPoller(IngestionState.POSSIBLE_DATA_LOSS)  # type: ignore[assignment]

        component = await pipeline._check_ingestion()

        assert component.status == "degraded"
        assert component.last_error == "ingestion state: possible-data-loss"

    async def test_degraded_ingestion_keeps_readiness_but_is_reported(self) -> None:
        """FR-002: readiness fails only for unavailable/terminal states; degraded is visible."""
        pipeline = Pipeline(make_test_settings())
        pipeline._trade_poller = _StubPoller(  # type: ignore[assignment]
            IngestionState.DEGRADED, last_error="acquisition cycle failed: 503"
        )
        pipeline._redis = FakeAsyncRedis()
        db = _HealthyDatabaseStub()
        pipeline._db_manager = db  # type: ignore[assignment]

        ready_status, reason, components = await pipeline.check_readiness()

        assert ready_status is True
        assert reason is None
        assert components["ingestion"] == "degraded"

    async def test_running_poller_with_recent_success_still_reports_up(self) -> None:
        pipeline = Pipeline(make_test_settings())
        pipeline._trade_poller = _StubPoller(IngestionState.RUNNING)  # type: ignore[assignment]

        component = await pipeline._check_ingestion()

        assert component.status == "up"
        assert component.last_error is None


class TestReadinessRequiresSuccessfulAcquisition:
    """A running poller that has never reached the source, or whose last success is
    stale, is not ready: request start is not resource acquisition (FR-002)."""

    async def test_starting_poller_that_never_connected_is_down(self) -> None:
        pipeline = Pipeline(make_test_settings())
        pipeline._trade_poller = _StubPoller(  # type: ignore[assignment]
            IngestionState.STARTING, seconds_since_last_success=None
        )

        component = await pipeline._check_ingestion()

        assert component.status == "down"
        assert "successful acquisition" in (component.last_error or "")

    async def test_degraded_poller_whose_first_request_failed_is_down(self) -> None:
        """The round-4 reproduction: a first-request connection failure stayed ready."""
        pipeline = Pipeline(make_test_settings())
        pipeline._trade_poller = _StubPoller(  # type: ignore[assignment]
            IngestionState.DEGRADED,
            last_error="acquisition cycle failed: connection refused",
            seconds_since_last_success=None,
        )
        pipeline._redis = FakeAsyncRedis()
        pipeline._db_manager = _HealthyDatabaseStub()  # type: ignore[assignment]

        component = await pipeline._check_ingestion()
        ready_status, reason, components = await pipeline.check_readiness()

        assert component.status == "down"
        assert "connection refused" in (component.last_error or "")
        assert ready_status is False
        assert reason == "ingestion_worker_failed"
        assert components["ingestion"] == "down"

    async def test_stale_success_is_down_even_while_state_says_running(self) -> None:
        pipeline = Pipeline(make_test_settings())
        pipeline._trade_poller = _StubPoller(  # type: ignore[assignment]
            IngestionState.RUNNING, seconds_since_last_success=120.0
        )

        component = await pipeline._check_ingestion()

        assert component.status == "down"
        assert "last successful acquisition" in (component.last_error or "")

    async def test_stale_success_degraded_source_is_down_not_degraded(self) -> None:
        """An unreachable DEGRADED source (stale success) is distinct from a
        progressing POSSIBLE_DATA_LOSS source (fresh success, covered above)."""
        pipeline = Pipeline(make_test_settings())
        pipeline._trade_poller = _StubPoller(  # type: ignore[assignment]
            IngestionState.DEGRADED,
            last_error="acquisition cycle failed: 503",
            seconds_since_last_success=600.0,
        )

        component = await pipeline._check_ingestion()

        assert component.status == "down"
        assert "acquisition cycle failed: 503" in (component.last_error or "")

    async def test_successful_empty_page_counts_as_acquisition_and_is_up(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        """A quiet market's empty page proves reachability; the source is up."""
        clock = FakeClock(POLL_START)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            make_test_settings(), redis=fake_redis, eth=FakeEth(), trades=server, poll_clock=clock
        )
        assert pipeline._trade_poller is not None

        await pipeline._trade_poller.run_cycle()
        # run_cycle() drives one acquisition without the poller loop; mark the loop
        # running so the check reflects steady-state operation, not lifecycle stop.
        pipeline._trade_poller._running = True
        component = await pipeline._check_ingestion()

        assert pipeline._trade_poller.seconds_since_last_success is not None
        assert component.status == "up"

    async def test_failed_acquisition_never_reports_acquisition_freshness(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        """The health monitor's acquisition time must come from a completed fetch,
        not from the request-start timestamp of a failed attempt."""
        from tests.fakes import server_error

        clock = FakeClock(POLL_START)
        server = FakeTradesServer(clock=clock)
        server.fail_next(server_error(), times=8)
        pipeline = await wire_pipeline(
            make_test_settings(), redis=fake_redis, eth=FakeEth(), trades=server, poll_clock=clock
        )
        assert pipeline._trade_poller is not None

        await pipeline._trade_poller.run_cycle()
        component = await pipeline._check_ingestion()

        assert pipeline._trade_poller.state is IngestionState.DEGRADED
        assert pipeline._trade_poller.status.last_acquisition_at is not None
        assert pipeline.health_monitor.last_acquisition_time is None
        assert component.status == "down"


class _HealthyDatabaseStub:
    """DatabaseManager stand-in whose health probe session succeeds."""

    def get_async_session(self) -> Any:
        import contextlib

        @contextlib.asynccontextmanager
        async def session() -> Any:
            class _Session:
                async def execute(self, _statement: Any) -> None:
                    return None

            yield _Session()

        return session()


class _RowCountingChannel:
    """Channel that records how many assessment rows were durable at send time."""

    def __init__(self, name: str, engine: AsyncEngine) -> None:
        self.name = name
        self._engine = engine
        self.rows_at_send: list[int] = []

    async def send(self, alert: Any) -> bool:
        _ = alert
        self.rows_at_send.append(len(await _persisted_assessments(self._engine)))
        return True


class _ExplodingFormatter:
    """Formatter failure injection: the crash window between scoring and delivery."""

    def format(self, assessment: Any) -> Any:
        _ = assessment
        raise RuntimeError("formatter exploded")


class _FailFirstSessionsDatabaseManager:
    """Delegates to a real manager after failing the first N session acquisitions."""

    def __init__(self, inner: DatabaseManager, failures: int) -> None:
        self._inner = inner
        self.failures_remaining = failures

    def get_async_session(self) -> Any:
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError("database briefly unavailable")
        return self._inner.get_async_session()


class TestAssessmentDurabilityOrdering:
    """A qualifying assessment must be durable before any delivery work and can
    never be lost between computation and persistence (FR-012, round-4 finding 3)."""

    async def test_qualifying_assessment_is_persisted_before_delivery_attempt(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        channel = _RowCountingChannel("discord", async_engine)
        pipeline = await wire_pipeline(
            make_test_settings(alert_threshold=0.4),
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[channel],
        )

        await pipeline._on_trade(sample_trade_event)

        # The pending row was already durable when the channel was contacted...
        assert channel.rows_at_send == [1]
        # ...and the same single row now carries the final delivery outcome.
        rows = await _persisted_assessments(async_engine)
        assert [row.delivery_disposition for row in rows] == ["delivered"]
        assert pipeline.stats.errors == 0

    async def test_formatter_failure_does_not_lose_the_assessment(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        """The round-4 reproduction: a formatter exception used to leave persist_calls=0."""
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            make_test_settings(alert_threshold=0.4),
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[channel],
        )
        pipeline._alert_formatter = _ExplodingFormatter()  # type: ignore[assignment]

        await pipeline._on_trade(sample_trade_event)

        rows = await _persisted_assessments(async_engine)
        assert len(rows) == 1
        assert rows[0].should_alert is True
        # No delivery outcome exists yet, so the row truthfully stays unrecorded.
        assert rows[0].delivery_disposition == "unrecorded"
        assert channel.deliveries == []
        assert pipeline.stats.errors == 1
        assert "formatter exploded" in (pipeline.stats.last_error or "")

    async def test_initial_persist_failure_is_observable_and_recovered_after_delivery(
        self,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        """FR-013: the pending write failing must not block delivery, must be counted,
        and the post-delivery write must still produce the durable record."""
        channel = FakeAlertChannel("discord")
        # The first two sessions serve the wallet/funding write and the pending
        # assessment write; both fail, then the database recovers.
        flaky = _FailFirstSessionsDatabaseManager(db_manager, failures=2)
        pipeline = await wire_pipeline(
            make_test_settings(alert_threshold=0.4),
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[channel],
        )
        pipeline._db_manager = flaky  # type: ignore[assignment]

        await pipeline._on_trade(sample_trade_event)

        assert len(channel.deliveries) == 1
        assert pipeline.stats.errors == 1
        assert "Failed to persist risk assessment" in (pipeline.stats.last_error or "")
        rows = await _persisted_assessments(async_engine)
        assert [row.delivery_disposition for row in rows] == ["delivered"]


class TestProcessingErrorVisibility:
    """Per-trade processing failures must be visible in the detailed health report (FR-003)."""

    def test_pipeline_wires_stats_last_error_into_health_body(self) -> None:
        from polymarket_insider_tracker.ingestor.health import HealthStatus

        pipeline = Pipeline(make_test_settings())
        pipeline._stats.last_error = "scoring failed for trade t-42"

        monitor = pipeline.health_monitor
        body = monitor._build_health_body(
            monitor.get_health_report(), {}, HealthStatus.HEALTHY, 0.0
        )

        assert body["last_error"] == "scoring failed for trade t-42"
