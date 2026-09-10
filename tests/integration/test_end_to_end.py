"""Deterministic end-to-end integration tests for the Polymarket Insider Tracker pipeline.

Tests cover:
1. FR-014 / SC-005 / G-021: Complete pipeline execution using deterministic local fakes
   (ingest -> profile -> detect -> score -> persist -> dry-run suppression -> clean shutdown)
2. FR-013: Assessment persistence failure does not prevent alert delivery
3. FR-018 / G-020: Dry-run mode produces zero notification calls and zero Redis keys
4. FR-010 / FR-017: Multi-channel delivery and duplicate suppression
5. FR-006: Worker crash transitions pipeline to ERROR and fails readiness
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp
import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from tests.fakes import (
    FakeAlertChannel,
    FakeClock,
    FakeEth,
    FakeTradesServer,
    make_test_settings,
    terminal,
    trade_row,
    wire_pipeline,
)

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.pipeline import PipelineState
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel

POLL_START = 1_788_983_720.0


@pytest.fixture
def test_settings(unused_tcp_port: int) -> Settings:
    return make_test_settings(health_port=unused_tcp_port)


def _market(trade: TradeEvent, category: str) -> MarketMetadata:
    return MarketMetadata(
        condition_id=trade.market_id,
        question="Test market?",
        description="",
        tokens=(Token(token_id=trade.asset_id, outcome=trade.outcome, price=trade.price),),
        category=category,
    )


@pytest.fixture
def sample_trade_event() -> TradeEvent:
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
def niche_market(sample_trade_event: TradeEvent) -> MarketMetadata:
    return _market(sample_trade_event, "science")


async def _persisted_assessments(engine: AsyncEngine) -> list[RiskAssessmentModel]:
    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(RiskAssessmentModel))).scalars().all())


def _seeded_server(clock: FakeClock) -> FakeTradesServer:
    server = FakeTradesServer(clock=clock)
    server.publish(trade_row(timestamp=int(POLL_START) - 1, transaction=1, wallet=1))
    server.publish(trade_row(timestamp=int(POLL_START) - 61, transaction=2, wallet=2))
    return server


def _pipeline_row(sample_trade: TradeEvent, clock: FakeClock, tx_num: int = 9) -> dict[str, object]:
    row = trade_row(
        timestamp=int(clock.now) + 1,
        transaction=tx_num,
        market=sample_trade.market_id,
        asset=sample_trade.asset_id,
        price=str(sample_trade.price),
        size=str(sample_trade.size),
    )
    row["proxyWallet"] = sample_trade.wallet_address
    return row


async def _run_until(predicate: Any, *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.002)
    raise AssertionError("condition was not reached")


class FailingDatabaseManager:
    """DatabaseManager fake that raises an exception when obtaining a session."""

    def get_async_session(self) -> Any:
        raise RuntimeError("Database connection pool exhausted")


class TestEndToEndPipelineHarness:
    """Deterministic end-to-end integration tests verifying US3 requirements."""

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_pipeline(
        self,
        unused_tcp_port: int,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        """Verify complete pipeline in dry-run mode: zero delivery calls, zero dedup keys, assessment persisted."""
        clock = FakeClock(POLL_START)
        server = _seeded_server(clock)
        discord = FakeAlertChannel("discord")
        telegram = FakeAlertChannel("telegram")

        settings = make_test_settings(
            dry_run=True,
            alert_threshold=0.4,
            health_port=unused_tcp_port,
        )

        pipeline = await wire_pipeline(
            settings,
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[discord, telegram],
            trades=server,
            poll_clock=clock,
        )

        try:
            await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
            await _run_until(lambda: len(server.requests) >= 1)

            # Check health readiness
            ready, reason, _ = await pipeline.check_readiness()
            assert ready is True
            assert reason is None

            # Verify HTTP health endpoints respond accurately
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{unused_tcp_port}/live") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data == {"live": True}

                async with session.get(f"http://127.0.0.1:{unused_tcp_port}/ready") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["ready"] is True

                async with session.get(f"http://127.0.0.1:{unused_tcp_port}/health") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert "components" in data

                async with session.get(f"http://127.0.0.1:{unused_tcp_port}/metrics") as resp:
                    assert resp.status == 200

            # Publish qualifying trade
            server.publish(_pipeline_row(sample_trade_event, clock))
            await _run_until(lambda: pipeline.stats.trades_processed >= 1)
            await _run_until(lambda: len(server.requests) >= 4)
        finally:
            await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)

        # Verification: dry run safety guarantees
        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.alerts_sent == 0
        assert len(discord.deliveries) == 0
        assert len(telegram.deliveries) == 0

        # Zero dedup keys in Redis
        assert await fake_redis.keys("alert:dedup:*") == []
        assert await fake_redis.keys("alert:ambiguous:*") == []

        # Assessment persisted with dry_run disposition and populated diagnostics
        rows = await _persisted_assessments(async_engine)
        assert len(rows) == 1
        assessment_row = rows[0]
        assert assessment_row.should_alert is True
        assert assessment_row.delivery_disposition == "dry_run"
        assert assessment_row.dry_run is True
        assert assessment_row.wallet_address == sample_trade_event.wallet_address.lower()
        assert assessment_row.wallet_tx_count == 0
        assert assessment_row.wallet_age_known is False
        assert assessment_row.signals_triggered == 2

    @pytest.mark.asyncio
    async def test_end_to_end_live_delivery_and_deduplication(
        self,
        unused_tcp_port: int,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        """Verify live multi-channel delivery and subsequent duplicate suppression."""
        clock = FakeClock(POLL_START)
        server = _seeded_server(clock)
        discord = FakeAlertChannel("discord")
        telegram = FakeAlertChannel("telegram")

        settings = make_test_settings(
            dry_run=False,
            alert_threshold=0.4,
            health_port=unused_tcp_port,
        )

        pipeline = await wire_pipeline(
            settings,
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=db_manager,
            channels=[discord, telegram],
            trades=server,
            poll_clock=clock,
        )

        try:
            await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
            await _run_until(lambda: len(server.requests) >= 1)

            # 1. First trade delivery
            server.publish(_pipeline_row(sample_trade_event, clock, tx_num=10))
            await _run_until(lambda: len(discord.deliveries) >= 1)
            await _run_until(lambda: len(telegram.deliveries) >= 1)
            await _run_until(lambda: pipeline.stats.alerts_sent >= 1)
            assert pipeline.stats.alerts_sent == 1

            wallet_lower = sample_trade_event.wallet_address.lower()
            market_id = sample_trade_event.market_id
            discord_key = f"alert:dedup:discord:{wallet_lower}:{market_id}"
            telegram_key = f"alert:dedup:telegram:{wallet_lower}:{market_id}"
            assert await fake_redis.exists(discord_key) == 1
            assert await fake_redis.exists(telegram_key) == 1

            # 2. Second trade for same wallet/market should be duplicate suppressed
            clock.advance(5.0)
            server.publish(_pipeline_row(sample_trade_event, clock, tx_num=11))
            await _run_until(lambda: pipeline.stats.trades_processed >= 2)
            await _run_until(lambda: len(server.requests) >= 6)

            # No second alert delivery to channels
            assert len(discord.deliveries) == 1
            assert len(telegram.deliveries) == 1
        finally:
            await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)

        rows = await _persisted_assessments(async_engine)
        assert len(rows) == 2
        dispositions = [r.delivery_disposition for r in rows]
        assert "delivered" in dispositions
        assert "duplicate" in dispositions

    @pytest.mark.asyncio
    async def test_end_to_end_persistence_failure_does_not_block_delivery(
        self,
        unused_tcp_port: int,
        fake_redis: FakeAsyncRedis,
        sample_trade_event: TradeEvent,
        niche_market: MarketMetadata,
    ) -> None:
        """FR-013: Persistence failure after scoring must not prevent delivery."""
        clock = FakeClock(POLL_START)
        server = _seeded_server(clock)
        discord = FakeAlertChannel("discord")

        settings = make_test_settings(
            dry_run=False,
            alert_threshold=0.4,
            health_port=unused_tcp_port,
        )

        pipeline = await wire_pipeline(
            settings,
            redis=fake_redis,
            eth=FakeEth(transaction_count=0),
            market=niche_market,
            db_manager=FailingDatabaseManager(),  # type: ignore[arg-type]
            channels=[discord],
            trades=server,
            poll_clock=clock,
        )

        try:
            await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
            await _run_until(lambda: len(server.requests) >= 1)

            server.publish(_pipeline_row(sample_trade_event, clock))
            await _run_until(lambda: len(discord.deliveries) >= 1)
            await _run_until(lambda: pipeline.stats.alerts_sent >= 1)

            # Delivery still succeeded despite database error
            assert len(discord.deliveries) == 1
            assert pipeline.stats.alerts_sent == 1
        finally:
            await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)

    @pytest.mark.asyncio
    async def test_end_to_end_worker_crash_causes_pipeline_error_and_unready(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
    ) -> None:
        """FR-006: Background worker terminal crash transitions pipeline to ERROR state and unready."""
        clock = FakeClock(POLL_START)
        server = FakeTradesServer(clock=clock)
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
            trades=server,
            poll_clock=clock,
        )

        pipeline._state = PipelineState.RUNNING
        try:
            await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
            await _run_until(lambda: len(server.requests) >= 1)

            ready_before, _, _ = await pipeline.check_readiness()
            assert ready_before is True

            # Trigger terminal 401 error on trade polling
            server.fail_next(terminal(401))
            await _run_until(lambda: pipeline.state is PipelineState.ERROR)

            assert pipeline.state is PipelineState.ERROR
            assert pipeline.stats.errors >= 1

            ready_after, reason, _ = await pipeline.check_readiness()
            assert ready_after is False
            assert reason is not None
        finally:
            await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)
