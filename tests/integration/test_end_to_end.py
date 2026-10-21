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
from decimal import Decimal
from typing import Any

import aiohttp
import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine
from tests.fakes import (
    FakeAlertChannel,
    FakeClock,
    FakeEth,
    FakeTradesServer,
    make_test_settings,
    terminal,
    wire_pipeline,
)
from tests.fakes.pipeline_access import PipelineAccess
from tests.fakes.redis_await import keys_matching
from tests.integration._journey_harness import (
    POLL_START,
    RecordingFormatter,
    dry_run_assessment,
    journey_settings,
    make_sample_trade_event,
    market_for_trade,
    persisted_assessments,
    pipeline_row,
    run_until,
    running_pipeline,
    second_participant_row,
    seeded_server,
    stamped_volume_market,
    wire_journey_pipeline,
)
from tests.integration.test_scoring_config_rows import evidence_of, member, read_row

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.scoring_config import WalletGroupStatus
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.pipeline import PipelineState
from polymarket_insider_tracker.storage.database import DatabaseManager


@pytest.fixture
def test_settings(unused_tcp_port: int) -> Settings:
    return make_test_settings(health_port=unused_tcp_port)


@pytest.fixture
def sample_trade_event() -> TradeEvent:
    return make_sample_trade_event()


@pytest.fixture
def niche_market(sample_trade_event: TradeEvent) -> MarketMetadata:
    """T021: a science market with a fresh stamped $10,000 volume; size-only score 0.450."""
    return stamped_volume_market(sample_trade_event)


async def _await_readiness(
    pipeline: Any, expected: bool, *, timeout: float = 5.0
) -> tuple[bool, str | None, dict[str, str]]:
    """Poll readiness until it reaches the expected value (bounded), then return it.

    Under instrumentation a bounded dependency check can transiently exceed its
    1-second budget while the loop is busy; readiness truthfully dips for that
    moment. The assertion target is the settled state, so the poll converts
    scheduler latency into a bounded wait instead of a flaky one-shot assert.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        ready, reason, summary = await pipeline.check_readiness()
        if ready is expected or asyncio.get_running_loop().time() >= deadline:
            return ready, reason, summary
        await asyncio.sleep(0.01)


async def _get_ready_until_200(
    session: aiohttp.ClientSession, port: int, *, timeout: float = 5.0
) -> dict[str, Any]:
    """Poll GET /ready until it answers 200 (bounded), returning the final body."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        async with session.get(f"http://127.0.0.1:{port}/ready") as resp:
            body: dict[str, Any] = await resp.json()
            if resp.status == 200 or asyncio.get_running_loop().time() >= deadline:
                assert resp.status == 200, body
                return body
        await asyncio.sleep(0.01)


UNREACHABLE_DATABASE_URL = "postgresql+psycopg://tracker@127.0.0.1:1/unreachable"


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
        server = seeded_server(clock)
        discord = FakeAlertChannel("discord")
        telegram = FakeAlertChannel("telegram")

        settings = journey_settings(dry_run=True, health_port=unused_tcp_port)

        pipeline = await wire_journey_pipeline(
            settings,
            redis=fake_redis,
            market=niche_market,
            db_manager=db_manager,
            channels=[discord, telegram],
            trades=server,
            poll_clock=clock,
        )
        formatter = RecordingFormatter()
        PipelineAccess.replace_components(pipeline, alert_formatter=formatter)

        async with running_pipeline(pipeline):
            await run_until(lambda: len(server.requests) >= 1)

            # Check health readiness (bounded poll; the settled state is asserted)
            ready, reason, _ = await _await_readiness(pipeline, True)
            assert ready is True
            assert reason is None

            # Verify HTTP health endpoints respond accurately
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{unused_tcp_port}/live") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data == {"live": True}

                data = await _get_ready_until_200(session, unused_tcp_port)
                assert data["ready"] is True

                async with session.get(f"http://127.0.0.1:{unused_tcp_port}/health") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert "components" in data

                async with session.get(f"http://127.0.0.1:{unused_tcp_port}/metrics") as resp:
                    assert resp.status == 200

            # Publish qualifying trade
            server.publish(pipeline_row(sample_trade_event, clock))
            await run_until(lambda: pipeline.stats.trades_processed >= 1)
            await run_until(lambda: len(server.requests) >= 4)

        # Verification: dry run safety guarantees
        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.alerts_sent == 0
        assert len(discord.deliveries) == 0
        assert len(telegram.deliveries) == 0

        # Zero dedup keys in Redis
        assert await keys_matching(fake_redis, "alert:dedup:*") == []
        assert await keys_matching(fake_redis, "alert:ambiguous:*") == []

        # Assessment persisted with dry_run disposition and populated diagnostics
        rows = await persisted_assessments(async_engine)
        assessment_row = dry_run_assessment(rows, sample_trade_event)
        assert assessment_row.wallet_tx_count == 0
        assert assessment_row.wallet_age_known is False
        # G1: the unknown-age wallet (nonce 0) is recorded evidence; only the size group scores.
        assert assessment_row.signals_triggered == 1
        assert float(assessment_row.weighted_score) == 0.45
        # The row is a schema-valid 004.1 document identifying the algorithm that produced it.
        evidence = evidence_of(read_row(assessment_row))
        assert evidence["result"] == {
            "weighted_score": "0.450",
            "signals_triggered": 1,
            "should_alert": True,
        }
        # FR-016: analysis -> score -> row -> dry-run rendering carry the same values and flags.
        ((assessment, alert),) = formatter.rendered
        size, signal = member(evidence, "size"), assessment.size_anomaly_signal
        assert signal is not None and signal.volume_evidence is not None
        assert signal.volume_evidence.verdict.state == size["volume_state"] == "available"
        assert f"{signal.confidence:.3f}" == size["confidence"] == "0.750"
        assert assessment_row.size_anomaly_confidence == Decimal(str(size["confidence"]))
        assert f"{assessment.weighted_score:.3f}" == str(assessment_row.weighted_score)
        assert member(evidence, "wallet")["age_state"] == "unknown"
        assert "Risk Score: 0.45" in alert.plain_text and "(Age:" not in alert.plain_text
        assert "Size Anomaly: 75%" in str(alert.discord_embed)

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_scores_each_participant_below_threshold(
        self,
        unused_tcp_port: int,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade_event: TradeEvent,
    ) -> None:
        """T022 negative journey: no volume value, so only the D3 category size signal (0.120).

        Two participants of one transaction (FR-021) are evaluated as separate trades.
        """
        clock = FakeClock(POLL_START)
        server = seeded_server(clock)
        discord = FakeAlertChannel("discord")
        pipeline = await wire_journey_pipeline(
            journey_settings(dry_run=True, health_port=unused_tcp_port),
            redis=fake_redis,
            market=market_for_trade(sample_trade_event, "science"),
            db_manager=db_manager,
            channels=[discord],
            trades=server,
            poll_clock=clock,
        )
        other_wallet = "0x" + "e" * 40
        async with running_pipeline(pipeline):
            await run_until(lambda: len(server.requests) >= 1)
            row = pipeline_row(sample_trade_event, clock)
            server.publish(row)
            server.publish(second_participant_row(row, other_wallet))
            await run_until(lambda: pipeline.stats.trades_processed >= 2)
            await run_until(lambda: len(server.requests) >= 4)

        rows = await persisted_assessments(async_engine)
        assert sorted(r.wallet_address for r in rows) == sorted(
            [sample_trade_event.wallet_address.lower(), other_wallet]
        )
        assert len({r.trade_id for r in rows}) == 1
        for stored in rows:
            assert (stored.weighted_score, stored.signals_triggered) == (Decimal("0.120"), 1)
            assert (stored.should_alert, stored.delivery_disposition) == (False, "below_threshold")
            document = read_row(stored)
            assert document.wallet_group_status is WalletGroupStatus.UNQUALIFIED_PRE_T027
            evidence = evidence_of(document)
            assert member(evidence, "size")["volume_state"] == "unavailable"
            assert member(evidence, "wallet")["age_state"] == "unknown"
        assert discord.deliveries == [] and pipeline.stats.alerts_sent == 0

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
        server = seeded_server(clock)
        discord = FakeAlertChannel("discord")
        telegram = FakeAlertChannel("telegram")

        settings = journey_settings(dry_run=False, health_port=unused_tcp_port)

        pipeline = await wire_journey_pipeline(
            settings,
            redis=fake_redis,
            market=niche_market,
            db_manager=db_manager,
            channels=[discord, telegram],
            trades=server,
            poll_clock=clock,
        )

        async with running_pipeline(pipeline):
            await run_until(lambda: len(server.requests) >= 1)

            # 1. First trade delivery
            server.publish(pipeline_row(sample_trade_event, clock, tx_num=10))
            await run_until(lambda: len(discord.deliveries) >= 1)
            await run_until(lambda: len(telegram.deliveries) >= 1)
            await run_until(lambda: pipeline.stats.alerts_sent >= 1)
            assert pipeline.stats.alerts_sent == 1

            wallet_lower = sample_trade_event.wallet_address.lower()
            market_id = sample_trade_event.market_id
            discord_key = f"alert:dedup:discord:{wallet_lower}:{market_id}"
            telegram_key = f"alert:dedup:telegram:{wallet_lower}:{market_id}"
            assert await fake_redis.exists(discord_key) == 1
            assert await fake_redis.exists(telegram_key) == 1

            # 2. Second trade for same wallet/market should be duplicate suppressed
            clock.advance(5.0)
            server.publish(pipeline_row(sample_trade_event, clock, tx_num=11))
            await run_until(lambda: pipeline.stats.trades_processed >= 2)
            await run_until(lambda: len(server.requests) >= 6)

            # No second alert delivery to channels
            assert len(discord.deliveries) == 1
            assert len(telegram.deliveries) == 1

        rows = await persisted_assessments(async_engine)
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
        server = seeded_server(clock)
        discord = FakeAlertChannel("discord")

        settings = journey_settings(dry_run=False, health_port=unused_tcp_port)

        pipeline = await wire_journey_pipeline(
            settings,
            redis=fake_redis,
            market=niche_market,
            db_manager=DatabaseManager(UNREACHABLE_DATABASE_URL),
            channels=[discord],
            trades=server,
            poll_clock=clock,
        )

        async with running_pipeline(pipeline):
            await run_until(lambda: len(server.requests) >= 1)

            server.publish(pipeline_row(sample_trade_event, clock))
            await run_until(lambda: len(discord.deliveries) >= 1)
            await run_until(lambda: pipeline.stats.alerts_sent >= 1)

            # Delivery still succeeded despite database error
            assert len(discord.deliveries) == 1
            assert pipeline.stats.alerts_sent == 1

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

        async with running_pipeline(pipeline):
            await run_until(lambda: len(server.requests) >= 1)

            ready_before, _, _ = await _await_readiness(pipeline, True)
            assert ready_before is True

            # Trigger terminal 401 error on trade polling
            server.fail_next(terminal(401))
            await run_until(lambda: pipeline.state is PipelineState.ERROR)

            assert pipeline.state is PipelineState.ERROR
            assert pipeline.stats.errors >= 1

            ready_after, reason, _ = await _await_readiness(pipeline, False)
            assert ready_after is False
            assert reason is not None
