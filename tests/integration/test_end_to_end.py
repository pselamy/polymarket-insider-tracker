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
from dataclasses import dataclass
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
    Substitutes,
    make_test_settings,
    terminal,
    wire_pipeline,
)
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

from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.scoring_config import WalletGroupStatus
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.volume_observation import JsonValue
from polymarket_insider_tracker.pipeline import PipelineState
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel


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


@dataclass(frozen=True)
class DryRunObservation:
    """What one dry-run journey exposes to its checks; the journey itself asserts nothing."""

    trades_processed: int
    alerts_sent: int
    ready: object
    ready_reason: object
    live_status: int
    live_body: object
    ready_body: dict[str, Any]
    health_status: int
    health_body: dict[str, Any]
    metrics_status: int
    discord_deliveries: int
    telegram_deliveries: int
    dedup_keys: list[bytes]
    ambiguous_keys: list[bytes]
    row: RiskAssessmentModel
    evidence: dict[str, JsonValue]
    size_evidence: dict[str, JsonValue]
    wallet_evidence: dict[str, JsonValue]
    rendered_count: int
    rendered_score: str
    rendered_alert: FormattedAlert
    signal_confidence: str | None
    signal_volume_state: str | None


async def _observe_dry_run(
    *,
    port: int,
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    trade: TradeEvent,
    market: MarketMetadata,
) -> DryRunObservation:
    clock = FakeClock(POLL_START)
    server = seeded_server(clock)
    discord = FakeAlertChannel("discord")
    telegram = FakeAlertChannel("telegram")
    formatter = RecordingFormatter()
    pipeline = await wire_journey_pipeline(
        journey_settings(dry_run=True, health_port=port),
        redis=fake_redis,
        market=market,
        db_manager=db_manager,
        channels=[discord, telegram],
        trades=server,
        poll_clock=clock,
        substitutes=Substitutes(formatter=formatter),
    )
    async with running_pipeline(pipeline):
        await run_until(lambda: len(server.requests) >= 1)
        ready, reason, _ = await _await_readiness(pipeline, True)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/live") as resp:
                live_status, live_body = resp.status, await resp.json()
            ready_body = await _get_ready_until_200(session, port)
            async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                health_status, health_body = resp.status, await resp.json()
            async with session.get(f"http://127.0.0.1:{port}/metrics") as resp:
                metrics_status = resp.status
        server.publish(pipeline_row(trade, clock))
        await run_until(lambda: pipeline.stats.trades_processed >= 1)
        await run_until(lambda: len(server.requests) >= 4)

    row = dry_run_assessment(await persisted_assessments(async_engine), trade)
    evidence = evidence_of(read_row(row))
    assessment, alert = formatter.rendered[0]
    signal = assessment.size_anomaly_signal
    return DryRunObservation(
        trades_processed=pipeline.stats.trades_processed,
        alerts_sent=pipeline.stats.alerts_sent,
        ready=ready,
        ready_reason=reason,
        live_status=live_status,
        live_body=live_body,
        ready_body=ready_body,
        health_status=health_status,
        health_body=health_body,
        metrics_status=metrics_status,
        discord_deliveries=len(discord.deliveries),
        telegram_deliveries=len(telegram.deliveries),
        dedup_keys=await keys_matching(fake_redis, "alert:dedup:*"),
        ambiguous_keys=await keys_matching(fake_redis, "alert:ambiguous:*"),
        row=row,
        evidence=evidence,
        size_evidence=member(evidence, "size"),
        wallet_evidence=member(evidence, "wallet"),
        rendered_count=len(formatter.rendered),
        rendered_score=f"{assessment.weighted_score:.3f}",
        rendered_alert=alert,
        signal_confidence=f"{signal.confidence:.3f}" if signal is not None else None,
        signal_volume_state=(
            signal.volume_evidence.verdict.state.value
            if signal is not None and signal.volume_evidence is not None
            else None
        ),
    )


@dataclass(frozen=True)
class VolumeLessObservation:
    """The two participants of the volume-less journey and what the pipeline recorded for them."""

    rows: list[RiskAssessmentModel]
    sample_wallet: str
    other_wallet: str
    discord_deliveries: list[object]
    alerts_sent: int


async def _observe_volume_less(
    *,
    port: int,
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    trade: TradeEvent,
) -> VolumeLessObservation:
    clock = FakeClock(POLL_START)
    server = seeded_server(clock)
    discord = FakeAlertChannel("discord")
    pipeline = await wire_journey_pipeline(
        journey_settings(dry_run=True, health_port=port),
        redis=fake_redis,
        market=market_for_trade(trade, "science"),
        db_manager=db_manager,
        channels=[discord],
        trades=server,
        poll_clock=clock,
    )
    other_wallet = "0x" + "e" * 40
    async with running_pipeline(pipeline):
        await run_until(lambda: len(server.requests) >= 1)
        row = pipeline_row(trade, clock)
        server.publish(row)
        server.publish(second_participant_row(row, other_wallet))
        await run_until(lambda: pipeline.stats.trades_processed >= 2)
        await run_until(lambda: len(server.requests) >= 4)

    return VolumeLessObservation(
        rows=await persisted_assessments(async_engine),
        sample_wallet=trade.wallet_address.lower(),
        other_wallet=other_wallet,
        discord_deliveries=list(discord.deliveries),
        alerts_sent=pipeline.stats.alerts_sent,
    )


@pytest.fixture
async def dry_run_observed(
    unused_tcp_port: int,
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    sample_trade_event: TradeEvent,
    niche_market: MarketMetadata,
) -> DryRunObservation:
    return await _observe_dry_run(
        port=unused_tcp_port,
        fake_redis=fake_redis,
        db_manager=db_manager,
        async_engine=async_engine,
        trade=sample_trade_event,
        market=niche_market,
    )


@pytest.fixture
async def volume_less_observed(
    unused_tcp_port: int,
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    sample_trade_event: TradeEvent,
) -> VolumeLessObservation:
    return await _observe_volume_less(
        port=unused_tcp_port,
        fake_redis=fake_redis,
        db_manager=db_manager,
        async_engine=async_engine,
        trade=sample_trade_event,
    )


class TestEndToEndPipelineHarness:
    """Deterministic end-to-end integration tests verifying US3 requirements."""

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_pipeline(self, dry_run_observed: DryRunObservation) -> None:
        """The dry-run journey processes one trade and sends no alert."""
        # Arrange and Act: the journey runs in the dry_run_observed fixture.
        obs = dry_run_observed

        # Assert
        assert (obs.trades_processed, obs.alerts_sent) == (1, 0)

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_reports_readiness(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert (dry_run_observed.ready, dry_run_observed.ready_reason) == (True, None)

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_live_endpoint_answers(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert (dry_run_observed.live_status, dry_run_observed.live_body) == (
            200,
            {"live": True},
        )

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_ready_endpoint_answers(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert dry_run_observed.ready_body["ready"] is True

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_health_endpoint_lists_components(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert (
            dry_run_observed.health_status,
            "components" in dry_run_observed.health_body,
        ) == (200, True)

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_metrics_endpoint_answers(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert dry_run_observed.metrics_status == 200

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_sends_no_channel_deliveries(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert (
            dry_run_observed.discord_deliveries,
            dry_run_observed.telegram_deliveries,
        ) == (0, 0)

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_writes_no_dedup_keys(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert (dry_run_observed.dedup_keys, dry_run_observed.ambiguous_keys) == ([], [])

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_persists_the_unknown_age_wallet(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert (dry_run_observed.row.wallet_tx_count, dry_run_observed.row.wallet_age_known) == (
            0,
            False,
        )

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_scores_the_size_group_only(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert: G1, the unknown-age wallet is recorded evidence; only the size group scores.
        assert (
            dry_run_observed.row.signals_triggered,
            float(dry_run_observed.row.weighted_score),
        ) == (1, 0.45)

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_row_records_the_004_1_result(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert dry_run_observed.evidence["result"] == {
            "weighted_score": "0.450",
            "signals_triggered": 1,
            "should_alert": True,
        }

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_formatter_renders_one_alert(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert dry_run_observed.rendered_count == 1

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_volume_state_is_consistent_across_stages(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert: FR-016, the signal and the row evidence carry the same volume state.
        assert (
            dry_run_observed.signal_volume_state,
            dry_run_observed.size_evidence["volume_state"],
        ) == ("available", "available")

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_confidence_is_consistent_across_stages(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert: signal, row evidence and persisted column carry the same confidence.
        assert (
            dry_run_observed.signal_confidence,
            dry_run_observed.size_evidence["confidence"],
            dry_run_observed.row.size_anomaly_confidence,
        ) == ("0.750", "0.750", Decimal("0.750"))

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_score_is_consistent_across_stages(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert: the rendered assessment and the persisted row carry the same score.
        assert (dry_run_observed.rendered_score, str(dry_run_observed.row.weighted_score)) == (
            "0.450",
            "0.450",
        )

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_persists_the_measured_volume_without_book(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert (
            dry_run_observed.row.volume_impact,
            dry_run_observed.row.book_impact,
        ) == (Decimal("0.3250"), None)

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_records_the_wallet_age_as_unknown(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert dry_run_observed.wallet_evidence["age_state"] == "unknown"

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_plain_text_shows_the_risk_score(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert "Risk Score: 0.45" in dry_run_observed.rendered_alert.plain_text

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_plain_text_omits_the_wallet_age(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert "(Age:" not in dry_run_observed.rendered_alert.plain_text

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_plain_text_shows_the_volume_comparison(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert "Size vs 24h volume: 32.50% of $10,000" in dry_run_observed.rendered_alert.plain_text

    @pytest.mark.asyncio
    async def test_end_to_end_dry_run_discord_embed_shows_the_size_anomaly(
        self, dry_run_observed: DryRunObservation
    ) -> None:
        # Assert
        assert "Size Anomaly: 75%" in str(dry_run_observed.rendered_alert.discord_embed)

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_scores_each_participant_below_threshold(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        """T022 negative journey: no volume value, so only the D3 category size signal (0.120).

        Two participants of one transaction (FR-021) are evaluated as separate trades.
        """
        # Assert
        assert {
            (row.weighted_score, row.signals_triggered, row.should_alert)
            for row in volume_less_observed.rows
        } == {(Decimal("0.120"), 1, False)}

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_keeps_each_participant_as_a_wallet(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        # Assert
        assert sorted(row.wallet_address for row in volume_less_observed.rows) == sorted(
            [volume_less_observed.sample_wallet, volume_less_observed.other_wallet]
        )

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_shares_one_transaction_id(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        # Assert
        assert len({row.trade_id for row in volume_less_observed.rows}) == 1

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_records_no_volume_or_book_impact(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        # Assert
        assert {(row.volume_impact, row.book_impact) for row in volume_less_observed.rows} == {
            (None, None)
        }

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_records_the_below_threshold_disposition(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        # Assert
        assert {row.delivery_disposition for row in volume_less_observed.rows} == {
            "below_threshold"
        }

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_records_the_volume_as_unavailable(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        # Assert
        assert {
            str(member(evidence_of(read_row(row)), "size")["volume_state"])
            for row in volume_less_observed.rows
        } == {"unavailable"}

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_records_the_wallet_age_as_unknown(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        # Assert
        assert {
            str(member(evidence_of(read_row(row)), "wallet")["age_state"])
            for row in volume_less_observed.rows
        } == {"unknown"}

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_keeps_the_group_unqualified(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        # Assert
        assert {read_row(row).wallet_group_status for row in volume_less_observed.rows} == {
            WalletGroupStatus.UNQUALIFIED_PRE_T027
        }

    @pytest.mark.asyncio
    async def test_end_to_end_volume_less_journey_sends_no_alert(
        self, volume_less_observed: VolumeLessObservation
    ) -> None:
        # Assert
        assert (
            volume_less_observed.discord_deliveries,
            volume_less_observed.alerts_sent,
        ) == ([], 0)

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
