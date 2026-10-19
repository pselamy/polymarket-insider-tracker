"""One home for the shared dry-run journey scaffolding.

The v1, explicit-v2, and default-v2 journeys prove the same contract — exactly one stored
explainable assessment, zero channel deliveries, and zero successful-delivery dedup keys — over
the same fake boundaries. The pipeline wiring, seeded cursor history, waiting/persistence
helpers, journey execution, and contract assertions therefore live here once. Each journey file
carries only its own entry configuration and any journey-specific assertions. Not collected as
tests (its name does not start with ``test_``).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from tests.fakes import (
    FakeAlertChannel,
    FakeClock,
    FakeEth,
    FakeV2TradesServer,
    trade_row,
    v2_row,
    wire_pipeline,
)
from tests.fakes.pipeline import TradesServer

from polymarket_insider_tracker.alerter.dispatcher import AlertChannel
from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel

POLL_START = 1_788_983_720.0


@dataclass(frozen=True)
class JourneyEnvironment:
    """The standard per-test journey fixtures, bound once."""

    health_port: int
    redis: FakeAsyncRedis
    db_manager: DatabaseManager
    engine: AsyncEngine


def seeded_v2_server(clock: FakeClock) -> FakeV2TradesServer:
    """Two-row history served one row per page so a proof spans more than one cursor page."""
    server = FakeV2TradesServer(clock=clock, page_size=1)
    server.publish(v2_row(trade_row(timestamp=int(POLL_START) - 1, transaction=1, wallet=1)))
    server.publish(v2_row(trade_row(timestamp=int(POLL_START) - 61, transaction=2, wallet=2)))
    return server


def pipeline_row(sample_trade: TradeEvent, clock: FakeClock, tx_num: int = 9) -> dict[str, object]:
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


async def run_until(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.002)
    raise AssertionError("condition was not reached")


async def persisted_assessments(engine: AsyncEngine) -> list[RiskAssessmentModel]:
    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(RiskAssessmentModel))).scalars().all())


async def wire_journey_pipeline(
    settings: Settings,
    *,
    redis: FakeAsyncRedis,
    market: MarketMetadata,
    db_manager: DatabaseManager,
    channels: Sequence[AlertChannel],
    trades: TradesServer,
    poll_clock: FakeClock,
) -> Pipeline:
    """Build a journey's real pipeline over the fake boundaries."""
    return await wire_pipeline(
        settings,
        redis=redis,
        eth=FakeEth(transaction_count=0),
        market=market,
        db_manager=db_manager,
        channels=channels,
        trades=trades,
        poll_clock=poll_clock,
    )


@asynccontextmanager
async def running_pipeline(pipeline: Pipeline) -> AsyncIterator[Pipeline]:
    """Run the background services for the block, then stop them cleanly."""
    try:
        await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
        yield pipeline
    finally:
        await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)


def dry_run_assessment(
    rows: list[RiskAssessmentModel], sample_trade: TradeEvent
) -> RiskAssessmentModel:
    """Assert the single dry-run assessment contract and return it for further checks."""
    assert len(rows) == 1
    row = rows[0]
    assert row.should_alert is True
    assert row.delivery_disposition == "dry_run"
    assert row.dry_run is True
    assert row.wallet_address == sample_trade.wallet_address.lower()
    return row


async def run_dry_run_journey(
    environment: JourneyEnvironment,
    settings: Settings,
    server: FakeV2TradesServer,
    clock: FakeClock,
    sample_trade: TradeEvent,
    market: MarketMetadata,
) -> RiskAssessmentModel:
    """Run the seeded dry-run journey through the real wiring and assert the shared contract.

    The two fake delivery channels are dry-run suppressed, so the journey asserts zero
    deliveries and zero successful-delivery dedup keys. Returns the single stored assessment
    so each journey file can add its own assertions.
    """
    discord = FakeAlertChannel("discord")
    telegram = FakeAlertChannel("telegram")
    pipeline = await wire_journey_pipeline(
        settings,
        redis=environment.redis,
        market=market,
        db_manager=environment.db_manager,
        channels=[discord, telegram],
        trades=server,
        poll_clock=clock,
    )
    async with running_pipeline(pipeline):
        # A proof spanning the seeded two-row history forces more than one cursor page.
        await run_until(lambda: len(server.requests) >= 3)
        server.publish(v2_row(pipeline_row(sample_trade, clock)))
        await run_until(lambda: pipeline.stats.trades_processed >= 1)
        await run_until(lambda: len(server.requests) >= 6)

    assert pipeline.stats.trades_processed == 1
    assert pipeline.stats.alerts_sent == 0
    assert len(discord.deliveries) == 0
    assert len(telegram.deliveries) == 0
    assert await environment.redis.keys("alert:dedup:*") == []
    assert await environment.redis.keys("alert:ambiguous:*") == []

    rows = await persisted_assessments(environment.engine)
    return dry_run_assessment(rows, sample_trade)
