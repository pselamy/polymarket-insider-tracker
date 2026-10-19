"""One home for the shared v2 dry-run journey scaffolding.

Both v2 journeys prove the same dry-run contract — exactly one stored explainable assessment,
zero channel deliveries, and zero successful-delivery dedup keys — from different entry
configurations: the official v2 URL configured explicitly, and the approved default with
``POLYMARKET_TRADES_URL`` unset. The pipeline wiring, journey execution, and contract
assertions live here once; the shared fixtures live in this directory's ``conftest.py``. Each
journey file carries only its own configuration difference and any journey-specific assertions
on the returned assessment.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
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


async def _run_until(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.002)
    raise AssertionError("condition was not reached")


async def persisted_assessments(engine: AsyncEngine) -> list[RiskAssessmentModel]:
    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(RiskAssessmentModel))).scalars().all())


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
    pipeline: Pipeline = await wire_pipeline(
        settings,
        redis=environment.redis,
        eth=FakeEth(transaction_count=0),
        market=market,
        db_manager=environment.db_manager,
        channels=[discord, telegram],
        trades=server,
        poll_clock=clock,
    )
    try:
        await asyncio.wait_for(pipeline._start_background_services(), timeout=2.0)
        # A proof spanning the seeded two-row history forces more than one cursor page.
        await _run_until(lambda: len(server.requests) >= 3)
        server.publish(v2_row(pipeline_row(sample_trade, clock)))
        await _run_until(lambda: pipeline.stats.trades_processed >= 1)
        await _run_until(lambda: len(server.requests) >= 6)
    finally:
        await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)

    assert pipeline.stats.trades_processed == 1
    assert pipeline.stats.alerts_sent == 0
    assert len(discord.deliveries) == 0
    assert len(telegram.deliveries) == 0
    assert await environment.redis.keys("alert:dedup:*") == []
    assert await environment.redis.keys("alert:ambiguous:*") == []

    rows = await persisted_assessments(environment.engine)
    assert len(rows) == 1
    assessment = rows[0]
    assert assessment.should_alert is True
    assert assessment.delivery_disposition == "dry_run"
    assert assessment.dry_run is True
    assert assessment.wallet_address == sample_trade.wallet_address.lower()
    return assessment
