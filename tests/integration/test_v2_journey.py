"""The deterministic dry-run journey over the v2 cursor feed.

The same pipeline, fakes, and assertions as the v1 journey in ``test_end_to_end.py``, but with
``POLYMARKET_TRADES_URL`` pointed at the official v2 endpoint and a ``{data, pagination}`` cursor
provider behind it. It proves the wiring end to end at the dry-run boundary: exactly one stored
assessment, zero channel deliveries, and zero successful-delivery dedup keys.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from tests.fakes import (
    FakeAlertChannel,
    FakeClock,
    FakeEth,
    FakeV2TradesServer,
    make_test_settings,
    trade_row,
    v2_row,
    wire_pipeline,
)

from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel

POLL_START = 1_788_983_720.0
V2_TRADES_URL = "https://data-api.polymarket.com/v2/trades"


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
    return MarketMetadata(
        condition_id=sample_trade_event.market_id,
        question="Test market?",
        description="",
        tokens=(
            Token(
                token_id=sample_trade_event.asset_id,
                outcome=sample_trade_event.outcome,
                price=sample_trade_event.price,
            ),
        ),
        category="science",
    )


def _seeded_v2_server(clock: FakeClock) -> FakeV2TradesServer:
    """Two-row history served one row per page so a proof spans more than one cursor page."""
    server = FakeV2TradesServer(clock=clock, page_size=1)
    server.publish(v2_row(trade_row(timestamp=int(POLL_START) - 1, transaction=1, wallet=1)))
    server.publish(v2_row(trade_row(timestamp=int(POLL_START) - 61, transaction=2, wallet=2)))
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


async def _run_until(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.002)
    raise AssertionError("condition was not reached")


async def _persisted_assessments(engine: AsyncEngine) -> list[RiskAssessmentModel]:
    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(RiskAssessmentModel))).scalars().all())


@pytest.mark.asyncio
async def test_end_to_end_dry_run_pipeline_v2(
    unused_tcp_port: int,
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    sample_trade_event: TradeEvent,
    niche_market: MarketMetadata,
) -> None:
    """Dry run over v2: one assessment, zero delivery calls, zero dedup keys."""
    clock = FakeClock(POLL_START)
    server = _seeded_v2_server(clock)
    discord = FakeAlertChannel("discord")
    telegram = FakeAlertChannel("telegram")

    settings = make_test_settings(
        dry_run=True,
        alert_threshold=0.4,
        health_port=unused_tcp_port,
        trades_url=V2_TRADES_URL,
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
        # A proof spanning the seeded two-row history forces more than one cursor page.
        await _run_until(lambda: len(server.requests) >= 3)

        server.publish(v2_row(_pipeline_row(sample_trade_event, clock)))
        await _run_until(lambda: pipeline.stats.trades_processed >= 1)
        await _run_until(lambda: len(server.requests) >= 6)
    finally:
        await asyncio.wait_for(pipeline._stop_background_services(), timeout=2.0)

    assert pipeline.stats.trades_processed == 1
    assert pipeline.stats.alerts_sent == 0
    assert len(discord.deliveries) == 0
    assert len(telegram.deliveries) == 0

    assert await fake_redis.keys("alert:dedup:*") == []
    assert await fake_redis.keys("alert:ambiguous:*") == []

    rows = await _persisted_assessments(async_engine)
    assert len(rows) == 1
    assessment_row = rows[0]
    assert assessment_row.should_alert is True
    assert assessment_row.delivery_disposition == "dry_run"
    assert assessment_row.dry_run is True
    assert assessment_row.wallet_address == sample_trade_event.wallet_address.lower()
