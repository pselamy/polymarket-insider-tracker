"""Assessment persistence must complete when the processing task is cancelled mid-write."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from polymarket_insider_tracker.storage.database import DatabaseManager
from tests.fakes import FakeAlertChannel, FakeEth, make_test_settings, wire_pipeline
from tests.integration._journey_harness import (
    make_sample_trade_event,
    persisted_assessments,
    stamped_volume_market,
)

# The first session serves the wallet/funding write; the second is the pending assessment.
PENDING_ASSESSMENT_SESSION = 2


class _GatedWriteDatabaseManager(DatabaseManager):
    """A manager over the test engine that holds one write open before its commit.

    The chosen session has executed its INSERT but not committed when the gate
    closes, so a cancellation delivered there lands while the write is in flight.
    """

    def __init__(self, engine: AsyncEngine, gated_session: int) -> None:
        super().__init__(
            "postgresql+psycopg://tracker:unused@127.0.0.1:1/unit-tests", async_mode=True
        )
        self.attach_async_engine(engine)
        self.gated_session = gated_session
        self.sessions_opened = 0
        self.write_in_flight = asyncio.Event()
        self.release = asyncio.Event()

    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        self.sessions_opened += 1
        gated = self.sessions_opened == self.gated_session
        async with super().get_async_session() as session:
            yield session
            if gated:
                self.write_in_flight.set()
                await self.release.wait()


async def test_cancelled_trade_still_persists_the_scored_assessment(
    fake_redis: FakeAsyncRedis, async_engine: AsyncEngine
) -> None:
    trade = make_sample_trade_event()
    database = _GatedWriteDatabaseManager(async_engine, PENDING_ASSESSMENT_SESSION)
    channel = FakeAlertChannel("discord")
    pipeline = await wire_pipeline(
        make_test_settings(alert_threshold=0.4),
        redis=fake_redis,
        eth=FakeEth(transaction_count=0),
        market=stamped_volume_market(trade),
        db_manager=database,
        channels=[channel],
    )

    processing = asyncio.create_task(pipeline.process_trade(trade))
    await database.write_in_flight.wait()
    processing.cancel()
    # Let the cancellation reach the in-flight write before the database answers.
    await asyncio.sleep(0)
    database.release.set()

    with pytest.raises(asyncio.CancelledError):
        await processing

    rows = await persisted_assessments(async_engine)
    assert [row.delivery_disposition for row in rows] == ["unrecorded"]
    assert rows[0].should_alert is True
    # The cancelled task stops before any delivery work.
    assert channel.deliveries == []
