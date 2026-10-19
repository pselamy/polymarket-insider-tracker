"""Boundary outcomes on the v2 acquisition path, asserted through the poller.

These drive a real ``TradePoller`` configured at the official v2 endpoint over
``FakeV2TradesServer`` and check that the merged walk's outcomes map onto the existing boundary
semantics: an unproven bounded walk freezes the boundary and later records a loss event; a malformed
pagination envelope is a terminal source failure that never advances the boundary; and a first v2
cycle from an existing checkpoint resumes it rather than re-anchoring.
"""

from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.observation_boundary import (
    BoundaryOrigin,
    LossReason,
    ObservationBoundary,
)
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState, TradePoller
from polymarket_insider_tracker.ingestor.trades_source import MAX_RETRIES
from tests.fakes import (
    FakeClock,
    FakeTradesServer,
    FakeV2TradesServer,
    invalid_json,
    make_test_settings,
    trade_row,
    v2_row,
)

NOW = 1_788_983_720.0
T0 = int(NOW)
V2_TRADES_URL = "https://data-api.polymarket.com/v2/trades"


class RecordingTradeCallback:
    def __init__(self) -> None:
        self.trades: list[TradeEvent] = []

    async def __call__(self, trade: TradeEvent) -> None:
        self.trades.append(trade)

    def timestamps(self) -> list[int]:
        return [int(trade.timestamp.timestamp()) for trade in self.trades]


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(NOW)


@pytest.fixture
def settings() -> PolymarketSettings:
    return make_test_settings(trades_url=V2_TRADES_URL).polymarket


@pytest.fixture
def callback() -> RecordingTradeCallback:
    return RecordingTradeCallback()


def _poller(
    server: FakeV2TradesServer,
    clock: FakeClock,
    fake_redis: FakeAsyncRedis,
    settings: PolymarketSettings,
    callback: RecordingTradeCallback,
) -> TradePoller:
    return TradePoller(
        callback,
        redis=fake_redis,
        settings=settings,
        http_client=server.client(),
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )


async def _anchored_poller(
    server: FakeV2TradesServer,
    clock: FakeClock,
    fake_redis: FakeAsyncRedis,
    settings: PolymarketSettings,
    callback: RecordingTradeCallback,
) -> TradePoller:
    """A poller that completed a first-start cycle over a single anchor row."""
    server.publish(v2_row(trade_row(timestamp=T0 - 1, transaction=1)))
    poller = _poller(server, clock, fake_redis, settings, callback)
    await poller.run_cycle()
    clock.advance(5)
    return poller


class TestV2BoundaryOutcomes:
    async def test_unproven_bounded_walk_freezes_then_records_loss(
        self,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        server = FakeV2TradesServer(clock=clock, page_size=1)
        poller = await _anchored_poller(server, clock, fake_redis, settings, callback)
        for offset in range(1, 22):
            server.publish(v2_row(trade_row(timestamp=T0 + offset, transaction=offset)))

        await poller.run_cycle()
        frozen = poller.status
        assert frozen.state is IngestionState.POSSIBLE_DATA_LOSS
        assert frozen.boundary_time == T0 - 1
        assert frozen.boundary_origin is BoundaryOrigin.FIRST_START
        assert frozen.loss_events == ()
        assert (
            await fake_redis.hget(poller.boundary.checkpoint_key, "boundary_time")
            == str(T0 - 1).encode()
        )

        clock.advance(600)
        await poller.run_cycle()

        recorded = poller.status
        assert recorded.state is IngestionState.RUNNING
        assert recorded.boundary_origin is BoundaryOrigin.RE_ANCHORED
        assert len(recorded.loss_events) == 1
        assert recorded.loss_events[0].reason is LossReason.HORIZON_EXPIRED
        assert recorded.loss_events[0].from_time == T0 - 1
        # The walk stopped at the 20-page bound, so its oldest examined row is T0 + 2.
        assert recorded.loss_events[0].to_time == T0 + 2

    async def test_malformed_pagination_fails_without_advancing_the_boundary(
        self,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        server = FakeV2TradesServer(clock=clock, page_size=1)
        poller = _poller(server, clock, fake_redis, settings, callback)
        server.fail_next({"data": [], "pagination": {"next_cursor": 5, "has_more": True}})

        await poller.run_cycle()

        status = poller.status
        assert status.state is IngestionState.FAILED
        assert "incompatible-schema" in (status.last_error or "")
        assert status.boundary_time is None
        assert callback.trades == []

    async def test_first_v2_cycle_resumes_an_existing_checkpoint(
        self,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        # PR #150 (automatic v1->v2 source_id continuity) is unmerged, so the checkpoint is written
        # here under the poller's own namespace -- the shared namespace once #150 lands -- and the
        # first v2 cycle must resume it rather than re-anchoring.
        server = FakeV2TradesServer(clock=clock, page_size=1)
        server.publish(v2_row(trade_row(timestamp=T0, transaction=2)))
        server.publish(v2_row(trade_row(timestamp=T0 - 61, transaction=1)))
        boundary = ObservationBoundary(
            fake_redis,
            trades_url=V2_TRADES_URL,
            coverage=settings.trades_coverage.value,
            horizon_seconds=600,
            clock=clock,
        )
        await boundary.advance(
            boundary_time=T0 - 1,
            origin=BoundaryOrigin.FIRST_START,
            last_request_end=T0 - 1,
            identities={},
        )
        poller = _poller(server, clock, fake_redis, settings, callback)

        await poller.run_cycle()

        status = poller.status
        assert status.boundary_origin is BoundaryOrigin.PROVEN
        assert status.boundary_time == T0
        assert callback.timestamps() == [T0]


class TestExhaustedUnparseableParity:
    async def test_exhausted_unparseable_body_fails_identically_on_both_paths(
        self,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        """An unparseable 200 body retried to exhaustion is a terminal failure on both paths."""
        v1_server = FakeTradesServer(clock=clock)
        v1_server.fail_next(invalid_json(), times=MAX_RETRIES + 1)
        v1_poller = TradePoller(
            callback,
            redis=fake_redis,
            settings=make_test_settings(trades_url="https://trades.invalid/trades").polymarket,
            http_client=v1_server.client(),
            clock=clock,
            sleeper=clock.sleep,
            random_source=lambda: 0.0,
        )
        await v1_poller.run_cycle()

        v2_server = FakeV2TradesServer(clock=clock, page_size=1)
        for _ in range(MAX_RETRIES + 1):
            v2_server.fail_next_raw(b"<html>not json</html>")
        v2_poller = _poller(v2_server, clock, fake_redis, settings, callback)
        await v2_poller.run_cycle()

        for poller in (v1_poller, v2_poller):
            assert poller.status.state is IngestionState.FAILED
            assert "incompatible-schema" in (poller.status.last_error or "")
            assert poller.status.boundary_time is None
        assert callback.trades == []
