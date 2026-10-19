"""First start under the official v2 URL resumes the checkpoint written under the official v1 URL.

Uses the real ``TradePoller`` over the shared ``fakeredis`` boundary harness. Contract item 4: the
official endpoint pair with the same coverage mode keeps one logical namespace, so switching
endpoints must continue the durable complete-through boundary and the retained identity window
instead of re-anchoring (which would reset first-start suppression and open a spurious gap). The v1
endpoint is served by the list-shaped ``FakeTradesServer`` and the wired v2 endpoint by the
cursor-envelope ``FakeV2TradesServer``.
"""

from __future__ import annotations

from fakeredis import FakeAsyncRedis, FakeServer

from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.ingestor.observation_boundary import BoundaryOrigin
from tests.fakes import (
    FakeClock,
    FakeTradesServer,
    FakeV2TradesServer,
    make_test_settings,
    trade_row,
    v2_row,
)
from tests.ingestor._poller_harness import (
    RecordingTradeCallback,
)
from tests.ingestor._poller_harness import (
    build_poller as _poller,
)

OFFICIAL_V1 = "https://data-api.polymarket.com/trades"
OFFICIAL_V2 = "https://data-api.polymarket.com/v2/trades"
NOW = 1_788_983_720.0
T0 = int(NOW)


def _polymarket(trades_url: str) -> PolymarketSettings:
    return make_test_settings(trades_url=trades_url).polymarket


def _legacy_v1_polymarket() -> PolymarketSettings:
    """The pre-upgrade configuration: the official v1 URL was the default when it was written.

    Validation now rejects an explicitly configured v1 URL, so this historical state is built
    with ``model_construct`` — the boundary and poller machinery under test is unchanged.
    """
    return PolymarketSettings.model_construct(trades_url=OFFICIAL_V1)


def _seed_history(server: FakeTradesServer, *, count: int, newest: int = T0 - 1) -> None:
    for offset in range(count):
        server.publish(trade_row(timestamp=newest - offset * 60, transaction=offset, wallet=offset))


def _seed_v2_history(server: FakeV2TradesServer, *, count: int, newest: int = T0 - 1) -> None:
    for offset in range(count):
        server.publish(
            v2_row(trade_row(timestamp=newest - offset * 60, transaction=offset, wallet=offset))
        )


async def _zset(redis: FakeAsyncRedis, key: str) -> dict[str, int]:
    members = await redis.zrangebyscore(key, "-inf", "+inf", withscores=True)
    return {member.decode(): int(score) for member, score in members}


class TestEndpointSwitchResume:
    async def test_first_v2_cycle_resumes_the_v1_checkpoint_and_window(self) -> None:
        clock = FakeClock(NOW)
        redis = FakeAsyncRedis(server=FakeServer())
        v1_server = FakeTradesServer(clock=FakeClock(NOW))
        _seed_history(v1_server, count=2)

        first = _poller(v1_server, clock, redis, _legacy_v1_polymarket(), RecordingTradeCallback())
        await first.run_cycle()
        assert first.status.boundary_origin is BoundaryOrigin.FIRST_START
        checkpoint_key = first.boundary.checkpoint_key
        identities_key = first.boundary.identities_key
        window_before = await _zset(redis, identities_key)
        await first.stop()

        clock.advance(5)
        v2_server = FakeV2TradesServer(clock=clock)
        v2_server.publish(v2_row(trade_row(timestamp=T0 + 1, transaction=9, wallet=9)))
        _seed_v2_history(v2_server, count=2)
        second_callback = RecordingTradeCallback()
        second = _poller(v2_server, clock, redis, _polymarket(OFFICIAL_V2), second_callback)

        await second.run_cycle()

        assert second.boundary.checkpoint_key == checkpoint_key
        assert second.status.boundary_origin is BoundaryOrigin.PROVEN
        assert second.status.boundary_time == T0 + 1
        assert second_callback.timestamps() == [T0 + 1]
        assert second.status.loss_events == ()
        window_after = await _zset(redis, identities_key)
        assert set(window_before) <= set(window_after)

    async def test_v2_with_a_different_coverage_does_not_resume_the_v1_checkpoint(self) -> None:
        clock = FakeClock(NOW)
        redis = FakeAsyncRedis(server=FakeServer())
        v1_server = FakeTradesServer(clock=FakeClock(NOW))
        _seed_history(v1_server, count=2)

        first = _poller(v1_server, clock, redis, _legacy_v1_polymarket(), RecordingTradeCallback())
        await first.run_cycle()
        await first.stop()

        clock.advance(5)
        v2_server = FakeV2TradesServer(clock=clock)
        v2_server.publish(v2_row(trade_row(timestamp=T0 + 1, transaction=9, wallet=9)))
        _seed_v2_history(v2_server, count=2)
        switched = _poller(
            v2_server,
            clock,
            redis,
            make_test_settings(trades_url=OFFICIAL_V2, trades_coverage="taker-only").polymarket,
            RecordingTradeCallback(),
        )

        await switched.run_cycle()

        assert switched.boundary.checkpoint_key != first.boundary.checkpoint_key
        assert switched.status.boundary_origin is BoundaryOrigin.FIRST_START
