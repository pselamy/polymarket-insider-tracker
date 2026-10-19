"""First start under the official v2 URL resumes the checkpoint written under the official v1 URL.

Uses the real ``TradePoller`` over the shared ``fakeredis`` boundary harness and the
``FakeTradesServer`` provider fake. Contract item 4: the official endpoint pair with the same
coverage mode keeps one logical namespace, so switching endpoints must continue the durable
complete-through boundary and the retained identity window instead of re-anchoring (which would
reset first-start suppression and open a spurious gap).
"""

from __future__ import annotations

from fakeredis import FakeAsyncRedis, FakeServer

from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.observation_boundary import BoundaryOrigin
from polymarket_insider_tracker.ingestor.trade_poller import TradePoller
from tests.fakes import FakeClock, FakeTradesServer, make_test_settings, trade_row

OFFICIAL_V1 = "https://data-api.polymarket.com/trades"
OFFICIAL_V2 = "https://data-api.polymarket.com/v2/trades"
NOW = 1_788_983_720.0
T0 = int(NOW)


class RecordingTradeCallback:
    """The downstream trade consumer; keeps every delivered event in order."""

    def __init__(self) -> None:
        self.trades: list[TradeEvent] = []

    async def __call__(self, trade: TradeEvent) -> None:
        self.trades.append(trade)

    def timestamps(self) -> list[int]:
        return [int(trade.timestamp.timestamp()) for trade in self.trades]


def _poller(
    server: FakeTradesServer,
    clock: FakeClock,
    redis: FakeAsyncRedis,
    settings: PolymarketSettings,
    callback: RecordingTradeCallback,
) -> TradePoller:
    return TradePoller(
        callback,
        redis=redis,
        settings=settings,
        metadata=None,
        on_state_change=None,
        http_client=server.client(),
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )


def _polymarket(trades_url: str) -> PolymarketSettings:
    return make_test_settings(trades_url=trades_url).polymarket


def _seed_history(server: FakeTradesServer, *, count: int, newest: int = T0 - 1) -> None:
    for offset in range(count):
        server.publish(trade_row(timestamp=newest - offset * 60, transaction=offset, wallet=offset))


async def _zset(redis: FakeAsyncRedis, key: str) -> dict[str, int]:
    members = await redis.zrangebyscore(key, "-inf", "+inf", withscores=True)
    return {member.decode(): int(score) for member, score in members}


class TestEndpointSwitchResume:
    async def test_first_v2_cycle_resumes_the_v1_checkpoint_and_window(self) -> None:
        server = FakeTradesServer(clock=FakeClock(NOW))
        clock = FakeClock(NOW)
        redis = FakeAsyncRedis(server=FakeServer())
        _seed_history(server, count=2)

        first = _poller(server, clock, redis, _polymarket(OFFICIAL_V1), RecordingTradeCallback())
        await first.run_cycle()
        assert first.status.boundary_origin is BoundaryOrigin.FIRST_START
        checkpoint_key = first.boundary.checkpoint_key
        identities_key = first.boundary.identities_key
        window_before = await _zset(redis, identities_key)
        await first.stop()

        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 1, transaction=9, wallet=9))
        second_callback = RecordingTradeCallback()
        second = _poller(server, clock, redis, _polymarket(OFFICIAL_V2), second_callback)

        await second.run_cycle()

        assert second.boundary.checkpoint_key == checkpoint_key
        assert second.status.boundary_origin is BoundaryOrigin.PROVEN
        assert second.status.boundary_time == T0 + 1
        assert second_callback.timestamps() == [T0 + 1]
        assert second.status.loss_events == ()
        window_after = await _zset(redis, identities_key)
        assert set(window_before) <= set(window_after)

    async def test_v2_with_a_different_coverage_does_not_resume_the_v1_checkpoint(self) -> None:
        server = FakeTradesServer(clock=FakeClock(NOW))
        clock = FakeClock(NOW)
        redis = FakeAsyncRedis(server=FakeServer())
        _seed_history(server, count=2)

        first = _poller(server, clock, redis, _polymarket(OFFICIAL_V1), RecordingTradeCallback())
        await first.run_cycle()
        await first.stop()

        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 1, transaction=9, wallet=9))
        switched = _poller(
            server,
            clock,
            redis,
            make_test_settings(trades_url=OFFICIAL_V2, trades_coverage="taker-only").polymarket,
            RecordingTradeCallback(),
        )

        await switched.run_cycle()

        assert switched.boundary.checkpoint_key != first.boundary.checkpoint_key
        assert switched.status.boundary_origin is BoundaryOrigin.FIRST_START
