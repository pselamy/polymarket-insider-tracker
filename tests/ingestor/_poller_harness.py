"""Shared poller test harness: the downstream consumer and the poller-wiring recipe.

Not collected as tests (its name does not start with ``test_``). The v1 and v2 poller
suites share this single home for the consumer double that records deliveries and for
building a ``TradePoller`` over a fake provider on the loopback clock.
"""

from __future__ import annotations

from typing import Protocol

import httpx
from fakeredis import FakeAsyncRedis, FakeServer

from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import TradePoller
from tests.fakes import FakeClock, FakeTradesServer


class RecordingTradeCallback:
    """The downstream trade consumer; keeps every delivered event in order."""

    def __init__(self) -> None:
        self.trades: list[TradeEvent] = []

    async def __call__(self, trade: TradeEvent) -> None:
        self.trades.append(trade)

    def timestamps(self) -> list[int]:
        return [int(trade.timestamp.timestamp()) for trade in self.trades]


class TradeProvider(Protocol):
    """A fake provider exposing an ``httpx`` client to its scripted responses."""

    def client(self) -> httpx.AsyncClient: ...


def build_poller(
    server: TradeProvider,
    clock: FakeClock,
    redis: FakeAsyncRedis,
    settings: PolymarketSettings,
    callback: RecordingTradeCallback,
) -> TradePoller:
    """A poller over ``server`` on the fake clock; the shared wiring recipe."""
    return TradePoller(
        callback,
        redis=redis,
        settings=settings,
        http_client=server.client(),
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )


class RedisDropsOnRequest:
    """A provider whose first served request cuts the Redis server, after the boundary loaded."""

    def __init__(self, server: FakeTradesServer, redis_server: FakeServer) -> None:
        self._server = server
        self._redis_server = redis_server
        self._dropped = False

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self._serve))

    def _serve(self, request: httpx.Request) -> httpx.Response:
        if not self._dropped:
            self._dropped = True
            self._redis_server.connected = False
        return self._server(request)
