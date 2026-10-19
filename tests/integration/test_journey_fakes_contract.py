"""Contract shared by the two trade-provider fakes: identical request recording.

``FakeTradesServer`` (list-shaped v1) and ``FakeV2TradesServer`` (cursor-shaped v2,
added by #151) must capture a received request into a ``RecordedRequest`` the same
way — url, query params, clock reading, headers and timeout — so that tests
asserting on ``server.requests`` mean the same thing on either path. This pins the
request-recording knowledge at both call sites before it is given one home.
"""

from __future__ import annotations

import pytest
from tests.fakes import FakeClock, FakeTradesServer, FakeV2TradesServer
from tests.fakes.trades import RecordedRequest

REQUEST_URL = "https://trades.invalid/trades?limit=10&takerOnly=true"


@pytest.mark.asyncio
async def test_both_trade_fakes_record_a_request_identically() -> None:
    clock = FakeClock(1_234.5)
    servers = (FakeTradesServer(clock=clock), FakeV2TradesServer(clock=clock))

    for server in servers:
        async with server.client() as client:
            await client.get(REQUEST_URL)

    first, second = (server.requests[-1] for server in servers)
    assert isinstance(first, RecordedRequest)
    assert first == second
    assert first.url == REQUEST_URL
    assert first.params == {"limit": "10", "takerOnly": "true"}
    assert first.at == 1_234.5
    assert first.timeout is not None
    assert first.timeout == second.timeout
    assert first.headers == second.headers
