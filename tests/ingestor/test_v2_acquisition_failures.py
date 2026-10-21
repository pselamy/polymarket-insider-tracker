"""How the v2 trades source reports each failure of the cursor feed to the poller.

The source folds the walk's own error taxonomy into the v1 ``TradesSourceError`` pair the poller
already understands: a retry-exhausted transient failure stays transient (with the provider's
``Retry-After``), while an incompatible envelope, an unparseable body, and a terminal HTTP status
are terminal with the documented reason.
"""

from __future__ import annotations

import httpx
import pytest

from polymarket_insider_tracker.ingestor.acquisition import V2TradesSource
from polymarket_insider_tracker.ingestor.trades_source import (
    TradesTerminalError,
    TradesTransientError,
)
from polymarket_insider_tracker.ingestor.v2_page import V2TransportError
from tests.fakes import FakeClock, FakeV2TradesServer, trade_row, v2_row

NOW = 1_788_983_720.0
T0 = int(NOW)
V2_TRADES_URL = "https://data-api.polymarket.com/v2/trades"


def _source(client: httpx.AsyncClient, clock: FakeClock) -> V2TradesSource:
    return V2TradesSource(
        client,
        url=V2_TRADES_URL,
        coverage="all",
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
        max_retries=0,
    )


def _status_client(status: int, headers: dict[str, str] | None = None) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, headers=headers, json={}, request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_terminal_http_status_is_a_terminal_http_status_failure() -> None:
    source = _source(_status_client(404), FakeClock(NOW))

    with pytest.raises(TradesTerminalError) as raised:
        await source.fetch_primary(boundary_time=None, horizon_seconds=600)

    assert raised.value.reason == "http-status"


async def test_exhausted_transient_status_stays_transient_and_keeps_retry_after() -> None:
    source = _source(_status_client(503, {"Retry-After": "7"}), FakeClock(NOW))

    with pytest.raises(TradesTransientError) as raised:
        await source.fetch_primary(boundary_time=None, horizon_seconds=600)

    assert (raised.value.kind, raised.value.retry_after_seconds) == ("transport", 7.0)


async def test_exhausted_transport_failure_is_transient() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    source = _source(client, FakeClock(NOW))

    with pytest.raises(TradesTransientError) as raised:
        await source.fetch_primary(boundary_time=None, horizon_seconds=600)

    assert raised.value.kind == "transport"


async def test_unparseable_body_is_incompatible_schema() -> None:
    clock = FakeClock(NOW)
    server = FakeV2TradesServer(clock=clock)
    server.fail_next_raw(b"<html>not json</html>")
    source = _source(server.client(), clock)

    with pytest.raises(TradesTerminalError) as raised:
        await source.fetch_primary(boundary_time=None, horizon_seconds=600)

    assert raised.value.reason == "incompatible-schema"
    assert str(raised.value).startswith("incompatible-schema:")


async def test_malformed_envelope_is_incompatible_schema() -> None:
    clock = FakeClock(NOW)
    server = FakeV2TradesServer(clock=clock)
    server.publish(v2_row(trade_row(timestamp=T0, transaction=1)))
    server.fail_next({"data": "not-a-list"})
    source = _source(server.client(), clock)

    with pytest.raises(TradesTerminalError) as raised:
        await source.fetch_primary(boundary_time=None, horizon_seconds=600)

    assert raised.value.reason == "incompatible-schema"


async def test_the_request_cutoff_survives_a_restart_through_restore_last_end() -> None:
    clock = FakeClock(NOW)
    server = FakeV2TradesServer(clock=clock)
    source = _source(server.client(), clock)
    assert source.last_request_end is None

    source.restore_last_end(T0 + 500)

    assert source.last_request_end == T0 + 500


async def test_a_bare_transport_error_from_the_wire_is_transient() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        raise V2TransportError("transport gave up")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    source = _source(client, FakeClock(NOW))

    with pytest.raises(TradesTransientError) as raised:
        await source.fetch_primary(boundary_time=None, horizon_seconds=600)

    assert (raised.value.kind, raised.value.status, str(raised.value)) == (
        "transport",
        None,
        "transport gave up",
    )
