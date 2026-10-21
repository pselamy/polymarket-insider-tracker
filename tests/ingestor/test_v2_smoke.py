"""``V2SmokeSource`` / ``make_smoke_source``: envelope selection, rows, and terminal failures.

Every wallet and transaction is synthetic; the transport is a functional ``httpx`` fake.
"""

from __future__ import annotations

import httpx
import pytest

from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.trades_source import (
    PAGE_LIMIT,
    TradesSourceClient,
    TradesTerminalError,
)
from polymarket_insider_tracker.ingestor.v2_page import V2_PAGE_LIMIT
from polymarket_insider_tracker.ingestor.v2_smoke import V2SmokeSource, make_smoke_source
from tests.fakes import FakeClock, trade_row, v2_row

NOW = 1_700_000_000.0
V1_URL = "https://data-api.example.com/trades"


def _client(body: object, requests: list[dict[str, str]]) -> httpx.AsyncClient:
    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params.items()))
        return httpx.Response(200, json=body)

    return httpx.AsyncClient(transport=httpx.MockTransport(handle))


def _smoke(client: httpx.AsyncClient, clock: FakeClock, url: str) -> V2SmokeSource:
    source, _ = make_smoke_source(
        client, url=url, coverage="all", clock=clock, sleeper=clock.sleep, random_source=lambda: 0.0
    )
    assert isinstance(source, V2SmokeSource)
    return source


async def test_v2_route_selects_the_v2_source_and_page_limit() -> None:
    async with _client({}, []) as client:
        source, limit = make_smoke_source(client, url=OFFICIAL_V2_TRADES_URL, coverage="all")

    assert isinstance(source, V2SmokeSource)
    assert limit == V2_PAGE_LIMIT


async def test_other_urls_keep_the_v1_client_and_page_limit() -> None:
    async with _client([], []) as client:
        source, limit = make_smoke_source(client, url=V1_URL, coverage="taker-only")

    assert type(source) is TradesSourceClient
    assert limit == PAGE_LIMIT


async def test_first_page_translates_envelope_rows_with_v2_parameters() -> None:
    clock = FakeClock(NOW)
    requests: list[dict[str, str]] = []
    row = trade_row(timestamp=int(NOW) - 1, transaction=1)
    body = {"data": [v2_row(row)], "pagination": {"next_cursor": None, "has_more": False}}
    async with _client(body, requests) as client:
        source = _smoke(client, clock, OFFICIAL_V2_TRADES_URL)

        page = await source.fetch_primary(boundary_time=None, horizon_seconds=60, proof=None)

    assert page.http_status == 200
    assert len(page.rows) == 1
    assert len(requests) == 1
    assert "offset" not in requests[0]
    assert requests[0]["start"] == "0"
    assert int(requests[0]["end"]) == page.request.end


async def test_non_envelope_body_is_a_terminal_incompatible_schema() -> None:
    clock = FakeClock(NOW)
    requests: list[dict[str, str]] = []
    async with _client({"rows": []}, requests) as client:
        source = _smoke(client, clock, OFFICIAL_V2_TRADES_URL)

        with pytest.raises(TradesTerminalError) as caught:
            await source.fetch_primary(boundary_time=None, horizon_seconds=60)

    assert caught.value.reason == "incompatible-schema"
    assert caught.value.status == 200
    assert len(requests) == 1
