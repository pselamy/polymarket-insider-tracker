"""Tests for the gamma-api client."""

from __future__ import annotations

import itertools
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from polymarket_insider_tracker.ingestor.gamma_client import (
    GammaClient,
    GammaClientError,
    GammaMarketStats,
)
from polymarket_insider_tracker.ingestor.volume_observation import JsonValue
from tests.ingestor.conftest import PatchAsyncClient


def _make_client(
    _handler: httpx.MockTransport,
    *,
    page_limit: int = 100,
    max_pages: int = 5,
    page_concurrency: int = 5,
    max_retries: int = 1,
) -> GammaClient:
    """Build a GammaClient that constructs httpx.AsyncClient with the given transport.

    GammaClient creates its own AsyncClient inside `get_active_market_stats`,
    so we monkeypatch the AsyncClient factory in the module to inject the mock
    transport.
    """
    return GammaClient(
        page_limit=page_limit,
        max_pages=max_pages,
        page_concurrency=page_concurrency,
        max_retries=max_retries,
        retry_base_delay_seconds=0.0,
    )


def _first_page_then(
    first_page: object, status: int, rest: object
) -> Callable[[httpx.Request], httpx.Response]:
    """Serve ``first_page`` at offset 0 and ``status`` with ``rest`` for every later offset."""

    def handler(request: httpx.Request) -> httpx.Response:
        if int(request.url.params.get("offset", "0")) == 0:
            return httpx.Response(200, json=first_page)
        return httpx.Response(status, json=rest)

    return handler


@pytest.mark.asyncio
async def test_get_active_market_stats_single_page(patch_async_client: PatchAsyncClient) -> None:
    page_one = [
        {"conditionId": "0xa", "volume24hr": "100", "liquidityNum": "10"},
        {"conditionId": "0xb", "volume24hr": "200", "liquidityNum": "20"},
    ]
    calls: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.url.params))
        offset = int(request.url.params.get("offset", "0"))
        if offset == 0:
            return httpx.Response(200, json=page_one)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=2, max_pages=3)

    result = await client.get_active_market_stats()

    assert set(result.keys()) == {"0xa", "0xb"}
    assert isinstance(result["0xa"], GammaMarketStats)
    assert result["0xa"].volume24hr_raw == "100"
    assert calls[0]["limit"] == "2"
    assert calls[0]["order"] == "volume24hr"
    assert calls[0]["ascending"] == "false"


@pytest.mark.asyncio
async def test_get_active_market_stats_short_page_stops(
    patch_async_client: PatchAsyncClient,
) -> None:
    """A page shorter than page_limit signals end-of-data after a small empty streak."""
    page_zero = [{"conditionId": f"0x{i}", "volume24hr": str(i)} for i in range(5)]
    page_one_short = [{"conditionId": "0xshort", "volume24hr": "1"}]

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        if offset == 0:
            return httpx.Response(200, json=page_zero)
        if offset == 5:
            return httpx.Response(200, json=page_one_short)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=5, max_pages=10)

    result = await client.get_active_market_stats()

    assert "0xshort" in result
    assert len(result) == 6


@pytest.mark.asyncio
async def test_get_active_market_stats_offset_cap_clean_stop(
    patch_async_client: PatchAsyncClient,
) -> None:
    """Gamma rejects offsets past its hard cap; that error is swallowed cleanly."""

    handler = _first_page_then(
        [{"conditionId": "0xa", "volume24hr": "1"}], 400, {"error": "offset too large"}
    )

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=1, max_pages=4, max_retries=1)

    result = await client.get_active_market_stats()
    assert "0xa" in result


@pytest.mark.asyncio
async def test_get_with_retry_recovers_after_transient_error(
    patch_async_client: PatchAsyncClient,
) -> None:
    """Transient HTTP errors retry up to max_retries before giving up."""
    state = {"attempts": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        if offset == 0:
            state["attempts"] += 1
            if state["attempts"] < 2:
                return httpx.Response(503, json={"error": "transient"})
            return httpx.Response(200, json=[{"conditionId": "0xrecover", "volume24hr": "1"}])
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=1, max_pages=2, max_retries=3)

    result = await client.get_active_market_stats()
    assert "0xrecover" in result
    assert state["attempts"] == 2


@pytest.mark.asyncio
async def test_unexpected_response_shape_is_handled(patch_async_client: PatchAsyncClient) -> None:
    """A non-list payload becomes a clean stop, not a crash."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=1, max_pages=2, max_retries=1)

    result = await client.get_active_market_stats()
    assert result == {}


@pytest.mark.asyncio
async def test_skips_non_dict_entries(patch_async_client: PatchAsyncClient) -> None:
    """Defensive: server returning mixed-type list items shouldn't crash."""

    handler = _first_page_then(
        [{"conditionId": "0xa", "volume24hr": "1"}, "garbage", None, 42], 200, []
    )

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=4, max_pages=2)

    result = await client.get_active_market_stats()
    assert list(result.keys()) == ["0xa"]


def test_gamma_client_error_inherits_exception() -> None:
    assert issubclass(GammaClientError, Exception)


def _page(*condition_ids: str) -> list[JsonValue]:
    return [{"conditionId": cid, "volume24hr": "1"} for cid in condition_ids]


def _paged_client(
    patch_async_client: PatchAsyncClient, pages: list[list[JsonValue]]
) -> GammaClient:
    """A client over ``pages`` fetched one at a time, each stamped one second after the last."""

    def handler(request: httpx.Request) -> httpx.Response:
        index = int(request.url.params.get("offset", "0")) // 100
        return httpx.Response(200, json=pages[index] if index < len(pages) else [])

    patch_async_client(httpx.MockTransport(handler))
    ticks = itertools.count()
    epoch = datetime(2026, 10, 19, tzinfo=UTC)
    return GammaClient(
        page_limit=100,
        max_pages=len(pages),
        page_concurrency=1,
        max_retries=1,
        retry_base_delay_seconds=0.0,
        clock=lambda: epoch + timedelta(seconds=next(ticks)),
    )


async def _stats_of(
    patch_async_client: PatchAsyncClient, *items: JsonValue
) -> dict[str, GammaMarketStats]:
    """The stats the client keeps from one page holding ``items``."""
    page: list[JsonValue] = list(items)
    return await _paged_client(patch_async_client, [page]).get_active_market_stats()


class TestParseMarket:
    @pytest.mark.asyncio
    async def test_keeps_the_volume24hr_value_exactly_as_received(
        self, patch_async_client: PatchAsyncClient
    ) -> None:
        raw: dict[str, JsonValue] = {"conditionId": "0xabc", "volume24hr": "12345.67"}
        stats = (await _stats_of(patch_async_client, raw)).get("0xabc")
        assert stats is not None
        assert stats.condition_id == "0xabc"
        assert (stats.volume24hr_present, stats.volume24hr_raw) == (True, "12345.67")

    @pytest.mark.asyncio
    async def test_garbage_is_preserved_for_the_classifier_not_dropped(
        self, patch_async_client: PatchAsyncClient
    ) -> None:
        result = await _stats_of(
            patch_async_client, {"conditionId": "0x3", "volume24hr": "not-a-number"}
        )
        assert result["0x3"].volume24hr_raw == "not-a-number"

    @pytest.mark.asyncio
    async def test_handles_a_missing_volume_key(self, patch_async_client: PatchAsyncClient) -> None:
        stats = (await _stats_of(patch_async_client, {"conditionId": "0x2"}))["0x2"]
        assert (stats.volume24hr_present, stats.volume24hr_raw) == (False, None)

    @pytest.mark.asyncio
    async def test_rejects_missing_condition_id(self, patch_async_client: PatchAsyncClient) -> None:
        result = await _stats_of(
            patch_async_client, {"volume24hr": "1"}, {"conditionId": ""}, {"conditionId": 123}
        )
        assert result == {}


class TestAggregatePages:
    """Pages arrive fully fetched, so aggregation must never discard a later page."""

    @pytest.mark.asyncio
    async def test_keeps_every_page_after_empty_pages(
        self, patch_async_client: PatchAsyncClient
    ) -> None:
        pages: list[list[JsonValue]] = [_page("0xa", "0xb"), [], [], _page("0xd")]

        result = await _paged_client(patch_async_client, pages).get_active_market_stats()

        assert set(result) == {"0xa", "0xb", "0xd"}

    @pytest.mark.asyncio
    async def test_keeps_pages_after_a_short_page(
        self, patch_async_client: PatchAsyncClient
    ) -> None:
        pages: list[list[JsonValue]] = [_page("0xa"), [], _page("0xc")]

        result = await _paged_client(patch_async_client, pages).get_active_market_stats()

        assert set(result) == {"0xa", "0xc"}

    @pytest.mark.asyncio
    async def test_skips_non_dict_and_unparseable_items(
        self, patch_async_client: PatchAsyncClient
    ) -> None:
        result = await _stats_of(patch_async_client, *_page("0xa"), "garbage", {"conditionId": ""})

        assert set(result) == {"0xa"}

    @pytest.mark.asyncio
    async def test_later_page_wins_for_a_repeated_condition_id(
        self, patch_async_client: PatchAsyncClient
    ) -> None:
        pages: list[list[JsonValue]] = [_page("0xa"), [{"conditionId": "0xa", "volume24hr": "9"}]]

        result = await _paged_client(patch_async_client, pages).get_active_market_stats()

        assert result["0xa"].volume24hr_raw == "9"

    @pytest.mark.asyncio
    async def test_no_pages_yields_no_stats(self, patch_async_client: PatchAsyncClient) -> None:
        assert await _paged_client(patch_async_client, []).get_active_market_stats() == {}
