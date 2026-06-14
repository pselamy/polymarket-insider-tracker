"""Tests for the gamma-api client."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest

from polymarket_insider_tracker.ingestor import gamma_client as gamma_module
from polymarket_insider_tracker.ingestor.gamma_client import (
    GammaClient,
    GammaClientError,
    GammaMarketStats,
    _parse_market,
)


class TestParseMarket:
    def test_parses_full_payload(self) -> None:
        raw = {
            "conditionId": "0xabc",
            "volume24hr": "12345.67",
            "volume1wk": "100000",
            "volume1mo": "500000",
            "volumeNum": "999999.5",
            "liquidityNum": "42000",
        }
        stats = _parse_market(raw)
        assert stats is not None
        assert stats.condition_id == "0xabc"
        assert stats.daily_volume == Decimal("12345.67")
        assert stats.weekly_volume == Decimal("100000")
        assert stats.monthly_volume == Decimal("500000")
        assert stats.total_volume == Decimal("999999.5")
        assert stats.liquidity == Decimal("42000")

    def test_falls_back_to_alternative_keys(self) -> None:
        raw = {
            "conditionId": "0x1",
            "volume24hr": "1",
            "volume": "777",
            "liquidity": "55",
        }
        stats = _parse_market(raw)
        assert stats is not None
        assert stats.total_volume == Decimal("777")
        assert stats.liquidity == Decimal("55")

    def test_handles_missing_numeric_fields(self) -> None:
        stats = _parse_market({"conditionId": "0x2"})
        assert stats is not None
        assert stats.daily_volume is None
        assert stats.liquidity is None

    def test_drops_garbage_decimals(self) -> None:
        stats = _parse_market(
            {"conditionId": "0x3", "volume24hr": "not-a-number", "liquidityNum": ""}
        )
        assert stats is not None
        assert stats.daily_volume is None
        assert stats.liquidity is None

    def test_rejects_missing_condition_id(self) -> None:
        assert _parse_market({"volume24hr": "1"}) is None
        assert _parse_market({"conditionId": ""}) is None
        assert _parse_market({"conditionId": 123}) is None  # type: ignore[arg-type]


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


@pytest.fixture
def patch_async_client(monkeypatch: pytest.MonkeyPatch):
    """Replace the AsyncClient used by gamma_client with one bound to a MockTransport."""

    def _apply(handler: httpx.MockTransport) -> None:
        original = gamma_module.httpx.AsyncClient

        def factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
            kwargs["transport"] = handler  # type: ignore[index]
            return original(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(gamma_module.httpx, "AsyncClient", factory)

    return _apply


@pytest.mark.asyncio
async def test_get_active_market_stats_single_page(patch_async_client) -> None:
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
    assert result["0xa"].daily_volume == Decimal("100")
    assert calls[0]["limit"] == "2"
    assert calls[0]["order"] == "volume24hr"
    assert calls[0]["ascending"] == "false"


@pytest.mark.asyncio
async def test_get_active_market_stats_short_page_stops(patch_async_client) -> None:
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
    patch_async_client,
) -> None:
    """Gamma rejects offsets past its hard cap; that error is swallowed cleanly."""

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        if offset == 0:
            return httpx.Response(200, json=[{"conditionId": "0xa", "volume24hr": "1"}])
        return httpx.Response(400, json={"error": "offset too large"})

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=1, max_pages=4, max_retries=1)

    result = await client.get_active_market_stats()
    assert "0xa" in result


@pytest.mark.asyncio
async def test_get_with_retry_recovers_after_transient_error(
    patch_async_client,
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
async def test_unexpected_response_shape_is_handled(patch_async_client) -> None:
    """A non-list payload becomes a clean stop, not a crash."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=1, max_pages=2, max_retries=1)

    result = await client.get_active_market_stats()
    assert result == {}


@pytest.mark.asyncio
async def test_skips_non_dict_entries(patch_async_client) -> None:
    """Defensive: server returning mixed-type list items shouldn't crash."""

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        if offset == 0:
            return httpx.Response(
                200,
                json=[
                    {"conditionId": "0xa", "volume24hr": "1"},
                    "garbage",
                    None,
                    42,
                ],
            )
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    patch_async_client(transport)
    client = _make_client(transport, page_limit=4, max_pages=2)

    result = await client.get_active_market_stats()
    assert list(result.keys()) == ["0xa"]


def test_gamma_client_error_inherits_exception() -> None:
    assert issubclass(GammaClientError, Exception)
