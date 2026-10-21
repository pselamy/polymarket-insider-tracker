"""Acquisition stamps, raw values and crawl accounting of the Gamma client (T012a)."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from polymarket_insider_tracker.ingestor.gamma_client import GammaClient, GammaMarketStats
from polymarket_insider_tracker.ingestor.gamma_volume_refresh import (
    DEFAULT_GAMMA_REFRESH_INTERVAL_SECONDS,
)
from polymarket_insider_tracker.ingestor.volume_observation import GAMMA_SOURCE
from polymarket_insider_tracker.ingestor.volume_store import VOLUME_RECORD_TTL_SECONDS
from tests.ingestor.conftest import PatchAsyncClient

START = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


class StepClock:
    """Each read advances one second, so every reading identifies its position."""

    def __init__(self) -> None:
        self._now = START

    def __call__(self) -> datetime:
        self._now += timedelta(seconds=1)
        return self._now


def _client(max_pages: int = 3, clock: Callable[[], datetime] | None = None) -> GammaClient:
    return GammaClient(
        page_limit=2,
        max_pages=max_pages,
        page_concurrency=1,
        max_retries=1,
        retry_base_delay_seconds=0.0,
        clock=clock or StepClock(),
    )


def _serve(pages: dict[int, object]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        index = int(request.url.params["offset"]) // 2
        body = pages.get(index)
        if body is None:
            return httpx.Response(500, json={})
        return httpx.Response(200, content=json.dumps(body).encode())

    return httpx.MockTransport(handler)


def _market(cid: str, **fields: object) -> dict[str, object]:
    return {"conditionId": cid, **fields}


async def _stats(
    patch: PatchAsyncClient, pages: dict[int, object], max_pages: int = 3
) -> dict[str, GammaMarketStats]:
    patch(_serve(pages))
    return (await _client(max_pages).crawl_active_markets()).stats


@pytest.mark.asyncio
async def test_each_market_carries_its_own_page_receipt_stamp(
    patch_async_client: PatchAsyncClient,
) -> None:
    stats = await _stats(
        patch_async_client,
        {0: [_market("0xa", volume24hr="5")], 1: [_market("0xb", volume24hr="6")], 2: []},
    )

    assert stats["0xa"].acquired_at == START + timedelta(seconds=2)
    assert stats["0xb"].acquired_at == START + timedelta(seconds=3)
    assert stats["0xa"].daily_volume_source == GAMMA_SOURCE


@pytest.mark.asyncio
async def test_crawl_reports_pages_and_start_finish(patch_async_client: PatchAsyncClient) -> None:
    patch_async_client(_serve({0: [_market("0xa")], 1: [], 2: []}))

    crawl = await _client().crawl_active_markets()

    assert (crawl.pages_ok, crawl.pages_failed, crawl.truncated) == (3, 0, False)
    assert crawl.started == START + timedelta(seconds=1)
    assert crawl.finished > crawl.started


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("served", "pages_ok", "pages_failed"),
    [
        ({1: [], 2: []}, 0, 1),
        ({0: [_market("0xa")], 2: []}, 1, 1),
        ({0: [_market("0xa")], 1: []}, 2, 1),
    ],
)
async def test_a_failed_page_marks_the_crawl_truncated(
    patch_async_client: PatchAsyncClient,
    served: dict[int, object],
    pages_ok: int,
    pages_failed: int,
) -> None:
    patch_async_client(_serve(served))

    crawl = await _client().crawl_active_markets()

    assert (crawl.pages_ok, crawl.pages_failed, crawl.truncated) == (pages_ok, pages_failed, True)


@pytest.mark.asyncio
async def test_duplicate_id_keeps_the_later_stamp(patch_async_client: PatchAsyncClient) -> None:
    stats = await _stats(
        patch_async_client,
        {0: [_market("0xa", volume24hr="1")], 1: [_market("0xa", volume24hr="9")], 2: []},
    )

    assert stats["0xa"].volume24hr_raw == "9"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    [None, True, 5, 5.0, "5", "", [1], {"a": 1}, 12345.678901234567],
    ids=["null", "bool", "int", "float", "string", "empty", "array", "object", "float17"],
)
async def test_raw_value_is_the_decoded_json_value(
    patch_async_client: PatchAsyncClient, raw: object
) -> None:
    stats = await _stats(patch_async_client, {0: [_market("0xa", volume24hr=raw)], 1: [], 2: []})

    assert stats["0xa"].volume24hr_present is True
    assert stats["0xa"].volume24hr_raw == raw
    assert type(stats["0xa"].volume24hr_raw) is type(raw)


@pytest.mark.asyncio
async def test_absent_key_is_distinct_from_null(patch_async_client: PatchAsyncClient) -> None:
    stats = await _stats(
        patch_async_client,
        {0: [_market("0xa"), _market("0xb", volume24hr=None)], 1: [], 2: []},
    )

    assert (stats["0xa"].volume24hr_present, stats["0xb"].volume24hr_present) == (False, True)
    assert stats["0xa"].volume24hr_raw is None


@pytest.mark.asyncio
async def test_non_finite_tokens_decode_to_floats_kept_as_received(
    patch_async_client: PatchAsyncClient,
) -> None:
    patch_async_client(
        httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                content=(
                    b'[{"conditionId":"0xn","volume24hr":NaN},'
                    b'{"conditionId":"0xi","volume24hr":-Infinity}]'
                    if request.url.params["offset"] == "0"
                    else b"[]"
                ),
            )
        )
    )

    stats = (await _client().crawl_active_markets()).stats

    assert str(stats["0xn"].volume24hr_raw) == "nan"
    assert stats["0xi"].volume24hr_raw == float("-inf")


@pytest.mark.asyncio
async def test_get_active_market_stats_is_the_crawl_mapping(
    patch_async_client: PatchAsyncClient,
) -> None:
    patch_async_client(_serve({0: [_market("0xa", volume24hr="5")], 1: [], 2: []}))

    stats = await _client().get_active_market_stats()

    assert set(stats) == {"0xa"}


@pytest.mark.asyncio
async def test_worst_case_entry_age_stays_inside_the_recency_rule(
    patch_async_client: PatchAsyncClient,
) -> None:
    """data-model 1.4 arithmetic: S_g + 2g + w_g <= 600 with g measured from a full fake crawl.

    This pins the default cadence against the measured fake durations only; it does not bound
    ``g`` under real timeouts and retries (up to 3 x 15 s per page), which no test can prove.
    """
    patch_async_client(_serve({index: [] for index in range(50)}))

    crawl = await _client(max_pages=50).crawl_active_markets()
    offset = (crawl.finished - crawl.started).total_seconds()  # g: stamp offset inside a crawl
    write = 30.0  # w_g: a deliberately pessimistic batch write for 5,000 records

    assert DEFAULT_GAMMA_REFRESH_INTERVAL_SECONDS + 2 * offset + write <= 600
    assert VOLUME_RECORD_TTL_SECONDS == 2 * 600
