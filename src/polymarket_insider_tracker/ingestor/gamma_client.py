"""Gamma API client for Polymarket market volume / liquidity data.

The CLOB API does not expose 24h volume or liquidity. The public Gamma API
(https://gamma-api.polymarket.com) does, with no auth required. This module
fetches the volume/liquidity snapshot keyed by condition_id so the
size_anomaly detector can do real ratio math instead of falling back to
the niche-base 0.2 confidence floor.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import httpx

from polymarket_insider_tracker.ingestor.volume_observation import (
    GAMMA_SOURCE,
    JsonValue,
    VolumeObservation,
)
from polymarket_insider_tracker.redaction import redact_exception_message

logger = logging.getLogger(__name__)

DEFAULT_HOST = "https://gamma-api.polymarket.com"
DEFAULT_TIMEOUT_SECONDS = 15.0
# Gamma /markets enforces a server-side max of 100 per page even when a
# higher `limit` is sent. Using 100 lines our page size up with the actual
# response so pagination doesn't bail out after the first page.
DEFAULT_PAGE_LIMIT = 100
# Gamma also caps `offset` around 10000 for this collection. Combined with
# the 100/page limit that gives ~10k markets max, sequential — way too slow
# at default sync interval. We sort by 24h volume desc and only walk the
# top N pages, since markets with zero recent volume don't need a real
# ratio anyway (the niche path handles them).
DEFAULT_MAX_PAGES = 50  # 50 * 100 = 5000 most-traded markets per sync
DEFAULT_PAGE_CONCURRENCY = 5
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_DELAY_SECONDS = 1.0


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class GammaMarketStats:
    """The 24h volume observation of a single market from gamma-api.

    ``acquired_at`` is the instant the page body carrying this market was received;
    ``volume24hr_present`` / ``volume24hr_raw`` are the key's presence and its decoded JSON value
    exactly as received (data-model section 1.1). No lossy decimal is derived here: the volume
    state classifier is the only reader of the value.
    """

    condition_id: str
    acquired_at: datetime | None = None
    volume24hr_present: bool = False
    volume24hr_raw: JsonValue = None
    daily_volume_source: str | None = None

    def volume_observation(self) -> VolumeObservation:
        """The acquisition facts of this market's volume, ready for the volume record."""
        stamp = self.acquired_at.astimezone(UTC).isoformat() if self.acquired_at else None
        return VolumeObservation(
            self.volume24hr_present, self.volume24hr_raw, stamp, self.daily_volume_source
        )


@dataclass(frozen=True)
class GammaPage:
    """One page fetch: ``items`` and the body-receipt stamp when ``status`` is ``ok``."""

    index: int
    status: Literal["ok", "failed", "skipped"]
    acquired_at: datetime | None = None
    items: tuple[JsonValue, ...] = ()


@dataclass(frozen=True)
class GammaCrawl:
    """Result of one crawl: per-market stats plus what the crawl could and could not see."""

    stats: dict[str, GammaMarketStats]
    pages_ok: int
    pages_failed: int
    truncated: bool
    started: datetime
    finished: datetime


def _parse_market(
    raw: dict[str, JsonValue], acquired_at: datetime | None = None
) -> GammaMarketStats | None:
    cid = raw.get("conditionId")
    if not cid or not isinstance(cid, str):
        return None
    return GammaMarketStats(
        condition_id=cid,
        acquired_at=acquired_at,
        volume24hr_present="volume24hr" in raw,
        volume24hr_raw=raw.get("volume24hr"),
        daily_volume_source=GAMMA_SOURCE,
    )


class GammaClientError(Exception):
    """Raised when gamma-api returns an unrecoverable error."""


def _try_parse_item(raw: JsonValue, acquired_at: datetime | None) -> GammaMarketStats | None:
    if isinstance(raw, dict):
        return _parse_market(raw, acquired_at)
    return None


def supersedes(new: GammaMarketStats, kept: GammaMarketStats) -> bool:
    """Duplicate-id rule: the later ``acquired_at`` wins; a tie keeps the lower page index."""
    if new.acquired_at is None:
        return False
    return kept.acquired_at is None or new.acquired_at > kept.acquired_at


def _keep_freshest(results: dict[str, GammaMarketStats], parsed: GammaMarketStats) -> None:
    kept = results.get(parsed.condition_id)
    if kept is None or supersedes(parsed, kept):
        results[parsed.condition_id] = parsed


def _parse_page_items(page: GammaPage, results: dict[str, GammaMarketStats]) -> None:
    for raw in page.items:
        parsed = _try_parse_item(raw, page.acquired_at)
        if parsed is not None:
            _keep_freshest(results, parsed)


def _aggregate_pages(pages: Sequence[GammaPage]) -> dict[str, GammaMarketStats]:
    """Merge every fetched page into one mapping keyed by condition id.

    Every page is already fetched by the time this runs, so no page is
    skipped: a failed or empty page never hides a later page that succeeded.
    Pages are visited in index order, so a stamp tie keeps the lower page index.
    """
    results: dict[str, GammaMarketStats] = {}
    for page in sorted(pages, key=lambda candidate: candidate.index):
        _parse_page_items(page, results)
    return results


class GammaClient:
    """Async client for the public gamma-api markets endpoint.

    Provides batched, paginated reads of every active market with their
    24h/weekly/monthly volume and current liquidity. Designed to be called
    from MarketMetadataSync once per sync interval; results are merged into
    Redis-cached MarketMetadata objects.
    """

    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        page_limit: int = DEFAULT_PAGE_LIMIT,
        max_pages: int = DEFAULT_MAX_PAGES,
        page_concurrency: int = DEFAULT_PAGE_CONCURRENCY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_base_delay_seconds: float = DEFAULT_RETRY_BASE_DELAY_SECONDS,
        clock: Clock = utc_now,
    ) -> None:
        self._host = host.rstrip("/")
        self._timeout = timeout_seconds
        self._page_limit = page_limit
        self._max_pages = max_pages
        self._page_concurrency = page_concurrency
        self._max_retries = max_retries
        self._retry_base = retry_base_delay_seconds
        self._clock = clock

    async def _fetch_page_payload(
        self,
        client: httpx.AsyncClient,
        path: str,
        params: dict[str, str | int],
    ) -> tuple[list[JsonValue], datetime]:
        resp = await client.get(path, params=params)
        acquired_at = self._clock().astimezone(UTC)
        resp.raise_for_status()
        payload: JsonValue = resp.json()
        if not isinstance(payload, list):
            raise GammaClientError(
                f"Unexpected gamma response shape for {path}: {type(payload).__name__}"
            )
        return payload, acquired_at

    @staticmethod
    async def _retry_sleep(attempt: int, max_retries: int, delay: float) -> float:
        if attempt < max_retries - 1:
            await asyncio.sleep(delay)
            return delay * 2
        return delay

    async def _get_with_retry(
        self,
        client: httpx.AsyncClient,
        path: str,
        params: dict[str, str | int],
    ) -> tuple[list[JsonValue], datetime]:
        last_exc: Exception | None = None
        delay = self._retry_base
        for attempt in range(self._max_retries):
            try:
                return await self._fetch_page_payload(client, path, params)
            except (httpx.HTTPError, ValueError) as exc:
                last_exc = exc
                logger.warning(
                    "gamma %s attempt %d/%d failed: %s",
                    path,
                    attempt + 1,
                    self._max_retries,
                    redact_exception_message(exc),
                )
                delay = await self._retry_sleep(attempt, self._max_retries, delay)
        detail = redact_exception_message(last_exc) if last_exc is not None else "None"
        raise GammaClientError(f"gamma {path} failed after {self._max_retries} attempts: {detail}")

    async def _fetch_single_page(
        self,
        client: httpx.AsyncClient,
        sem: asyncio.Semaphore,
        stop: asyncio.Event,
        page_index: int,
    ) -> GammaPage:
        if stop.is_set():
            return GammaPage(page_index, "skipped")
        params: dict[str, str | int] = {
            "limit": self._page_limit,
            "offset": page_index * self._page_limit,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        async with sem:
            if stop.is_set():
                return GammaPage(page_index, "skipped")
            try:
                items, acquired_at = await self._get_with_retry(client, "/markets", params)
            except GammaClientError as exc:
                logger.debug("gamma stop at page %d: %s", page_index, exc)
                stop.set()
                return GammaPage(page_index, "failed")
            return GammaPage(page_index, "ok", acquired_at, tuple(items))

    async def _fetch_all_pages(self, client: httpx.AsyncClient) -> list[GammaPage]:
        sem = asyncio.Semaphore(self._page_concurrency)
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(self._fetch_single_page(client, sem, stop, i))
            for i in range(self._max_pages)
        ]
        pages = await asyncio.gather(*tasks)
        return list(pages)

    async def crawl_active_markets(self) -> GammaCrawl:
        """Crawl the most-traded active markets, stamping every page at body receipt.

        Walks up to `max_pages` pages of `page_limit` markets each, sorted
        by 24h volume descending, with bounded concurrency. Markets beyond
        that window have effectively zero recent volume — the size_anomaly
        niche path handles them without needing a ratio.

        Returns:
            The per-market stats and the page counts that tell absence from truncation.
        """
        started = self._clock().astimezone(UTC)
        async with httpx.AsyncClient(
            base_url=self._host,
            timeout=self._timeout,
            headers={"User-Agent": "polymarket-insider-tracker/0.1"},
        ) as client:
            pages = await self._fetch_all_pages(client)

        stats = _aggregate_pages(pages)
        logger.info("gamma sync: fetched stats for %d active markets", len(stats))
        pages_ok = sum(page.status == "ok" for page in pages)
        pages_failed = sum(page.status == "failed" for page in pages)
        return GammaCrawl(
            stats=stats,
            pages_ok=pages_ok,
            pages_failed=pages_failed,
            truncated=pages_ok < len(pages),
            started=started,
            finished=self._clock().astimezone(UTC),
        )

    async def get_active_market_stats(self) -> dict[str, GammaMarketStats]:
        """Fetch volume/liquidity for the most-traded active markets.

        Returns:
            Mapping condition_id -> GammaMarketStats.
        """
        return (await self.crawl_active_markets()).stats
