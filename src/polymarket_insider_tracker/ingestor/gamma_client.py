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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import httpx

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


@dataclass(frozen=True)
class GammaMarketStats:
    """Volume / liquidity snapshot for a single market from gamma-api."""

    condition_id: str
    daily_volume: Decimal | None
    weekly_volume: Decimal | None
    monthly_volume: Decimal | None
    total_volume: Decimal | None
    liquidity: Decimal | None


def _to_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _parse_market(raw: dict[str, object]) -> GammaMarketStats | None:
    cid = raw.get("conditionId")
    if not cid or not isinstance(cid, str):
        return None
    return GammaMarketStats(
        condition_id=cid,
        daily_volume=_to_decimal(raw.get("volume24hr")),
        weekly_volume=_to_decimal(raw.get("volume1wk")),
        monthly_volume=_to_decimal(raw.get("volume1mo")),
        total_volume=_to_decimal(raw.get("volumeNum") or raw.get("volume")),
        liquidity=_to_decimal(raw.get("liquidityNum") or raw.get("liquidity")),
    )


class GammaClientError(Exception):
    """Raised when gamma-api returns an unrecoverable error."""


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
    ) -> None:
        self._host = host.rstrip("/")
        self._timeout = timeout_seconds
        self._page_limit = page_limit
        self._max_pages = max_pages
        self._page_concurrency = page_concurrency
        self._max_retries = max_retries
        self._retry_base = retry_base_delay_seconds

    async def _get_with_retry(
        self,
        client: httpx.AsyncClient,
        path: str,
        params: dict[str, object],
    ) -> list[dict[str, object]]:
        last_exc: Exception | None = None
        delay = self._retry_base
        for attempt in range(self._max_retries):
            try:
                resp = await client.get(path, params=params)
                resp.raise_for_status()
                payload = resp.json()
                if not isinstance(payload, list):
                    raise GammaClientError(
                        f"Unexpected gamma response shape for {path}: {type(payload).__name__}"
                    )
                return payload
            except (httpx.HTTPError, ValueError) as exc:
                last_exc = exc
                logger.warning(
                    "gamma %s attempt %d/%d failed: %s",
                    path,
                    attempt + 1,
                    self._max_retries,
                    exc,
                )
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(delay)
                    delay *= 2
        raise GammaClientError(
            f"gamma {path} failed after {self._max_retries} attempts: {last_exc}"
        )

    async def get_active_market_stats(self) -> dict[str, GammaMarketStats]:
        """Fetch volume/liquidity for the most-traded active markets.

        Walks up to `max_pages` pages of `page_limit` markets each, sorted
        by 24h volume descending, with bounded concurrency. Markets beyond
        that window have effectively zero recent volume — the size_anomaly
        niche path handles them without needing a ratio.

        Returns:
            Mapping condition_id -> GammaMarketStats.
        """
        results: dict[str, GammaMarketStats] = {}
        sem = asyncio.Semaphore(self._page_concurrency)
        stop = asyncio.Event()

        async with httpx.AsyncClient(
            base_url=self._host,
            timeout=self._timeout,
            headers={"User-Agent": "polymarket-insider-tracker/0.1"},
        ) as client:

            async def fetch_page(page_index: int) -> list[dict[str, object]]:
                if stop.is_set():
                    return []
                params = {
                    "limit": self._page_limit,
                    "offset": page_index * self._page_limit,
                    "active": "true",
                    "closed": "false",
                    "order": "volume24hr",
                    "ascending": "false",
                }
                async with sem:
                    if stop.is_set():
                        return []
                    try:
                        return await self._get_with_retry(client, "/markets", params)
                    except GammaClientError as exc:
                        # Gamma rejects offsets past its hard cap with a
                        # validation error; treat that as a clean stop.
                        logger.debug("gamma stop at page %d: %s", page_index, exc)
                        stop.set()
                        return []

            tasks = [asyncio.create_task(fetch_page(i)) for i in range(self._max_pages)]
            pages = await asyncio.gather(*tasks)

        empty_streak = 0
        for page in pages:
            if not page:
                empty_streak += 1
                continue
            empty_streak = 0
            for raw in page:
                if not isinstance(raw, dict):
                    continue
                parsed = _parse_market(raw)
                if parsed is not None:
                    results[parsed.condition_id] = parsed
            if len(page) < self._page_limit:
                # short page — we walked past the end of the active set
                empty_streak += 1
            if empty_streak >= 2:
                break

        logger.info("gamma sync: fetched stats for %d active markets", len(results))
        return results
