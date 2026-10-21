"""Gamma volume refresh on its own cadence, decoupled from the CLOB crawl.

Contract: ``specs/004-truthful-detection-contract/data-model.md`` section 1.4 and research R02a.
The CLOB crawl duration no longer delays volume freshness: the oldest stamp a reader can see is
``interval + 2 * crawl_offset + write_time`` (default interval 180 s against the 600 s rule).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from polymarket_insider_tracker.ingestor.gamma_client import (
    Clock,
    GammaClient,
    GammaCrawl,
    utc_now,
)
from polymarket_insider_tracker.ingestor.volume_store import VolumeStore, crawl_outcome
from polymarket_insider_tracker.redaction import redact_exception_message

logger = logging.getLogger(__name__)

DEFAULT_GAMMA_REFRESH_INTERVAL_SECONDS = 180


class GammaVolumeRefresh:
    """Periodically crawls Gamma and stores one volume record per returned market."""

    def __init__(
        self,
        gamma: GammaClient,
        store: VolumeStore,
        *,
        interval_seconds: float = DEFAULT_GAMMA_REFRESH_INTERVAL_SECONDS,
        clock: Clock = utc_now,
    ) -> None:
        self._gamma = gamma
        self._store = store
        self._interval = interval_seconds
        self._clock = clock
        self._task: asyncio.Task[None] | None = None

    async def _crawl(self) -> GammaCrawl | None:
        try:
            return await self._gamma.crawl_active_markets()
        except Exception as e:
            logger.warning(
                "gamma stats fetch failed (continuing without volume): %s",
                redact_exception_message(e),
            )
            return None

    async def refresh_once(self) -> None:
        """One crawl and store; never raises, so a degraded Gamma never stops the CLOB sync."""
        crawl = await self._crawl()
        finished = crawl.finished if crawl is not None else self._clock()
        try:
            written = await self._store.write_crawl(crawl, finished)
        except Exception as e:
            logger.warning("gamma volume store failed: %s", redact_exception_message(e))
            return
        logger.info("gamma volume refresh: %s, %d records stored", crawl_outcome(crawl), written)

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self.refresh_once()

    def start(self) -> None:
        """Start the periodic refresh; the caller has already run the first one."""
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
