"""Fake for the gamma-api market statistics boundary."""

from __future__ import annotations

from datetime import UTC, datetime

from polymarket_insider_tracker.ingestor.gamma_client import (
    GammaClient,
    GammaCrawl,
    GammaMarketStats,
)

CRAWL_INSTANT = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


class FakeGammaClient(GammaClient):
    """Serves market volume/liquidity statistics keyed by condition ID, or raises ``raise_error``."""

    def __init__(
        self,
        stats: dict[str, GammaMarketStats] | None = None,
        raise_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self._stats = stats or {}
        self._raise_error = raise_error

    async def get_active_market_stats(self) -> dict[str, GammaMarketStats]:
        if self._raise_error is not None:
            raise self._raise_error
        return dict(self._stats)

    async def crawl_active_markets(self) -> GammaCrawl:
        return GammaCrawl(
            stats=await self.get_active_market_stats(),
            pages_ok=1,
            pages_failed=0,
            truncated=False,
            started=CRAWL_INSTANT,
            finished=CRAWL_INSTANT,
        )
