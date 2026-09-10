"""In-memory market metadata lookup used in place of ``MarketMetadataSync``."""

from __future__ import annotations

from polymarket_insider_tracker.ingestor.models import MarketMetadata


class FakeMetadataSync:
    """A market-metadata lookup keyed by condition ID.

    ``markets`` holds known markets, ``fallback`` answers every other lookup, and ``failures`` maps
    condition IDs to the exception the lookup raises for them. Behaviour is decided by the
    requested market, never by call order.
    """

    def __init__(self) -> None:
        self.markets: dict[str, MarketMetadata] = {}
        self.fallback: MarketMetadata | None = None
        self.failures: dict[str, Exception] = {}

    async def get_market(self, condition_id: str) -> MarketMetadata | None:
        if condition_id in self.failures:
            raise self.failures[condition_id]
        return self.markets.get(condition_id, self.fallback)

    async def get_cached_market(self, condition_id: str) -> MarketMetadata | None:
        """The cache-only lookup: known markets answer, everything else is absent."""
        if condition_id in self.failures:
            raise self.failures[condition_id]
        return self.markets.get(condition_id)
