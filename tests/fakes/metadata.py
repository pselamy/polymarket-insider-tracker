"""In-memory fake for MarketMetadataSync."""

from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.models import MarketMetadata


class FakeMetadataSync(MarketMetadataSync):
    """In-memory fake for MarketMetadataSync with deterministic control."""

    def __init__(
        self,
        markets: dict[str, MarketMetadata | None] | None = None,
        default_market: MarketMetadata | None = None,
        errors: list[Exception] | None = None,
    ) -> None:
        self.markets: dict[str, MarketMetadata | None] = dict(markets or {})
        self.default_market: MarketMetadata | None = default_market
        self._errors: list[Exception] = list(errors or [])
        self.calls: list[str] = []

    def set_market(self, condition_id: str, metadata: MarketMetadata | None) -> None:
        """Set metadata for a specific market condition ID."""
        self.markets[condition_id] = metadata

    def set_default(self, metadata: MarketMetadata | None) -> None:
        """Set fallback metadata returned when condition ID is not in markets."""
        self.default_market = metadata

    def add_error(self, error: Exception) -> None:
        """Queue an exception to be raised on the next call to get_market."""
        self._errors.append(error)

    async def get_market(self, condition_id: str) -> MarketMetadata | None:
        """Retrieve market metadata or raise queued error."""
        self.calls.append(condition_id)
        if self._errors:
            raise self._errors.pop(0)
        if condition_id in self.markets:
            return self.markets[condition_id]
        return self.default_market
