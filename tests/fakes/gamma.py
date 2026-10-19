"""Fake for the gamma-api market statistics boundary."""

from __future__ import annotations

from polymarket_insider_tracker.ingestor.gamma_client import GammaClient, GammaMarketStats


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
