"""Fake for the gamma-api market statistics boundary."""

from __future__ import annotations

from typing import Any


class FakeGammaClient:
    """Serves market volume/liquidity statistics keyed by condition ID, or raises ``raise_error``."""

    def __init__(
        self,
        stats: dict[str, Any] | None = None,
        raise_error: Exception | None = None,
    ) -> None:
        self._stats = stats or {}
        self._raise_error = raise_error

    async def get_active_market_stats(self) -> dict[str, Any]:
        if self._raise_error is not None:
            raise self._raise_error
        return dict(self._stats)
