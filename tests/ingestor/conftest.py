"""Shared fixtures for the ingestor tests."""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from polymarket_insider_tracker.ingestor.models import TradeEvent


@pytest.fixture
def sample_trade_event() -> TradeEvent:
    """Create a sample trade event."""
    return TradeEvent(
        market_id="0xmarket123",
        trade_id="0xtx456",
        wallet_address="0xwallet789",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.65"),
        size=Decimal("1000"),
        timestamp=datetime(2026, 1, 4, 12, 0, 0, tzinfo=UTC),
        asset_id="token123",
        market_slug="will-it-rain",
        event_slug="weather-markets",
        event_title="Weather Predictions",
        trader_name="Alice",
        trader_pseudonym="AliceTrader",
    )


PatchAsyncClient = Callable[[httpx.MockTransport], None]


@pytest.fixture
def patch_async_client(monkeypatch: pytest.MonkeyPatch) -> PatchAsyncClient:
    """Replace the AsyncClient used by gamma_client with one bound to a MockTransport."""

    def _apply(handler: httpx.MockTransport) -> None:
        original = httpx.AsyncClient

        def factory(*, base_url: str, timeout: float, headers: dict[str, str]) -> httpx.AsyncClient:
            return original(base_url=base_url, timeout=timeout, headers=headers, transport=handler)

        monkeypatch.setattr(httpx, "AsyncClient", factory)

    return _apply
