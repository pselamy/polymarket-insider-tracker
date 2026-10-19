"""Shared fixtures for the ingestor tests."""

from datetime import UTC, datetime
from decimal import Decimal

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
