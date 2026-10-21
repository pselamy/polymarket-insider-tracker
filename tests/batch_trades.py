"""Indexed sample trades shared by the batch tests of the scorer and size-anomaly detector."""

from datetime import UTC, datetime
from decimal import Decimal

from polymarket_insider_tracker.ingestor.models import TradeEvent


def indexed_trades(count: int) -> list[TradeEvent]:
    """Return ``count`` BUY trades whose market, trade id and wallet carry their index."""
    return [
        TradeEvent(
            market_id=f"market_{i}",
            trade_id=f"tx_{i}",
            wallet_address=f"0xwallet{i}",
            side="BUY",
            outcome="Yes",
            outcome_index=0,
            price=Decimal("0.50"),
            size=Decimal("10000"),
            timestamp=datetime.now(UTC),
            asset_id="token_123",
        )
        for i in range(count)
    ]
