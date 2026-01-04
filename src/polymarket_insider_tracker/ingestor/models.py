"""Data models for the ingestor module."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal


@dataclass(frozen=True)
class Token:
    """Represents a token in a Polymarket market."""

    token_id: str
    outcome: str
    price: Decimal | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Token":
        """Create a Token from a dictionary."""
        price = data.get("price")
        return cls(
            token_id=str(data["token_id"]),
            outcome=str(data["outcome"]),
            price=Decimal(str(price)) if price is not None else None,
        )


@dataclass(frozen=True)
class Market:
    """Represents a Polymarket prediction market."""

    condition_id: str
    question: str
    description: str
    tokens: tuple[Token, ...]
    end_date: datetime | None = None
    active: bool = True
    closed: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Market":
        """Create a Market from a dictionary response."""
        tokens_data = data.get("tokens", [])
        tokens = tuple(Token.from_dict(t) for t in tokens_data)

        end_date = None
        end_date_iso = data.get("end_date_iso")
        if end_date_iso:
            try:
                end_date = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        return cls(
            condition_id=str(data["condition_id"]),
            question=str(data.get("question", "")),
            description=str(data.get("description", "")),
            tokens=tokens,
            end_date=end_date,
            active=bool(data.get("active", True)),
            closed=bool(data.get("closed", False)),
        )


@dataclass(frozen=True)
class OrderbookLevel:
    """Represents a single price level in an orderbook."""

    price: Decimal
    size: Decimal

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrderbookLevel":
        """Create an OrderbookLevel from a dictionary."""
        return cls(
            price=Decimal(str(data["price"])),
            size=Decimal(str(data["size"])),
        )


@dataclass(frozen=True)
class Orderbook:
    """Represents an orderbook for a Polymarket token."""

    market: str
    asset_id: str
    bids: tuple[OrderbookLevel, ...]
    asks: tuple[OrderbookLevel, ...]
    tick_size: Decimal
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_clob_orderbook(cls, orderbook: Any) -> "Orderbook":
        """Create an Orderbook from a py-clob-client orderbook object."""
        bids = tuple(
            OrderbookLevel(
                price=Decimal(str(bid.price)),
                size=Decimal(str(bid.size)),
            )
            for bid in (orderbook.bids or [])
        )
        asks = tuple(
            OrderbookLevel(
                price=Decimal(str(ask.price)),
                size=Decimal(str(ask.size)),
            )
            for ask in (orderbook.asks or [])
        )

        return cls(
            market=str(orderbook.market),
            asset_id=str(orderbook.asset_id),
            bids=bids,
            asks=asks,
            tick_size=Decimal(str(orderbook.tick_size)),
        )

    @property
    def best_bid(self) -> Decimal | None:
        """Return the best bid price, or None if no bids."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        """Return the best ask price, or None if no asks."""
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> Decimal | None:
        """Return the bid-ask spread, or None if missing data."""
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None

    @property
    def midpoint(self) -> Decimal | None:
        """Return the midpoint price, or None if missing data."""
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2
        return None


@dataclass(frozen=True)
class TradeEvent:
    """Represents a trade event from the Polymarket WebSocket feed.

    This captures all the information about a single trade execution,
    including the market, wallet, trade details, and metadata.
    """

    # Core trade identifiers
    market_id: str  # conditionId - the market/CTF condition ID
    trade_id: str  # transactionHash - unique trade identifier
    wallet_address: str  # proxyWallet - trader's wallet address

    # Trade details
    side: Literal["BUY", "SELL"]
    outcome: str  # Human-readable outcome (e.g., "Yes", "No")
    outcome_index: int  # Index of the outcome (0 or 1)
    price: Decimal
    size: Decimal  # Number of shares traded
    timestamp: datetime

    # Asset information
    asset_id: str  # ERC1155 token ID

    # Market metadata
    market_slug: str = ""
    event_slug: str = ""
    event_title: str = ""

    # Trader metadata (optional - may not be available for all trades)
    trader_name: str = ""
    trader_pseudonym: str = ""

    @classmethod
    def from_websocket_message(cls, data: dict[str, Any]) -> "TradeEvent":
        """Create a TradeEvent from a WebSocket activity/trade message.

        Args:
            data: The payload from a WebSocket trade message.

        Returns:
            TradeEvent instance.
        """
        # Parse timestamp - it's a Unix timestamp in seconds
        raw_timestamp = data.get("timestamp", 0)
        if isinstance(raw_timestamp, int):
            timestamp = datetime.fromtimestamp(raw_timestamp, tz=timezone.utc)
        else:
            timestamp = datetime.now(timezone.utc)

        # Parse side - normalize to uppercase
        side_raw = str(data.get("side", "BUY")).upper()
        side: Literal["BUY", "SELL"] = "BUY" if side_raw == "BUY" else "SELL"

        return cls(
            market_id=str(data.get("conditionId", "")),
            trade_id=str(data.get("transactionHash", "")),
            wallet_address=str(data.get("proxyWallet", "")),
            side=side,
            outcome=str(data.get("outcome", "")),
            outcome_index=int(data.get("outcomeIndex", 0)),
            price=Decimal(str(data.get("price", 0))),
            size=Decimal(str(data.get("size", 0))),
            timestamp=timestamp,
            asset_id=str(data.get("asset", "")),
            market_slug=str(data.get("slug", "")),
            event_slug=str(data.get("eventSlug", "")),
            event_title=str(data.get("title", "")),
            trader_name=str(data.get("name", "")),
            trader_pseudonym=str(data.get("pseudonym", "")),
        )

    @property
    def is_buy(self) -> bool:
        """Return True if this is a buy trade."""
        return self.side == "BUY"

    @property
    def is_sell(self) -> bool:
        """Return True if this is a sell trade."""
        return self.side == "SELL"

    @property
    def notional_value(self) -> Decimal:
        """Return the notional value of the trade (price * size)."""
        return self.price * self.size
