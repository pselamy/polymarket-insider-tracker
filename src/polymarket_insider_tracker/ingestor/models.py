"""Data models for the ingestor module."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
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
            timestamp = datetime.fromtimestamp(raw_timestamp, tz=UTC)
        else:
            timestamp = datetime.now(UTC)

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


# Category keywords for market classification
_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "politics": [
        "election",
        "president",
        "congress",
        "senate",
        "house",
        "governor",
        "mayor",
        "vote",
        "ballot",
        "democrat",
        "republican",
        "trump",
        "biden",
        "political",
        "party",
        "campaign",
        "poll",
        "primary",
        "caucus",
    ],
    "crypto": [
        "bitcoin",
        "ethereum",
        "crypto",
        "btc",
        "eth",
        "blockchain",
        "token",
        "defi",
        "nft",
        "altcoin",
        "solana",
        "cardano",
        "dogecoin",
    ],
    "sports": [
        "nfl",
        "nba",
        "mlb",
        "nhl",
        "soccer",
        "football",
        "basketball",
        "baseball",
        "hockey",
        "tennis",
        "golf",
        "ufc",
        "boxing",
        "olympics",
        "championship",
        "super bowl",
        "world cup",
        "playoffs",
        "finals",
    ],
    "entertainment": [
        "movie",
        "film",
        "oscar",
        "grammy",
        "emmy",
        "album",
        "song",
        "celebrity",
        "netflix",
        "disney",
        "streaming",
        "box office",
        "tv show",
        "series",
        "actor",
        "actress",
        "music",
    ],
    "finance": [
        "stock",
        "market",
        "fed",
        "interest rate",
        "inflation",
        "gdp",
        "unemployment",
        "recession",
        "economy",
        "s&p",
        "nasdaq",
        "dow",
        "treasury",
        "bond",
        "forex",
        "gold",
        "oil",
        "commodity",
    ],
    "tech": [
        "apple",
        "google",
        "microsoft",
        "amazon",
        "meta",
        "tesla",
        "ai",
        "artificial intelligence",
        "chatgpt",
        "openai",
        "semiconductor",
        "iphone",
        "android",
        "software",
        "hardware",
        "startup",
    ],
    "science": [
        "nasa",
        "space",
        "climate",
        "weather",
        "vaccine",
        "covid",
        "fda",
        "drug",
        "trial",
        "research",
        "study",
        "discovery",
    ],
}


def derive_category(title: str) -> str:
    """Derive a market category from the market title.

    Args:
        title: The market question or title.

    Returns:
        Category string, or "other" if no match found.
    """
    title_lower = title.lower()

    for category, keywords in _CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in title_lower:
                return category

    return "other"


@dataclass(frozen=True)
class MarketMetadata:
    """Extended market metadata with derived fields and caching support.

    This combines the core Market data with derived metadata like category
    and is designed for efficient caching in Redis.
    """

    # Core market data
    condition_id: str
    question: str
    description: str
    tokens: tuple[Token, ...]
    end_date: datetime | None = None
    active: bool = True
    closed: bool = False

    # Derived metadata
    category: str = "other"

    # Cache metadata
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def from_market(cls, market: Market) -> "MarketMetadata":
        """Create MarketMetadata from a Market object.

        Args:
            market: The source Market object.

        Returns:
            MarketMetadata with derived fields populated.
        """
        return cls(
            condition_id=market.condition_id,
            question=market.question,
            description=market.description,
            tokens=market.tokens,
            end_date=market.end_date,
            active=market.active,
            closed=market.closed,
            category=derive_category(market.question),
            last_updated=datetime.now(UTC),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary for Redis storage.

        Returns:
            Dictionary representation suitable for JSON serialization.
        """
        return {
            "condition_id": self.condition_id,
            "question": self.question,
            "description": self.description,
            "tokens": [
                {
                    "token_id": t.token_id,
                    "outcome": t.outcome,
                    "price": str(t.price) if t.price is not None else None,
                }
                for t in self.tokens
            ],
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "active": self.active,
            "closed": self.closed,
            "category": self.category,
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MarketMetadata":
        """Deserialize from a dictionary (from Redis storage).

        Args:
            data: Dictionary from Redis.

        Returns:
            MarketMetadata instance.
        """
        tokens_data = data.get("tokens", [])
        tokens = tuple(Token.from_dict(t) for t in tokens_data)

        end_date = None
        end_date_str = data.get("end_date")
        if end_date_str:
            try:
                end_date = datetime.fromisoformat(end_date_str)
            except (ValueError, AttributeError):
                pass

        last_updated_str = data.get("last_updated")
        if last_updated_str:
            try:
                last_updated = datetime.fromisoformat(last_updated_str)
            except (ValueError, AttributeError):
                last_updated = datetime.now(UTC)
        else:
            last_updated = datetime.now(UTC)

        return cls(
            condition_id=str(data["condition_id"]),
            question=str(data.get("question", "")),
            description=str(data.get("description", "")),
            tokens=tokens,
            end_date=end_date,
            active=bool(data.get("active", True)),
            closed=bool(data.get("closed", False)),
            category=str(data.get("category", "other")),
            last_updated=last_updated,
        )
