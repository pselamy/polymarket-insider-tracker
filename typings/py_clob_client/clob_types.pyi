from dataclasses import dataclass

@dataclass
class BookParams:
    token_id: str
    side: str = ""

@dataclass
class OrderSummary:
    price: str | None = None
    size: str | None = None

@dataclass
class OrderBookSummary:
    market: str | None = None
    asset_id: str | None = None
    timestamp: str | None = None
    bids: list[OrderSummary] | None = None
    asks: list[OrderSummary] | None = None
    min_order_size: str | None = None
    neg_risk: bool | None = None
    tick_size: str | None = None
    hash: str | None = None
