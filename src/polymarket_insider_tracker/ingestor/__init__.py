"""Data ingestion layer - Real-time Polymarket trade streaming."""

from polymarket_insider_tracker.ingestor.clob_client import (
    ClobClient,
    ClobClientError,
    RetryError,
)
from polymarket_insider_tracker.ingestor.metadata_sync import (
    MarketMetadataSync,
    MetadataSyncError,
    SyncState,
    SyncStats,
)
from polymarket_insider_tracker.ingestor.models import (
    Market,
    MarketMetadata,
    Orderbook,
    OrderbookLevel,
    Token,
    TradeEvent,
    derive_category,
)
from polymarket_insider_tracker.ingestor.websocket import (
    ConnectionState,
    StreamStats as WebSocketStreamStats,
    TradeStreamError,
    TradeStreamHandler,
)

__all__ = [
    # CLOB Client
    "ClobClient",
    "ClobClientError",
    "RetryError",
    # Metadata Sync
    "MarketMetadataSync",
    "MetadataSyncError",
    "SyncState",
    "SyncStats",
    # Models
    "Market",
    "MarketMetadata",
    "Orderbook",
    "OrderbookLevel",
    "Token",
    "TradeEvent",
    "derive_category",
    # WebSocket
    "ConnectionState",
    "WebSocketStreamStats",
    "TradeStreamError",
    "TradeStreamHandler",
]
