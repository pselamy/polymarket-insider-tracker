"""Data ingestion layer - near-real-time acquisition of public Polymarket trades."""

from polymarket_insider_tracker.ingestor.clob_client import (
    ClobClient,
    ClobClientError,
    RetryError,
)
from polymarket_insider_tracker.ingestor.health import (
    HealthMonitor,
    HealthReport,
    HealthStatus,
    StreamHealth,
    StreamStatus,
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
from polymarket_insider_tracker.ingestor.publisher import (
    ConsumerGroupExistsError,
    EventPublisher,
    PublisherError,
    StreamEntry,
)
from polymarket_insider_tracker.ingestor.trade_poller import (
    IngestionState,
    IngestionStatus,
    TradePoller,
)
from polymarket_insider_tracker.ingestor.trade_rows import TradeObservation
from polymarket_insider_tracker.ingestor.trades_source import (
    TradesSourceError,
    TradesTerminalError,
    TradesTransientError,
)
from polymarket_insider_tracker.ingestor.websocket import (
    ConnectionState,
    TradeStreamError,
    TradeStreamHandler,
)
from polymarket_insider_tracker.ingestor.websocket import (
    StreamStats as WebSocketStreamStats,
)

__all__ = [
    # CLOB Client
    "ClobClient",
    "ClobClientError",
    "RetryError",
    # Health Monitor
    "HealthMonitor",
    "HealthReport",
    "HealthStatus",
    "StreamHealth",
    "StreamStatus",
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
    # Publisher
    "ConsumerGroupExistsError",
    "EventPublisher",
    "PublisherError",
    "StreamEntry",
    # Trade acquisition
    "IngestionState",
    "IngestionStatus",
    "TradePoller",
    "TradeObservation",
    "TradesSourceError",
    "TradesTerminalError",
    "TradesTransientError",
    # WebSocket (deprecated; retained for the deprecation window)
    "ConnectionState",
    "WebSocketStreamStats",
    "TradeStreamError",
    "TradeStreamHandler",
]
