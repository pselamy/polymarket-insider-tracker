"""Redis Streams event publisher for trade events.

This module provides an event publisher that writes normalized trade events
to Redis Streams, enabling downstream consumers to process events independently.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .models import TradeEvent

logger = logging.getLogger(__name__)


# Default configuration
DEFAULT_STREAM_NAME = "trades"
DEFAULT_MAX_LEN = 100_000  # 100k events
DEFAULT_BLOCK_MS = 1000
DEFAULT_COUNT = 10


class PublisherError(Exception):
    """Base exception for publisher errors."""

    pass


class ConsumerGroupExistsError(PublisherError):
    """Raised when trying to create a consumer group that already exists."""

    pass


@dataclass
class StreamEntry:
    """Represents an entry read from a Redis Stream."""

    entry_id: str
    event: TradeEvent


def _serialize_trade_event(event: TradeEvent) -> dict[str, str]:
    """Serialize a TradeEvent to a dict suitable for Redis Streams.

    Redis Streams require string key-value pairs, so we convert all
    values to strings.

    Args:
        event: The TradeEvent to serialize.

    Returns:
        Dictionary with string keys and values.
    """
    return {
        "market_id": event.market_id,
        "trade_id": event.trade_id,
        "wallet_address": event.wallet_address,
        "side": event.side,
        "outcome": event.outcome,
        "outcome_index": str(event.outcome_index),
        "price": str(event.price),
        "size": str(event.size),
        "timestamp": event.timestamp.isoformat(),
        "asset_id": event.asset_id,
        "market_slug": event.market_slug,
        "event_slug": event.event_slug,
        "event_title": event.event_title,
        "trader_name": event.trader_name,
        "trader_pseudonym": event.trader_pseudonym,
    }


def _deserialize_trade_event(data: dict[bytes | str, bytes | str]) -> TradeEvent:
    """Deserialize a TradeEvent from Redis Stream data.

    Args:
        data: The raw data from Redis Stream (may have bytes keys/values).

    Returns:
        TradeEvent instance.
    """
    # Convert bytes to strings if needed
    decoded: dict[str, str] = {}
    for k, v in data.items():
        key = k.decode() if isinstance(k, bytes) else k
        value = v.decode() if isinstance(v, bytes) else v
        decoded[key] = value

    # Parse timestamp
    timestamp_str = decoded.get("timestamp", "")
    try:
        timestamp = datetime.fromisoformat(timestamp_str)
    except (ValueError, TypeError):
        timestamp = datetime.now(UTC)

    # Parse side
    side_raw = decoded.get("side", "BUY").upper()
    side: Literal["BUY", "SELL"] = "BUY" if side_raw == "BUY" else "SELL"

    return TradeEvent(
        market_id=decoded.get("market_id", ""),
        trade_id=decoded.get("trade_id", ""),
        wallet_address=decoded.get("wallet_address", ""),
        side=side,
        outcome=decoded.get("outcome", ""),
        outcome_index=int(decoded.get("outcome_index", "0")),
        price=Decimal(decoded.get("price", "0")),
        size=Decimal(decoded.get("size", "0")),
        timestamp=timestamp,
        asset_id=decoded.get("asset_id", ""),
        market_slug=decoded.get("market_slug", ""),
        event_slug=decoded.get("event_slug", ""),
        event_title=decoded.get("event_title", ""),
        trader_name=decoded.get("trader_name", ""),
        trader_pseudonym=decoded.get("trader_pseudonym", ""),
    )


class EventPublisher:
    """Event publisher using Redis Streams.

    This class wraps Redis Streams to provide:
    - Publishing single or batch trade events
    - Consumer group management
    - Event reading for consumers

    Example:
        ```python
        redis = Redis.from_url("redis://localhost:6379")
        publisher = EventPublisher(redis)

        # Publish events
        event_id = await publisher.publish(trade_event)

        # Create consumer group for downstream processing
        await publisher.create_consumer_group("wallet-profiler")

        # Read events as consumer
        entries = await publisher.read_events(
            group_name="wallet-profiler",
            consumer_name="worker-1"
        )
        for entry in entries:
            process(entry.event)
            await publisher.ack(entry.entry_id)
        ```
    """

    def __init__(
        self,
        redis: Redis,
        stream_name: str = DEFAULT_STREAM_NAME,
        *,
        max_len: int = DEFAULT_MAX_LEN,
    ) -> None:
        """Initialize the event publisher.

        Args:
            redis: Redis async client.
            stream_name: Name of the Redis Stream.
            max_len: Maximum number of entries to keep in stream.
        """
        self._redis = redis
        self._stream_name = stream_name
        self._max_len = max_len

    @property
    def stream_name(self) -> str:
        """Return the stream name."""
        return self._stream_name

    async def publish(self, event: TradeEvent) -> str:
        """Publish a single trade event to the stream.

        Args:
            event: The TradeEvent to publish.

        Returns:
            The entry ID assigned by Redis.
        """
        data = _serialize_trade_event(event)
        # redis-py typing expects broader dict type than dict[str, str]
        entry_id = await self._redis.xadd(
            self._stream_name,
            data,  # type: ignore[arg-type]
            maxlen=self._max_len,
        )
        # entry_id may be bytes or str
        if isinstance(entry_id, bytes):
            return entry_id.decode()
        return str(entry_id)

    async def publish_batch(self, events: Sequence[TradeEvent]) -> list[str]:
        """Publish multiple trade events atomically.

        Uses a Redis pipeline for efficiency.

        Args:
            events: Sequence of TradeEvents to publish.

        Returns:
            List of entry IDs assigned by Redis.
        """
        if not events:
            return []

        pipe = self._redis.pipeline()
        for event in events:
            data = _serialize_trade_event(event)
            # redis-py typing expects broader dict type than dict[str, str]
            pipe.xadd(self._stream_name, data, maxlen=self._max_len)  # type: ignore[arg-type]

        results = await pipe.execute()

        entry_ids: list[str] = []
        for entry_id in results:
            if isinstance(entry_id, bytes):
                entry_ids.append(entry_id.decode())
            else:
                entry_ids.append(str(entry_id))

        return entry_ids

    async def create_consumer_group(
        self,
        group_name: str,
        start_id: str = "0",
        *,
        mkstream: bool = True,
    ) -> None:
        """Create a consumer group for the stream.

        Args:
            group_name: Name of the consumer group.
            start_id: ID to start reading from ("0" = beginning, "$" = new only).
            mkstream: Create the stream if it doesn't exist.

        Raises:
            ConsumerGroupExistsError: If the group already exists.
        """
        try:
            await self._redis.xgroup_create(
                self._stream_name,
                group_name,
                id=start_id,
                mkstream=mkstream,
            )
            logger.info(f"Created consumer group '{group_name}' on stream '{self._stream_name}'")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                raise ConsumerGroupExistsError(
                    f"Consumer group '{group_name}' already exists"
                ) from e
            raise

    async def ensure_consumer_group(
        self,
        group_name: str,
        start_id: str = "0",
    ) -> bool:
        """Ensure a consumer group exists, creating it if needed.

        Args:
            group_name: Name of the consumer group.
            start_id: ID to start reading from if creating.

        Returns:
            True if the group was created, False if it already existed.
        """
        try:
            await self.create_consumer_group(group_name, start_id)
            return True
        except ConsumerGroupExistsError:
            return False

    async def read_events(
        self,
        group_name: str,
        consumer_name: str,
        *,
        count: int = DEFAULT_COUNT,
        block_ms: int = DEFAULT_BLOCK_MS,
    ) -> list[StreamEntry]:
        """Read events from the stream as a consumer.

        Args:
            group_name: Consumer group name.
            consumer_name: Name of this consumer within the group.
            count: Maximum number of entries to read.
            block_ms: Milliseconds to block waiting for new entries.

        Returns:
            List of StreamEntry with entry ID and TradeEvent.
        """
        # Read new entries (> means entries not delivered to this consumer)
        results = await self._redis.xreadgroup(
            group_name,
            consumer_name,
            {self._stream_name: ">"},
            count=count,
            block=block_ms,
        )

        entries: list[StreamEntry] = []
        if not results:
            return entries

        # Results format: [[stream_name, [(entry_id, data), ...]]]
        for _stream_name, stream_entries in results:
            for entry_id, data in stream_entries:
                # Decode entry_id
                entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)

                try:
                    event = _deserialize_trade_event(data)
                    entries.append(StreamEntry(entry_id=entry_id_str, event=event))
                except Exception as e:
                    logger.warning(f"Failed to deserialize entry {entry_id_str}: {e}")

        return entries

    async def read_pending(
        self,
        group_name: str,
        consumer_name: str,
        *,
        count: int = DEFAULT_COUNT,
    ) -> list[StreamEntry]:
        """Read pending (unacknowledged) entries for a consumer.

        This is useful for recovering from crashes - entries that were
        delivered but not acknowledged will be re-read.

        Args:
            group_name: Consumer group name.
            consumer_name: Name of this consumer.
            count: Maximum number of entries to read.

        Returns:
            List of pending StreamEntry.
        """
        # Read pending entries (0 means all pending entries)
        results = await self._redis.xreadgroup(
            group_name,
            consumer_name,
            {self._stream_name: "0"},
            count=count,
        )

        entries: list[StreamEntry] = []
        if not results:
            return entries

        for _stream_name, stream_entries in results:
            for entry_id, data in stream_entries:
                entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)

                # Skip entries with no data (already acked)
                if not data:
                    continue

                try:
                    event = _deserialize_trade_event(data)
                    entries.append(StreamEntry(entry_id=entry_id_str, event=event))
                except Exception as e:
                    logger.warning(f"Failed to deserialize pending entry {entry_id_str}: {e}")

        return entries

    async def ack(self, group_name: str, *entry_ids: str) -> int:
        """Acknowledge that entries have been processed.

        Args:
            group_name: Consumer group name.
            *entry_ids: Entry IDs to acknowledge.

        Returns:
            Number of entries acknowledged.
        """
        if not entry_ids:
            return 0
        result = await self._redis.xack(self._stream_name, group_name, *entry_ids)
        return int(result)

    async def get_stream_info(self) -> dict[str, Any]:
        """Get information about the stream.

        Returns:
            Dictionary with stream info (length, groups, etc.).
        """
        try:
            info = await self._redis.xinfo_stream(self._stream_name)
            return dict(info) if info else {}
        except ResponseError:
            return {}

    async def get_stream_length(self) -> int:
        """Get the current length of the stream.

        Returns:
            Number of entries in the stream.
        """
        result = await self._redis.xlen(self._stream_name)
        return int(result)

    async def trim_stream(self, max_len: int | None = None) -> int:
        """Trim the stream to a maximum length.

        Args:
            max_len: Maximum entries to keep (uses default if not specified).

        Returns:
            Number of entries removed.
        """
        length = max_len or self._max_len
        result = await self._redis.xtrim(self._stream_name, maxlen=length)
        return int(result)
