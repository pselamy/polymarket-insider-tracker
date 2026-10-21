"""Redis Streams event publisher for trade events.

This module provides an event publisher that writes normalized trade events
to Redis Streams, enabling downstream consumers to process events independently.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT

from polymarket_insider_tracker.redaction import redact_exception_message

from ._publisher_codec import (
    decode_entry_id as _decode_entry_id,
)
from ._publisher_codec import (
    decode_stream_dict as _decode_stream_dict,
)
from ._publisher_codec import (
    deserialize_trade_event as _deserialize_trade_event,
)
from ._publisher_codec import (
    parse_event_timestamp as _parse_event_timestamp,
)
from ._publisher_codec import (
    serialize_trade_event as _serialize_trade_event,
)
from ._stream_reply import reply_batches, stream_row
from .models import TradeEvent

logger = logging.getLogger(__name__)


# Compatibility: keep the historical ``publisher.<name>`` module identity so
# existing import paths, qualnames and pickle references keep resolving.
_serialize_trade_event.__module__ = __name__
_serialize_trade_event.__qualname__ = "_serialize_trade_event"
_decode_stream_dict.__module__ = __name__
_decode_stream_dict.__qualname__ = "_decode_stream_dict"
_parse_event_timestamp.__module__ = __name__
_parse_event_timestamp.__qualname__ = "_parse_event_timestamp"
_decode_entry_id.__module__ = __name__
_decode_entry_id.__qualname__ = "_decode_entry_id"
_deserialize_trade_event.__module__ = __name__
_deserialize_trade_event.__qualname__ = "_deserialize_trade_event"


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


def _encodable_fields(data: dict[str, str]) -> dict[FieldT, EncodableT]:
    # redis-py's mutable dict parameter is invariant even though each string is encodable.
    fields: dict[FieldT, EncodableT] = {}
    for key, value in data.items():
        fields[key] = value
    return fields


def _parse_stream_entry(entry: object, *, context: str, skip_empty: bool) -> StreamEntry | None:
    row = stream_row(entry)
    if row is None:
        logger.warning("Failed to deserialize %s: malformed stream row", context)
        return None
    entry_id, data = row
    if skip_empty and not data:
        return None
    entry_id_str = _decode_entry_id(entry_id)
    try:
        if data is None:
            raise TypeError("stream entry has no field mapping")
        event = _deserialize_trade_event(data)
        return StreamEntry(entry_id=entry_id_str, event=event)
    except Exception as e:
        logger.warning(
            "Failed to deserialize %s %s: %s",
            context,
            entry_id_str,
            redact_exception_message(e),
        )
        return None


def _collect_entries_from_stream(
    stream_entries: list[object],
    *,
    context: str,
    skip_empty: bool,
) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for raw_entry in stream_entries:
        entry = _parse_stream_entry(raw_entry, context=context, skip_empty=skip_empty)
        if entry is not None:
            entries.append(entry)
    return entries


def _parse_stream_results(results: object, *, context: str, skip_empty: bool) -> list[StreamEntry]:
    """Flatten ``XREADGROUP`` results into entries.

    ``skip_empty`` drops entries whose data is empty; pending re-reads return
    such entries for messages that were already acknowledged or trimmed.
    """
    if not results:
        return []
    batches = reply_batches(results)
    if batches is None:
        raise PublisherError("malformed XREADGROUP reply")
    entries: list[StreamEntry] = []
    for stream_entries in batches:
        entries.extend(
            _collect_entries_from_stream(stream_entries, context=context, skip_empty=skip_empty)
        )
    return entries


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
        redis_fields = _encodable_fields(data)
        entry_id = await self._redis.xadd(
            self._stream_name,
            redis_fields,
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
            redis_fields = _encodable_fields(data)
            pipe.xadd(self._stream_name, redis_fields, maxlen=self._max_len)

        results = await pipe.execute()
        return [_decode_entry_id(entry_id) for entry_id in results]

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
        return _parse_stream_results(results, context="entry", skip_empty=False)

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
        return _parse_stream_results(results, context="pending entry", skip_empty=True)

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

    async def get_stream_info(self) -> dict[str, object]:
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
