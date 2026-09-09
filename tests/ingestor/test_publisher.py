"""Tests for the Redis Streams event publisher."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.publisher import (
    DEFAULT_MAX_LEN,
    DEFAULT_STREAM_NAME,
    ConsumerGroupExistsError,
    EventPublisher,
    StreamEntry,
    _deserialize_trade_event,
    _serialize_trade_event,
)
from tests.fakes.redis import FakeRedis


# Test fixtures
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


@pytest.fixture
def fake_redis() -> FakeRedis:
    """Create a working fake Redis client."""
    return FakeRedis()


class TestSerializationFunctions:
    """Tests for serialization helper functions."""

    def test_serialize_trade_event(self, sample_trade_event: TradeEvent) -> None:
        """Test serializing a trade event."""
        data = _serialize_trade_event(sample_trade_event)

        assert data["market_id"] == "0xmarket123"
        assert data["trade_id"] == "0xtx456"
        assert data["wallet_address"] == "0xwallet789"
        assert data["side"] == "BUY"
        assert data["outcome"] == "Yes"
        assert data["outcome_index"] == "0"
        assert data["price"] == "0.65"
        assert data["size"] == "1000"
        assert data["timestamp"] == "2026-01-04T12:00:00+00:00"
        assert data["asset_id"] == "token123"
        assert data["market_slug"] == "will-it-rain"
        assert data["trader_name"] == "Alice"

    def test_serialize_all_values_are_strings(self, sample_trade_event: TradeEvent) -> None:
        """Test that all serialized values are strings."""
        data = _serialize_trade_event(sample_trade_event)

        for key, value in data.items():
            assert isinstance(key, str), f"Key {key} is not a string"
            assert isinstance(value, str), f"Value for {key} is not a string"

    def test_deserialize_trade_event(self, sample_trade_event: TradeEvent) -> None:
        """Test deserializing a trade event."""
        data = _serialize_trade_event(sample_trade_event)
        restored = _deserialize_trade_event(data)

        assert restored.market_id == sample_trade_event.market_id
        assert restored.trade_id == sample_trade_event.trade_id
        assert restored.wallet_address == sample_trade_event.wallet_address
        assert restored.side == sample_trade_event.side
        assert restored.outcome == sample_trade_event.outcome
        assert restored.outcome_index == sample_trade_event.outcome_index
        assert restored.price == sample_trade_event.price
        assert restored.size == sample_trade_event.size
        assert restored.timestamp == sample_trade_event.timestamp
        assert restored.asset_id == sample_trade_event.asset_id

    def test_deserialize_with_bytes_keys(self, sample_trade_event: TradeEvent) -> None:
        """Test deserializing with bytes keys/values (as returned by Redis)."""
        data = _serialize_trade_event(sample_trade_event)
        # Convert to bytes like Redis returns
        bytes_data = {k.encode(): v.encode() for k, v in data.items()}

        restored = _deserialize_trade_event(bytes_data)

        assert restored.market_id == sample_trade_event.market_id
        assert restored.side == sample_trade_event.side

    def test_deserialize_with_invalid_timestamp(self) -> None:
        """Test deserializing with invalid timestamp falls back to now."""
        data = {
            "market_id": "0x123",
            "timestamp": "not-a-timestamp",
            "side": "BUY",
            "price": "0.5",
            "size": "100",
        }

        event = _deserialize_trade_event(data)

        assert event.market_id == "0x123"
        # Timestamp should be recent (within last minute)
        assert (datetime.now(UTC) - event.timestamp).total_seconds() < 60

    def test_deserialize_with_missing_fields(self) -> None:
        """Test deserializing with missing fields uses defaults."""
        data = {
            "market_id": "0x123",
            "side": "SELL",
        }

        event = _deserialize_trade_event(data)

        assert event.market_id == "0x123"
        assert event.side == "SELL"
        assert event.price == Decimal("0")
        assert event.outcome == ""


class TestEventPublisher:
    """Tests for the EventPublisher class."""

    def test_init(self, fake_redis: FakeRedis) -> None:
        """Test initialization."""
        publisher = EventPublisher(cast(Any, fake_redis))

        assert publisher.stream_name == DEFAULT_STREAM_NAME
        assert publisher._max_len == DEFAULT_MAX_LEN

    def test_init_custom_config(self, fake_redis: FakeRedis) -> None:
        """Test initialization with custom config."""
        publisher = EventPublisher(
            cast(Any, fake_redis),
            stream_name="custom-stream",
            max_len=50_000,
        )

        assert publisher.stream_name == "custom-stream"
        assert publisher._max_len == 50_000

    @pytest.mark.asyncio
    async def test_publish(self, fake_redis: FakeRedis, sample_trade_event: TradeEvent) -> None:
        """Test publishing a single event."""
        publisher = EventPublisher(cast(Any, fake_redis))

        entry_id = await publisher.publish(sample_trade_event)

        assert isinstance(entry_id, str)
        assert len(entry_id) > 0
        assert await fake_redis.xlen(DEFAULT_STREAM_NAME) == 1

    @pytest.mark.asyncio
    async def test_publish_returns_decoded_bytes(self, sample_trade_event: TradeEvent) -> None:
        """Test that publish handles bytes entry IDs."""

        class FakeBytesRedis(FakeRedis):
            async def xadd(
                self,
                name: str | bytes,
                fields: Any,
                id: str = "*",
                maxlen: int | None = None,
                approximate: bool = True,
            ) -> bytes:
                eid = await super().xadd(
                    name, fields, id=id, maxlen=maxlen, approximate=approximate
                )
                return eid.encode("utf-8")

        fake_redis = FakeBytesRedis()
        publisher = EventPublisher(cast(Any, fake_redis))

        entry_id = await publisher.publish(sample_trade_event)

        assert isinstance(entry_id, str)
        assert len(entry_id) > 0

    @pytest.mark.asyncio
    async def test_publish_batch(
        self, fake_redis: FakeRedis, sample_trade_event: TradeEvent
    ) -> None:
        """Test batch publishing."""
        publisher = EventPublisher(cast(Any, fake_redis))
        events = [sample_trade_event, sample_trade_event]

        entry_ids = await publisher.publish_batch(events)

        assert len(entry_ids) == 2
        assert await fake_redis.xlen(DEFAULT_STREAM_NAME) == 2

    @pytest.mark.asyncio
    async def test_publish_batch_empty(self, fake_redis: FakeRedis) -> None:
        """Test batch publishing with empty list."""
        publisher = EventPublisher(cast(Any, fake_redis))

        entry_ids = await publisher.publish_batch([])

        assert entry_ids == []
        assert await fake_redis.xlen(DEFAULT_STREAM_NAME) == 0

    @pytest.mark.asyncio
    async def test_create_consumer_group(self, fake_redis: FakeRedis) -> None:
        """Test creating a consumer group."""
        publisher = EventPublisher(cast(Any, fake_redis))

        await publisher.create_consumer_group("test-group")

        info = await fake_redis.xinfo_stream(DEFAULT_STREAM_NAME)
        assert info["length"] == 0

    @pytest.mark.asyncio
    async def test_create_consumer_group_custom_start_id(self, fake_redis: FakeRedis) -> None:
        """Test creating a consumer group with custom start ID."""
        publisher = EventPublisher(cast(Any, fake_redis))

        await publisher.create_consumer_group("test-group", start_id="$")
        assert await publisher.ensure_consumer_group("test-group") is False

    @pytest.mark.asyncio
    async def test_create_consumer_group_already_exists(self, fake_redis: FakeRedis) -> None:
        """Test creating a consumer group that already exists."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.create_consumer_group("existing-group")

        with pytest.raises(ConsumerGroupExistsError):
            await publisher.create_consumer_group("existing-group")

    @pytest.mark.asyncio
    async def test_ensure_consumer_group_creates(self, fake_redis: FakeRedis) -> None:
        """Test ensure_consumer_group creates if not exists."""
        publisher = EventPublisher(cast(Any, fake_redis))

        created = await publisher.ensure_consumer_group("new-group")

        assert created is True

    @pytest.mark.asyncio
    async def test_ensure_consumer_group_exists(self, fake_redis: FakeRedis) -> None:
        """Test ensure_consumer_group returns False if exists."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.ensure_consumer_group("existing-group")

        created = await publisher.ensure_consumer_group("existing-group")

        assert created is False

    @pytest.mark.asyncio
    async def test_read_events(self, fake_redis: FakeRedis, sample_trade_event: TradeEvent) -> None:
        """Test reading events from stream."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.create_consumer_group("test-group", start_id="0")
        entry_id = await publisher.publish(sample_trade_event)

        entries = await publisher.read_events("test-group", "worker-1")

        assert len(entries) == 1
        assert entries[0].entry_id == entry_id
        assert entries[0].event.market_id == sample_trade_event.market_id

    @pytest.mark.asyncio
    async def test_read_events_empty(self, fake_redis: FakeRedis) -> None:
        """Test reading when no events available."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.create_consumer_group("test-group", start_id="0")

        entries = await publisher.read_events("test-group", "worker-1")

        assert entries == []

    @pytest.mark.asyncio
    async def test_read_events_with_bytes(
        self, fake_redis: FakeRedis, sample_trade_event: TradeEvent
    ) -> None:
        """Test reading events with bytes data (as from real Redis)."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.create_consumer_group("test-group", start_id="0")
        entry_id = await publisher.publish(sample_trade_event)

        entries = await publisher.read_events("test-group", "worker-1")

        assert len(entries) == 1
        assert entries[0].entry_id == entry_id

    @pytest.mark.asyncio
    async def test_read_pending(
        self, fake_redis: FakeRedis, sample_trade_event: TradeEvent
    ) -> None:
        """Test reading pending events."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.create_consumer_group("test-group", start_id="0")
        entry_id = await publisher.publish(sample_trade_event)

        await publisher.read_events("test-group", "worker-1")
        entries = await publisher.read_pending("test-group", "worker-1")

        assert len(entries) == 1
        assert entries[0].entry_id == entry_id

    @pytest.mark.asyncio
    async def test_read_pending_skips_empty_data(self) -> None:
        """Test that read_pending skips entries with no data (already acked)."""

        class FakeEmptyPendingRedis(FakeRedis):
            async def xreadgroup(
                self,
                groupname: Any,
                consumername: Any,
                streams: Any,
                count: int | None = None,
                block: int | None = None,
                noack: bool = False,
            ) -> list[Any]:
                _ = (groupname, consumername, streams, count, block, noack)
                return [
                    (
                        "trades",
                        [
                            ("1704369600000-0", {}),
                            ("1704369600000-1", None),
                        ],
                    )
                ]

        fake_redis = FakeEmptyPendingRedis()
        publisher = EventPublisher(cast(Any, fake_redis))

        entries = await publisher.read_pending("test-group", "worker-1")

        assert entries == []

    @pytest.mark.asyncio
    async def test_ack(self, fake_redis: FakeRedis, sample_trade_event: TradeEvent) -> None:
        """Test acknowledging entries."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.create_consumer_group("test-group", start_id="0")
        entry_id = await publisher.publish(sample_trade_event)
        await publisher.read_events("test-group", "worker-1")

        count = await publisher.ack("test-group", entry_id)

        assert count == 1

    @pytest.mark.asyncio
    async def test_ack_empty(self, fake_redis: FakeRedis) -> None:
        """Test ack with no entry IDs."""
        publisher = EventPublisher(cast(Any, fake_redis))

        count = await publisher.ack("test-group")

        assert count == 0

    @pytest.mark.asyncio
    async def test_get_stream_info(
        self, fake_redis: FakeRedis, sample_trade_event: TradeEvent
    ) -> None:
        """Test getting stream info."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.publish(sample_trade_event)

        info = await publisher.get_stream_info()

        assert info["length"] == 1

    @pytest.mark.asyncio
    async def test_get_stream_info_not_exists(self, fake_redis: FakeRedis) -> None:
        """Test getting stream info when stream doesn't exist."""
        publisher = EventPublisher(cast(Any, fake_redis))

        info = await publisher.get_stream_info()

        assert info == {}

    @pytest.mark.asyncio
    async def test_get_stream_length(
        self, fake_redis: FakeRedis, sample_trade_event: TradeEvent
    ) -> None:
        """Test getting stream length."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.publish(sample_trade_event)

        length = await publisher.get_stream_length()

        assert length == 1

    @pytest.mark.asyncio
    async def test_trim_stream(self, fake_redis: FakeRedis, sample_trade_event: TradeEvent) -> None:
        """Test trimming stream."""
        publisher = EventPublisher(cast(Any, fake_redis))
        await publisher.publish(sample_trade_event)
        await publisher.publish(sample_trade_event)

        await publisher.trim_stream(1)

        assert await fake_redis.xlen(DEFAULT_STREAM_NAME) == 1

    @pytest.mark.asyncio
    async def test_trim_stream_default(self, fake_redis: FakeRedis) -> None:
        """Test trimming stream with default max_len."""
        publisher = EventPublisher(cast(Any, fake_redis))

        await publisher.trim_stream()

        assert await fake_redis.xlen(DEFAULT_STREAM_NAME) == 0


class TestStreamEntry:
    """Tests for the StreamEntry dataclass."""

    def test_stream_entry(self, sample_trade_event: TradeEvent) -> None:
        """Test creating a StreamEntry."""
        entry = StreamEntry(entry_id="1704369600000-0", event=sample_trade_event)

        assert entry.entry_id == "1704369600000-0"
        assert entry.event == sample_trade_event


class TestEmptyEntryHandling:
    """Only pending re-reads skip already-acknowledged (empty) entries."""

    @pytest.mark.asyncio
    async def test_read_events_keeps_entries_with_empty_data(self) -> None:
        class FakeEmptyDataRedis(FakeRedis):
            async def xreadgroup(self, *args: Any, **kwargs: Any) -> list[Any]:
                _ = (args, kwargs)
                return [("trades", [("1704369600000-0", {})])]

        publisher = EventPublisher(cast(Any, FakeEmptyDataRedis()))
        entries = await publisher.read_events("test-group", "worker-1")

        assert [entry.entry_id for entry in entries] == ["1704369600000-0"]
        assert entries[0].event.market_id == ""

    @pytest.mark.asyncio
    async def test_read_events_drops_undecodable_entries_and_keeps_the_rest(
        self, sample_trade_event: TradeEvent
    ) -> None:
        serialized = _serialize_trade_event(sample_trade_event)

        class FakeCorruptRedis(FakeRedis):
            async def xreadgroup(self, *args: Any, **kwargs: Any) -> list[Any]:
                _ = (args, kwargs)
                return [("trades", [("1704369600000-0", None), (b"1704369600000-1", serialized)])]

        publisher = EventPublisher(cast(Any, FakeCorruptRedis()))
        entries = await publisher.read_events("test-group", "worker-1")

        assert [entry.entry_id for entry in entries] == ["1704369600000-1"]

    @pytest.mark.asyncio
    async def test_read_pending_keeps_populated_entries_after_an_empty_one(
        self, sample_trade_event: TradeEvent
    ) -> None:
        serialized = _serialize_trade_event(sample_trade_event)

        class FakeMixedRedis(FakeRedis):
            async def xreadgroup(self, *args: Any, **kwargs: Any) -> list[Any]:
                _ = (args, kwargs)
                return [("trades", [("1704369600000-0", {}), ("1704369600000-1", serialized)])]

        publisher = EventPublisher(cast(Any, FakeMixedRedis()))
        entries = await publisher.read_pending("test-group", "worker-1")

        assert [entry.entry_id for entry in entries] == ["1704369600000-1"]
