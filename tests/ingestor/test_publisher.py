"""Tests for the Redis Streams event publisher."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import ResponseError

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.publisher import (
    DEFAULT_BLOCK_MS,
    DEFAULT_COUNT,
    DEFAULT_MAX_LEN,
    DEFAULT_STREAM_NAME,
    ConsumerGroupExistsError,
    EventPublisher,
    StreamEntry,
    _deserialize_trade_event,
    _serialize_trade_event,
)


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
def mock_redis() -> AsyncMock:
    """Create a mock Redis client."""
    redis = AsyncMock()
    redis.xadd = AsyncMock(return_value="1704369600000-0")
    redis.xreadgroup = AsyncMock(return_value=[])
    redis.xack = AsyncMock(return_value=1)
    redis.xlen = AsyncMock(return_value=100)
    redis.xtrim = AsyncMock(return_value=0)
    redis.xinfo_stream = AsyncMock(return_value={"length": 100})
    redis.xgroup_create = AsyncMock()
    redis.pipeline = MagicMock()
    return redis


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

    def test_init(self, mock_redis: AsyncMock) -> None:
        """Test initialization."""
        publisher = EventPublisher(mock_redis)

        assert publisher.stream_name == DEFAULT_STREAM_NAME
        assert publisher._max_len == DEFAULT_MAX_LEN

    def test_init_custom_config(self, mock_redis: AsyncMock) -> None:
        """Test initialization with custom config."""
        publisher = EventPublisher(
            mock_redis,
            stream_name="custom-stream",
            max_len=50_000,
        )

        assert publisher.stream_name == "custom-stream"
        assert publisher._max_len == 50_000

    @pytest.mark.asyncio
    async def test_publish(self, mock_redis: AsyncMock, sample_trade_event: TradeEvent) -> None:
        """Test publishing a single event."""
        publisher = EventPublisher(mock_redis)

        entry_id = await publisher.publish(sample_trade_event)

        assert entry_id == "1704369600000-0"
        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == DEFAULT_STREAM_NAME
        assert call_args[1]["maxlen"] == DEFAULT_MAX_LEN

    @pytest.mark.asyncio
    async def test_publish_returns_decoded_bytes(
        self, mock_redis: AsyncMock, sample_trade_event: TradeEvent
    ) -> None:
        """Test that publish handles bytes entry IDs."""
        mock_redis.xadd = AsyncMock(return_value=b"1704369600000-0")
        publisher = EventPublisher(mock_redis)

        entry_id = await publisher.publish(sample_trade_event)

        assert entry_id == "1704369600000-0"
        assert isinstance(entry_id, str)

    @pytest.mark.asyncio
    async def test_publish_batch(
        self, mock_redis: AsyncMock, sample_trade_event: TradeEvent
    ) -> None:
        """Test batch publishing."""
        mock_pipeline = AsyncMock()
        mock_pipeline.xadd = MagicMock()
        mock_pipeline.execute = AsyncMock(return_value=["1704369600000-0", "1704369600000-1"])
        mock_redis.pipeline.return_value = mock_pipeline

        publisher = EventPublisher(mock_redis)
        events = [sample_trade_event, sample_trade_event]

        entry_ids = await publisher.publish_batch(events)

        assert len(entry_ids) == 2
        assert entry_ids[0] == "1704369600000-0"
        assert entry_ids[1] == "1704369600000-1"
        assert mock_pipeline.xadd.call_count == 2

    @pytest.mark.asyncio
    async def test_publish_batch_empty(self, mock_redis: AsyncMock) -> None:
        """Test batch publishing with empty list."""
        publisher = EventPublisher(mock_redis)

        entry_ids = await publisher.publish_batch([])

        assert entry_ids == []
        mock_redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_consumer_group(self, mock_redis: AsyncMock) -> None:
        """Test creating a consumer group."""
        publisher = EventPublisher(mock_redis)

        await publisher.create_consumer_group("test-group")

        mock_redis.xgroup_create.assert_called_once_with(
            DEFAULT_STREAM_NAME,
            "test-group",
            id="0",
            mkstream=True,
        )

    @pytest.mark.asyncio
    async def test_create_consumer_group_custom_start_id(self, mock_redis: AsyncMock) -> None:
        """Test creating a consumer group with custom start ID."""
        publisher = EventPublisher(mock_redis)

        await publisher.create_consumer_group("test-group", start_id="$")

        call_args = mock_redis.xgroup_create.call_args
        assert call_args[1]["id"] == "$"

    @pytest.mark.asyncio
    async def test_create_consumer_group_already_exists(self, mock_redis: AsyncMock) -> None:
        """Test creating a consumer group that already exists."""
        mock_redis.xgroup_create.side_effect = ResponseError(
            "BUSYGROUP Consumer Group name already exists"
        )
        publisher = EventPublisher(mock_redis)

        with pytest.raises(ConsumerGroupExistsError):
            await publisher.create_consumer_group("existing-group")

    @pytest.mark.asyncio
    async def test_ensure_consumer_group_creates(self, mock_redis: AsyncMock) -> None:
        """Test ensure_consumer_group creates if not exists."""
        publisher = EventPublisher(mock_redis)

        created = await publisher.ensure_consumer_group("new-group")

        assert created is True
        mock_redis.xgroup_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_consumer_group_exists(self, mock_redis: AsyncMock) -> None:
        """Test ensure_consumer_group returns False if exists."""
        mock_redis.xgroup_create.side_effect = ResponseError("BUSYGROUP")
        publisher = EventPublisher(mock_redis)

        created = await publisher.ensure_consumer_group("existing-group")

        assert created is False

    @pytest.mark.asyncio
    async def test_read_events(self, mock_redis: AsyncMock, sample_trade_event: TradeEvent) -> None:
        """Test reading events from stream."""
        serialized = _serialize_trade_event(sample_trade_event)
        mock_redis.xreadgroup = AsyncMock(
            return_value=[
                (
                    "trades",
                    [
                        ("1704369600000-0", serialized),
                    ],
                )
            ]
        )
        publisher = EventPublisher(mock_redis)

        entries = await publisher.read_events("test-group", "worker-1")

        assert len(entries) == 1
        assert entries[0].entry_id == "1704369600000-0"
        assert entries[0].event.market_id == sample_trade_event.market_id
        mock_redis.xreadgroup.assert_called_once_with(
            "test-group",
            "worker-1",
            {DEFAULT_STREAM_NAME: ">"},
            count=DEFAULT_COUNT,
            block=DEFAULT_BLOCK_MS,
        )

    @pytest.mark.asyncio
    async def test_read_events_empty(self, mock_redis: AsyncMock) -> None:
        """Test reading when no events available."""
        mock_redis.xreadgroup = AsyncMock(return_value=None)
        publisher = EventPublisher(mock_redis)

        entries = await publisher.read_events("test-group", "worker-1")

        assert entries == []

    @pytest.mark.asyncio
    async def test_read_events_with_bytes(
        self, mock_redis: AsyncMock, sample_trade_event: TradeEvent
    ) -> None:
        """Test reading events with bytes data (as from real Redis)."""
        serialized = _serialize_trade_event(sample_trade_event)
        bytes_data = {k.encode(): v.encode() for k, v in serialized.items()}
        mock_redis.xreadgroup = AsyncMock(
            return_value=[
                (
                    b"trades",
                    [
                        (b"1704369600000-0", bytes_data),
                    ],
                )
            ]
        )
        publisher = EventPublisher(mock_redis)

        entries = await publisher.read_events("test-group", "worker-1")

        assert len(entries) == 1
        assert entries[0].entry_id == "1704369600000-0"

    @pytest.mark.asyncio
    async def test_read_pending(
        self, mock_redis: AsyncMock, sample_trade_event: TradeEvent
    ) -> None:
        """Test reading pending events."""
        serialized = _serialize_trade_event(sample_trade_event)
        mock_redis.xreadgroup = AsyncMock(
            return_value=[
                (
                    "trades",
                    [
                        ("1704369600000-0", serialized),
                    ],
                )
            ]
        )
        publisher = EventPublisher(mock_redis)

        entries = await publisher.read_pending("test-group", "worker-1")

        assert len(entries) == 1
        # Should read from "0" not ">"
        call_args = mock_redis.xreadgroup.call_args
        assert call_args[0][2] == {DEFAULT_STREAM_NAME: "0"}

    @pytest.mark.asyncio
    async def test_read_pending_skips_empty_data(self, mock_redis: AsyncMock) -> None:
        """Test that read_pending skips entries with no data (already acked)."""
        mock_redis.xreadgroup = AsyncMock(
            return_value=[
                (
                    "trades",
                    [
                        ("1704369600000-0", {}),  # Empty = already acked
                        ("1704369600000-1", None),  # None = already acked
                    ],
                )
            ]
        )
        publisher = EventPublisher(mock_redis)

        entries = await publisher.read_pending("test-group", "worker-1")

        assert entries == []

    @pytest.mark.asyncio
    async def test_ack(self, mock_redis: AsyncMock) -> None:
        """Test acknowledging entries."""
        publisher = EventPublisher(mock_redis)

        count = await publisher.ack("test-group", "1704369600000-0", "1704369600000-1")

        assert count == 1  # Mocked return value
        mock_redis.xack.assert_called_once_with(
            DEFAULT_STREAM_NAME,
            "test-group",
            "1704369600000-0",
            "1704369600000-1",
        )

    @pytest.mark.asyncio
    async def test_ack_empty(self, mock_redis: AsyncMock) -> None:
        """Test ack with no entry IDs."""
        publisher = EventPublisher(mock_redis)

        count = await publisher.ack("test-group")

        assert count == 0
        mock_redis.xack.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_stream_info(self, mock_redis: AsyncMock) -> None:
        """Test getting stream info."""
        publisher = EventPublisher(mock_redis)

        info = await publisher.get_stream_info()

        assert info["length"] == 100
        mock_redis.xinfo_stream.assert_called_once_with(DEFAULT_STREAM_NAME)

    @pytest.mark.asyncio
    async def test_get_stream_info_not_exists(self, mock_redis: AsyncMock) -> None:
        """Test getting stream info when stream doesn't exist."""
        mock_redis.xinfo_stream.side_effect = ResponseError("ERR no such key")
        publisher = EventPublisher(mock_redis)

        info = await publisher.get_stream_info()

        assert info == {}

    @pytest.mark.asyncio
    async def test_get_stream_length(self, mock_redis: AsyncMock) -> None:
        """Test getting stream length."""
        publisher = EventPublisher(mock_redis)

        length = await publisher.get_stream_length()

        assert length == 100
        mock_redis.xlen.assert_called_once_with(DEFAULT_STREAM_NAME)

    @pytest.mark.asyncio
    async def test_trim_stream(self, mock_redis: AsyncMock) -> None:
        """Test trimming stream."""
        publisher = EventPublisher(mock_redis)

        await publisher.trim_stream(50_000)

        mock_redis.xtrim.assert_called_once_with(DEFAULT_STREAM_NAME, maxlen=50_000)

    @pytest.mark.asyncio
    async def test_trim_stream_default(self, mock_redis: AsyncMock) -> None:
        """Test trimming stream with default max_len."""
        publisher = EventPublisher(mock_redis)

        await publisher.trim_stream()

        mock_redis.xtrim.assert_called_once_with(DEFAULT_STREAM_NAME, maxlen=DEFAULT_MAX_LEN)


class TestStreamEntry:
    """Tests for the StreamEntry dataclass."""

    def test_stream_entry(self, sample_trade_event: TradeEvent) -> None:
        """Test creating a StreamEntry."""
        entry = StreamEntry(entry_id="1704369600000-0", event=sample_trade_event)

        assert entry.entry_id == "1704369600000-0"
        assert entry.event == sample_trade_event
