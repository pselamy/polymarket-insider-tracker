"""Seam tests for the extracted publisher codec.

These tests pin the observable contract of the pure codec helpers in
:mod:`polymarket_insider_tracker.ingestor.publisher` after their extraction
into :mod:`polymarket_insider_tracker.ingestor._publisher_codec`:

- both import paths resolve to the same callable with the historical
  ``publisher.<name>`` module identity (pickle/qualname compatibility),
- wire keys, order, defaults, bytes handling, timestamp fallback and error
  behavior are unchanged,
- ``EventPublisher`` still dispatches through the module-global seams, so
  existing monkeypatch-based controls keep working.
"""

import pickle
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.ingestor import _publisher_codec as codec
from polymarket_insider_tracker.ingestor import publisher
from polymarket_insider_tracker.ingestor._publisher_codec import (
    decode_entry_id as codec_decode_entry_id,
)
from polymarket_insider_tracker.ingestor._publisher_codec import (
    decode_stream_dict as codec_decode_stream_dict,
)
from polymarket_insider_tracker.ingestor._publisher_codec import (
    deserialize_trade_event as codec_deserialize_trade_event,
)
from polymarket_insider_tracker.ingestor._publisher_codec import (
    parse_event_timestamp as codec_parse_event_timestamp,
)
from polymarket_insider_tracker.ingestor._publisher_codec import (
    serialize_trade_event as codec_serialize_trade_event,
)
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.publisher import (
    EventPublisher,
    _decode_entry_id,
    _decode_stream_dict,
    _deserialize_trade_event,
    _parse_event_timestamp,
    _serialize_trade_event,
)


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


EXPECTED_WIRE_KEYS = [
    "market_id",
    "trade_id",
    "wallet_address",
    "side",
    "outcome",
    "outcome_index",
    "price",
    "size",
    "timestamp",
    "asset_id",
    "market_slug",
    "event_slug",
    "event_title",
    "trader_name",
    "trader_pseudonym",
]

PRIVATE_NAMES = (
    "_serialize_trade_event",
    "_decode_stream_dict",
    "_parse_event_timestamp",
    "_decode_entry_id",
    "_deserialize_trade_event",
)


class TestCodecIdentity:
    """Both import paths resolve to the same callable with history intact."""

    def test_both_paths_share_callable(self) -> None:
        pairs = (
            ("_serialize_trade_event", codec_serialize_trade_event),
            ("_decode_stream_dict", codec_decode_stream_dict),
            ("_parse_event_timestamp", codec_parse_event_timestamp),
            ("_decode_entry_id", codec_decode_entry_id),
            ("_deserialize_trade_event", codec_deserialize_trade_event),
        )
        for private_name, public_func in pairs:
            assert getattr(publisher, private_name) is public_func
            assert getattr(codec, public_func.__name__) is public_func

    def test_historical_module_identity(self) -> None:
        for name in PRIVATE_NAMES:
            func = getattr(publisher, name)
            assert func.__module__ == publisher.__name__
            assert func.__qualname__ == name

    def test_pickle_round_trip_by_reference(self) -> None:
        func = pickle.loads(pickle.dumps(_serialize_trade_event))
        assert func is _serialize_trade_event

    def test_signatures_preserved(self) -> None:
        import inspect

        serialize_sig = inspect.signature(_serialize_trade_event)
        assert list(serialize_sig.parameters) == ["event"]
        assert serialize_sig.parameters["event"].annotation is TradeEvent
        assert serialize_sig.return_annotation == dict[str, str]
        decode_sig = inspect.signature(_decode_entry_id)
        assert list(decode_sig.parameters) == ["entry_id"]
        assert decode_sig.return_annotation is str


class TestWireContract:
    """Wire keys, order, defaults, bytes and timestamp behavior are unchanged."""

    def test_serialize_wire_keys_and_order(self, sample_trade_event: TradeEvent) -> None:
        data = _serialize_trade_event(sample_trade_event)
        assert list(data.keys()) == EXPECTED_WIRE_KEYS
        assert all(isinstance(v, str) for v in data.values())
        assert data["price"] == "0.65"
        assert data["timestamp"] == "2026-01-04T12:00:00+00:00"

    def test_bytes_round_trip(self, sample_trade_event: TradeEvent) -> None:
        wire = _serialize_trade_event(sample_trade_event)
        redis_form = {k.encode(): v.encode() for k, v in wire.items()}
        assert _deserialize_trade_event(redis_form) == sample_trade_event

    def test_mixed_str_bytes_decode(self) -> None:
        assert _decode_stream_dict({b"a": "1", "b": b"2"}) == {"a": "1", "b": "2"}

    def test_deserialize_defaults(self) -> None:
        event = _deserialize_trade_event({})
        assert event.side == "BUY"
        assert event.outcome_index == 0
        assert event.price == Decimal("0")
        assert event.size == Decimal("0")
        assert event.market_id == ""

    def test_side_normalization(self) -> None:
        assert _deserialize_trade_event({"side": "sell"}).side == "SELL"
        assert _deserialize_trade_event({"side": "BUY"}).side == "BUY"
        assert _deserialize_trade_event({"side": "maybe"}).side == "SELL"

    def test_invalid_timestamp_falls_back_to_now(self) -> None:
        before = datetime.now(UTC)
        parsed = _parse_event_timestamp("not-a-timestamp")
        after = datetime.now(UTC)
        assert before <= parsed <= after

    def test_valid_timestamp_parsed(self) -> None:
        parsed = _parse_event_timestamp("2026-01-04T12:00:00+00:00")
        assert parsed == datetime(2026, 1, 4, 12, 0, 0, tzinfo=UTC)

    def test_decode_entry_id(self) -> None:
        assert _decode_entry_id(b"1-0") == "1-0"
        assert _decode_entry_id("1-0") == "1-0"


class TestPublisherSeam:
    """EventPublisher still dispatches through the module-global codec seam."""

    async def test_publish_uses_module_global_serializer(
        self, sample_trade_event: TradeEvent, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []
        original = publisher._serialize_trade_event

        def recording(event: TradeEvent) -> dict[str, str]:
            calls.append(event.trade_id)
            return original(event)

        monkeypatch.setattr(publisher, "_serialize_trade_event", recording)
        client = FakeAsyncRedis()
        entry_id = await EventPublisher(client).publish(sample_trade_event)
        assert calls == [sample_trade_event.trade_id]
        assert isinstance(entry_id, str)
        await client.aclose()

    async def test_publish_read_round_trip_through_codec(
        self, sample_trade_event: TradeEvent
    ) -> None:
        client = FakeAsyncRedis()
        pub = EventPublisher(client)
        await pub.create_consumer_group("seam-group")
        await pub.publish(sample_trade_event)
        entries = await pub.read_events("seam-group", "seam-worker")
        assert len(entries) == 1
        assert entries[0].event == sample_trade_event
        await client.aclose()
