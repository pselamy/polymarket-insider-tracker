"""Edge behaviour of the ingestor models: lenient timestamp and date parsing."""

from datetime import UTC, datetime, timedelta

from polymarket_insider_tracker.ingestor.models import (
    MarketMetadata,
    TradeEvent,
)


def test_a_non_integer_websocket_timestamp_falls_back_to_receipt_time() -> None:
    before = datetime.now(UTC)
    trade = TradeEvent.from_websocket_message({"timestamp": "2024-01-01T00:00:00Z"})
    after = datetime.now(UTC)
    assert before <= trade.timestamp <= after


def test_an_integer_websocket_timestamp_is_read_as_unix_seconds() -> None:
    trade = TradeEvent.from_websocket_message({"timestamp": 1_700_000_000})
    assert trade.timestamp == datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)


def test_a_malformed_cached_date_degrades_to_none_and_receipt_time() -> None:
    before = datetime.now(UTC)
    market = MarketMetadata.from_dict(
        {"condition_id": "0xabc", "end_date": "not-a-date", "last_updated": "also-not-a-date"}
    )
    assert market.end_date is None
    assert before <= market.last_updated <= datetime.now(UTC) + timedelta(seconds=1)
