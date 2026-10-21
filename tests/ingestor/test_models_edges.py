"""Edge behaviour of the ingestor models: lenient timestamp and date parsing."""

from datetime import UTC, datetime, timedelta

import pytest

from polymarket_insider_tracker.ingestor.models import (
    ClobOrderbook,
    ClobOrderbookLevel,
    MarketMetadata,
    TradeEvent,
    derive_category,
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


def test_a_cached_market_without_dates_has_no_end_date_and_is_stamped_on_receipt() -> None:
    before = datetime.now(UTC)
    market = MarketMetadata.from_dict({"condition_id": "0xabc"})
    assert market.end_date is None
    assert before <= market.last_updated <= datetime.now(UTC)


def test_a_title_matching_no_keyword_is_categorised_as_other() -> None:
    assert derive_category("zzz qqq xyzzy") == "other"


@pytest.mark.parametrize(
    ("protocol", "name"),
    [
        (ClobOrderbookLevel, "price"),
        (ClobOrderbookLevel, "size"),
        (ClobOrderbook, "market"),
        (ClobOrderbook, "asset_id"),
        (ClobOrderbook, "bids"),
        (ClobOrderbook, "asks"),
        (ClobOrderbook, "tick_size"),
    ],
)
def test_the_clob_protocol_surfaces_declare_no_default_values(protocol: type, name: str) -> None:
    declared = vars(protocol)[name]
    assert isinstance(declared, property)
    assert declared.fget is not None
    assert declared.fget(object()) is None
