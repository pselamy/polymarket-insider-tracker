"""Direct tests for decoding documented Data API v2 trade rows into the existing parser contract.

Field names and the row shape are cited from the official Data API v2 documentation:
https://docs.polymarket.com/migrate/data-api-v1-to-v2 (v1 camelCase -> v2 snake_case)
https://docs.polymarket.com/api-reference/feeds/list-trades (the ``Trade`` schema: required
``proxy_wallet``, ``side``, ``token_id``, ``condition_id``, ``size``, ``price``, ``timestamp``,
``title``, ``slug``, ``event_slug``, ``outcome``, ``outcome_index``, ``transaction_hash`` and the
optional profile fields). Request ``taker_only=true`` serves each fill once on its taker side;
``taker_only=false`` also includes maker rows; both use the same row shape.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

import pytest

from polymarket_insider_tracker.ingestor.trade_row_v2 import parse_v2_trade_row
from polymarket_insider_tracker.ingestor.trade_rows import (
    InvalidRow,
    OutcomeResolution,
    RowDisposition,
    TradeObservation,
    parse_trade_row,
    repair_outcome,
)
from tests.fakes.trades import (
    MALFORMED_NUMERIC_CASES,
    market_metadata,
    synthetic_wallet,
    trade_row,
)

NOW = 1_788_983_720

RENAMES: dict[str, str] = {
    "proxyWallet": "proxy_wallet",
    "conditionId": "condition_id",
    "asset": "token_id",
    "transactionHash": "transaction_hash",
    "outcomeIndex": "outcome_index",
    "eventSlug": "event_slug",
    "profileImage": "profile_image",
    "profileImageOptimized": "profile_image_optimized",
}


def _as_v2(row: dict[str, object]) -> dict[str, object]:
    """Rename the documented v1 keys to their documented v2 spelling."""
    return {RENAMES.get(key, key): value for key, value in row.items()}


def _with_int_key(row: Mapping[str, object]) -> dict[object, object]:
    """Widen a decoded row to carry one non-string key the provider contract never produces."""
    widened: dict[object, object] = {}
    widened.update(row)
    widened[7] = "non-string key"
    return widened


def _observation(raw: object) -> TradeObservation:
    parsed = parse_v2_trade_row(raw, now=NOW)
    assert isinstance(parsed, TradeObservation)
    return parsed


class TestV2Decoding:
    def test_valid_v2_row_matches_v1_event_and_composite_identity(self) -> None:
        v1 = trade_row(timestamp=NOW - 5, transaction=7, wallet=3, price="0.65", size="5000")
        v1.update({"name": "Trader", "pseudonym": "Pseudo"})

        decoded = _observation(_as_v2(v1))
        expected = parse_trade_row(v1, now=NOW)

        assert isinstance(expected, TradeObservation)
        assert decoded.event == expected.event
        assert decoded.identity == expected.identity
        assert decoded.provider_timestamp == expected.provider_timestamp
        assert decoded.outcome_resolution is expected.outcome_resolution

    @pytest.mark.parametrize("taker_only", [True, False])
    def test_both_coverage_modes_share_the_documented_row_shape(self, taker_only: bool) -> None:
        v1 = trade_row(timestamp=NOW, transaction=1, wallet=1, side="BUY" if taker_only else "SELL")
        v2 = _as_v2(v1)
        v2["taker_only"] = taker_only

        decoded = _observation(v2)

        assert decoded.event.trade_id == v1["transactionHash"]
        assert decoded.event.wallet_address == synthetic_wallet(1)
        assert decoded.event.side == ("BUY" if taker_only else "SELL")

    def test_maker_and_taker_rows_keep_distinct_composite_identities(self) -> None:
        taker = _observation(_as_v2(trade_row(timestamp=NOW, transaction=1, wallet=1, side="BUY")))
        maker = _observation(_as_v2(trade_row(timestamp=NOW, transaction=1, wallet=2, side="SELL")))

        assert taker.event.trade_id == maker.event.trade_id
        assert taker.identity != maker.identity

    def test_non_object_row_is_the_same_invalid_schema_disposition(self) -> None:
        parsed = parse_v2_trade_row(["not", "an", "object"], now=NOW)

        assert isinstance(parsed, InvalidRow)
        assert parsed.disposition is RowDisposition.INVALID_SCHEMA
        assert parsed.field == "row"


class TestV2RequiredFields:
    @pytest.mark.parametrize(
        ("v2_field", "v1_field"),
        [
            ("transaction_hash", "transactionHash"),
            ("proxy_wallet", "proxyWallet"),
            ("condition_id", "conditionId"),
            ("token_id", "asset"),
            ("side", "side"),
            ("price", "price"),
            ("size", "size"),
            ("timestamp", "timestamp"),
        ],
    )
    def test_missing_required_field_is_rejected_per_field(
        self, v2_field: str, v1_field: str
    ) -> None:
        row = _as_v2(trade_row(timestamp=NOW))
        del row[v2_field]

        parsed = parse_v2_trade_row(row, now=NOW)

        assert isinstance(parsed, InvalidRow)
        assert parsed.disposition is RowDisposition(f"invalid:{v1_field}")
        assert parsed.field == v1_field

    @pytest.mark.parametrize(
        ("v2_field", "value"),
        MALFORMED_NUMERIC_CASES,
    )
    def test_malformed_numeric_field_is_rejected_per_field(
        self, v2_field: str, value: object
    ) -> None:
        row = _as_v2(trade_row(timestamp=NOW))
        row[v2_field] = value

        parsed = parse_v2_trade_row(row, now=NOW)

        assert isinstance(parsed, InvalidRow)
        assert parsed.disposition is RowDisposition(f"invalid:{v2_field}")
        assert parsed.field == v2_field


class TestV2OptionalAndUnknownFields:
    def test_unknown_extra_fields_are_ignored_like_the_v1_parser(self) -> None:
        v1 = trade_row(timestamp=NOW)
        v2 = _as_v2(v1)
        v2.update({"future_field": {"nested": [1, 2, 3]}, "another_note": "ignored"})

        decoded = _observation(v2)
        expected = parse_trade_row(dict(v1, future_field="x"), now=NOW)

        assert isinstance(expected, TradeObservation)
        assert decoded.event == expected.event
        assert decoded.identity == expected.identity

    def test_non_string_key_is_carried_through_exactly_like_the_v1_parser(self) -> None:
        v1 = trade_row(timestamp=NOW)
        v2 = _with_int_key(_as_v2(v1))

        decoded = parse_v2_trade_row(v2, now=NOW)
        expected = parse_trade_row(_with_int_key(v1), now=NOW)

        assert isinstance(decoded, TradeObservation)
        assert decoded == expected

    def test_missing_outcome_is_unknown_and_repairable_without_changing_identity(self) -> None:
        row = _as_v2(trade_row(timestamp=NOW, outcome=None, outcome_index=None))

        decoded = _observation(row)

        assert decoded.outcome_resolution is OutcomeResolution.UNKNOWN
        assert decoded.event.outcome == ""
        assert decoded.event.outcome_index == 0

        repaired = repair_outcome(
            decoded, market_metadata(("asset-no", "No"), ("asset-yes", "Yes"))
        )

        assert repaired.outcome_resolution is OutcomeResolution.REPAIRED
        assert repaired.event.outcome == "Yes"
        assert repaired.identity == decoded.identity

    def test_outcome_repair_matches_by_documented_token_id(self) -> None:
        row = _as_v2(trade_row(timestamp=NOW, outcome=None, outcome_index=None, asset="token-yes"))

        repaired = repair_outcome(
            _observation(row), market_metadata(("asset-no", "No"), ("token-yes", "Yes"))
        )

        assert repaired.event.outcome == "Yes"
        assert repaired.event.outcome_index == 1
        assert repaired.event.asset_id == "token-yes"

    def test_provided_outcome_is_preserved(self) -> None:
        decoded = _observation(_as_v2(trade_row(timestamp=NOW, outcome="No", outcome_index=1)))

        assert decoded.outcome_resolution is OutcomeResolution.PROVIDED
        assert decoded.event.outcome == "No"
        assert decoded.event.outcome_index == 1

    def test_out_of_range_outcome_index_is_treated_as_unknown(self) -> None:
        decoded = _observation(_as_v2(trade_row(timestamp=NOW, outcome_index=999)))

        assert decoded.outcome_resolution is OutcomeResolution.UNKNOWN
        assert decoded.event.outcome_index == 0

    def test_numeric_values_keep_decimal_identity_canonicalization(self) -> None:
        v1 = trade_row(timestamp=NOW, price="0.50", size="10.0")

        decoded = _observation(_as_v2(v1))

        assert decoded.event.price == Decimal("0.5")
        assert decoded.event.size == Decimal("10")
        assert (
            decoded.identity
            == _observation(_as_v2(trade_row(timestamp=NOW, price=0.5, size=10))).identity
        )
