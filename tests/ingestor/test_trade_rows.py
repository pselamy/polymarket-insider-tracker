"""Strict provider row parsing, composite identity, dispositions, and outcome repair."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token
from polymarket_insider_tracker.ingestor.trade_rows import (
    CLOCK_SKEW_ALLOWANCE_SECONDS,
    IDENTITY_FIELDS,
    InvalidRow,
    OutcomeResolution,
    RowDisposition,
    TradeObservation,
    canonical_decimal,
    classify_observation,
    parse_trade_row,
    repair_outcome,
    zero_disposition_counts,
)
from tests.fakes import synthetic_wallet, trade_row

NOW = 1_788_983_720


def _parse(row: object, *, now: int = NOW) -> TradeObservation | InvalidRow:
    return parse_trade_row(row, now=now)


def _observation(row: dict[str, object]) -> TradeObservation:
    parsed = _parse(row)
    assert isinstance(parsed, TradeObservation)
    return parsed


def _market(*tokens: tuple[str, str]) -> MarketMetadata:
    return MarketMetadata(
        condition_id="0xmarket",
        question="Synthetic market?",
        description="",
        tokens=tuple(Token(token_id=token_id, outcome=outcome) for token_id, outcome in tokens),
    )


class TestStrictParsing:
    def test_valid_row_preserves_every_field(self) -> None:
        row = trade_row(timestamp=NOW - 5, transaction=7, wallet=3, price="0.65", size="5000")
        row.update({"name": "Trader", "pseudonym": "Pseudo"})

        observation = _observation(row)
        event = observation.event

        assert event.trade_id == row["transactionHash"]
        assert event.wallet_address == synthetic_wallet(3)
        assert event.market_id == "0xmarket"
        assert event.asset_id == "asset-yes"
        assert event.side == "BUY"
        assert event.price == Decimal("0.65")
        assert event.size == Decimal("5000")
        assert event.timestamp == datetime.fromtimestamp(NOW - 5, tz=UTC)
        assert event.outcome == "Yes"
        assert event.outcome_index == 0
        assert (event.market_slug, event.event_slug, event.event_title) == (
            "synthetic-market",
            "synthetic-event",
            "Synthetic market?",
        )
        assert (event.trader_name, event.trader_pseudonym) == ("Trader", "Pseudo")
        assert observation.provider_timestamp == NOW - 5
        assert observation.outcome_resolution is OutcomeResolution.PROVIDED
        assert observation.outcome_known is True
        assert observation.repaired is False
        assert len(observation.identity) == 64
        assert int(observation.identity, 16) >= 0

    def test_identity_fields_are_exactly_the_documented_ones(self) -> None:
        assert IDENTITY_FIELDS == (
            "transactionHash",
            "proxyWallet",
            "conditionId",
            "asset",
            "side",
            "price",
            "size",
            "timestamp",
        )

    @pytest.mark.parametrize("missing", IDENTITY_FIELDS)
    def test_missing_identity_field_is_invalid_per_field(self, missing: str) -> None:
        row = trade_row(timestamp=NOW)
        del row[missing]

        parsed = _parse(row)

        assert parsed == InvalidRow(
            disposition=RowDisposition(f"invalid:{missing}"),
            field=missing,
            row_hash=hashlib.sha256(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        )

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("transactionHash", ""),
            ("proxyWallet", 12),
            ("conditionId", None),
            ("asset", ""),
            ("side", "HOLD"),
            ("price", "not-a-number"),
            ("price", "NaN"),
            ("price", "Infinity"),
            ("size", True),
            ("size", None),
            ("timestamp", "soon"),
            ("timestamp", 12.5),
            ("timestamp", False),
        ],
    )
    def test_malformed_identity_field_is_invalid(self, field: str, value: object) -> None:
        row = trade_row(timestamp=NOW)
        row[field] = value

        parsed = _parse(row)

        assert isinstance(parsed, InvalidRow)
        assert parsed.disposition is RowDisposition(f"invalid:{field}")
        assert parsed.field == field

    def test_side_is_case_normalized_but_never_defaulted(self) -> None:
        row = trade_row(timestamp=NOW, side="sell")

        assert _observation(row).event.side == "SELL"

    def test_digit_string_timestamp_is_accepted(self) -> None:
        row = trade_row(timestamp=NOW)
        row["timestamp"] = str(NOW - 1)

        assert _observation(row).provider_timestamp == NOW - 1

    def test_non_object_row_is_invalid_schema(self) -> None:
        parsed = _parse(["not", "an", "object"])

        assert isinstance(parsed, InvalidRow)
        assert parsed.disposition is RowDisposition.INVALID_SCHEMA
        assert parsed.field == "row"

    def test_future_timestamp_beyond_skew_allowance_is_invalid(self) -> None:
        allowed = _parse(trade_row(timestamp=NOW + CLOCK_SKEW_ALLOWANCE_SECONDS))
        rejected = _parse(trade_row(timestamp=NOW + CLOCK_SKEW_ALLOWANCE_SECONDS + 1))

        assert isinstance(allowed, TradeObservation)
        assert isinstance(rejected, InvalidRow)
        assert rejected.disposition is RowDisposition.INVALID_FUTURE_TIMESTAMP
        assert rejected.field == "timestamp"

    def test_invalid_row_diagnostic_is_wallet_free(self) -> None:
        row = trade_row(timestamp=NOW, wallet=9)
        row["name"] = "Real Name"
        del row["price"]

        parsed = _parse(row)

        assert isinstance(parsed, InvalidRow)
        rendered = json.dumps(dataclasses.asdict(parsed))
        assert synthetic_wallet(9) not in rendered
        assert "Real Name" not in rendered
        assert set(dataclasses.asdict(parsed)) == {"disposition", "field", "row_hash"}


class TestCompositeIdentity:
    def test_decimal_canonicalization_unifies_equivalent_spellings(self) -> None:
        assert canonical_decimal(Decimal("0.50")) == "0.5"
        assert canonical_decimal(Decimal("100")) == "100"
        assert canonical_decimal(Decimal("1E+2")) == "100"
        assert canonical_decimal(Decimal("10.0")) == "10"

    def test_equivalent_decimal_spellings_share_an_identity(self) -> None:
        first = _observation(trade_row(timestamp=NOW, price="0.50", size="10.0"))
        second = _observation(trade_row(timestamp=NOW, price=0.5, size=10))

        assert first.identity == second.identity

    def test_wallet_case_is_ignored_in_identity_but_preserved_in_event(self) -> None:
        upper = trade_row(timestamp=NOW)
        upper["proxyWallet"] = "0x" + "AB" * 20
        lower = trade_row(timestamp=NOW)
        lower["proxyWallet"] = "0x" + "ab" * 20

        assert _observation(upper).identity == _observation(lower).identity
        assert _observation(upper).event.wallet_address == "0x" + "AB" * 20

    def test_maker_and_taker_rows_sharing_a_transaction_are_distinct(self) -> None:
        taker = _observation(trade_row(timestamp=NOW, transaction=1, wallet=1, side="BUY"))
        maker = _observation(trade_row(timestamp=NOW, transaction=1, wallet=2, side="SELL"))

        assert taker.event.trade_id == maker.event.trade_id
        assert taker.identity != maker.identity

    def test_exact_repeat_has_an_equal_identity(self) -> None:
        row = trade_row(timestamp=NOW, transaction=4, wallet=4)

        assert _observation(row).identity == _observation(dict(row)).identity

    @pytest.mark.parametrize(
        "change",
        [
            {"timestamp": NOW + 1},
            {"price": "0.51"},
            {"size": "11"},
            {"asset": "asset-no"},
            {"conditionId": "0xother"},
            {"side": "SELL"},
            {"outcomeIndex": 1},
        ],
    )
    def test_any_identity_tuple_difference_is_a_distinct_observation(
        self, change: dict[str, object]
    ) -> None:
        base = trade_row(timestamp=NOW)
        changed = dict(base, **change)

        assert _observation(base).identity != _observation(changed).identity

    def test_missing_outcome_index_is_empty_in_identity(self) -> None:
        with_index = _observation(trade_row(timestamp=NOW, outcome_index=0))
        without = _observation(trade_row(timestamp=NOW, outcome=None, outcome_index=None))

        assert with_index.identity != without.identity


class TestOutcomeRepair:
    def test_missing_outcome_is_repaired_from_the_token_matching_asset(self) -> None:
        observation = _observation(trade_row(timestamp=NOW, outcome=None, outcome_index=None))
        assert observation.outcome_resolution is OutcomeResolution.UNKNOWN

        repaired = repair_outcome(observation, _market(("asset-no", "No"), ("asset-yes", "Yes")))

        assert repaired.event.outcome == "Yes"
        assert repaired.event.outcome_index == 1
        assert repaired.outcome_resolution is OutcomeResolution.REPAIRED
        assert repaired.repaired is True
        assert repaired.outcome_known is True
        assert repaired.identity == observation.identity
        assert repaired.event.price == observation.event.price

    def test_absent_metadata_leaves_the_outcome_unknown(self) -> None:
        observation = _observation(trade_row(timestamp=NOW, outcome=None, outcome_index=None))

        unrepaired = repair_outcome(observation, None)

        assert unrepaired.event.outcome == ""
        assert unrepaired.event.outcome_index == 0
        assert unrepaired.outcome_resolution is OutcomeResolution.UNKNOWN
        assert unrepaired.outcome_known is False
        assert unrepaired.repaired is False

    def test_metadata_without_a_matching_token_never_invents_an_outcome(self) -> None:
        observation = _observation(trade_row(timestamp=NOW, outcome=None, outcome_index=None))

        unrepaired = repair_outcome(observation, _market(("other-token", "Maybe")))

        assert unrepaired.outcome_resolution is OutcomeResolution.UNKNOWN
        assert unrepaired.event.outcome == ""

    def test_provided_outcome_is_never_overwritten(self) -> None:
        observation = _observation(trade_row(timestamp=NOW, outcome="Yes", outcome_index=0))

        repaired = repair_outcome(observation, _market(("asset-yes", "Different")))

        assert repaired is observation
        assert repaired.outcome_resolution is OutcomeResolution.PROVIDED

    def test_outcome_index_alone_marks_resolution_provided(self) -> None:
        observation = _observation(trade_row(timestamp=NOW, outcome=None, outcome_index=1))

        assert observation.outcome_resolution is OutcomeResolution.PROVIDED
        assert observation.event.outcome == ""
        assert observation.event.outcome_index == 1


class TestDispositions:
    def test_every_documented_disposition_exists_once(self) -> None:
        assert [disposition.value for disposition in RowDisposition] == [
            "emitted",
            "duplicate",
            "padding",
            "anchor-history",
            "deferred:future-cycle",
            "invalid:transactionHash",
            "invalid:proxyWallet",
            "invalid:conditionId",
            "invalid:asset",
            "invalid:side",
            "invalid:price",
            "invalid:size",
            "invalid:timestamp",
            "invalid:future-timestamp",
            "invalid:schema",
        ]
        assert zero_disposition_counts() == dict.fromkeys(RowDisposition, 0)
        assert [resolution.value for resolution in OutcomeResolution] == [
            "provided",
            "repaired",
            "unknown",
        ]

    def test_classification_is_mutually_exclusive_and_ordered(self) -> None:
        observation = _observation(trade_row(timestamp=NOW))
        retained = {observation.identity}

        deferred = classify_observation(
            observation, cutoff=NOW - 1, floor=NOW - 100, retained=retained
        )
        padding = classify_observation(observation, cutoff=NOW, floor=NOW + 1, retained=retained)
        duplicate = classify_observation(
            observation, cutoff=NOW, floor=NOW - 100, retained=retained
        )
        emitted = classify_observation(observation, cutoff=NOW, floor=NOW - 100, retained=set())

        assert deferred is RowDisposition.DEFERRED_FUTURE_CYCLE
        assert padding is RowDisposition.PADDING
        assert duplicate is RowDisposition.DUPLICATE
        assert emitted is RowDisposition.EMITTED

    def test_deferred_row_is_reacquired_once_the_cutoff_reaches_it(self) -> None:
        observation = _observation(trade_row(timestamp=NOW + 3))

        before = classify_observation(observation, cutoff=NOW, floor=NOW - 100, retained=set())
        after = classify_observation(observation, cutoff=NOW + 3, floor=NOW - 100, retained=set())

        assert before is RowDisposition.DEFERRED_FUTURE_CYCLE
        assert after is RowDisposition.EMITTED
