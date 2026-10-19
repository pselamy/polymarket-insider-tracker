"""Property tests using the existing v1 parser as the oracle for the v2 decoder.

The repository had no Hypothesis dependency or profile, so this module registers a deterministic
``repo`` profile (derandomized, bounded examples, no wall-clock deadline) and exercises the approved
contract: renaming a valid v1 row to documented v2 field names must decode to the same
``TradeObservation`` and composite identity, and invalid rows must reject with the same class.
"""

from __future__ import annotations

from decimal import Decimal

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from polymarket_insider_tracker.ingestor.trade_row_v2 import parse_v2_trade_row
from polymarket_insider_tracker.ingestor.trade_rows import (
    IDENTITY_FIELDS,
    InvalidRow,
    TradeObservation,
    parse_trade_row,
)
from tests.fakes import trade_row

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

settings.register_profile(
    "repo",
    derandomize=True,
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("repo")

_PRICES = st.decimals(min_value=Decimal("0.01"), max_value=Decimal("0.99"), places=2)
_SIZES = st.decimals(min_value=Decimal("0.01"), max_value=Decimal("100000"), places=2)
_SIDES = st.sampled_from(["BUY", "SELL", "buy", "sell"])
_ASSETS = st.sampled_from(["asset-yes", "asset-no", "token-123"])
_MARKETS = st.sampled_from(["0xmarket", "0xmarket-two", "0x" + "ab" * 32])
_OUTCOMES = st.sampled_from([None, "Yes", "No"])
_OUTCOME_INDEXES = st.sampled_from([None, 0, 1, 999])


def _as_v2(row: dict[str, object]) -> dict[str, object]:
    return {RENAMES.get(key, key): value for key, value in row.items()}


@st.composite
def _valid_rows(draw: st.DrawFn) -> dict[str, object]:
    return trade_row(
        timestamp=draw(st.integers(min_value=0, max_value=NOW)),
        transaction=draw(st.integers(min_value=0, max_value=10**12)),
        wallet=draw(st.integers(min_value=0, max_value=10**12)),
        market=draw(_MARKETS),
        asset=draw(_ASSETS),
        side=draw(_SIDES),
        price=str(draw(_PRICES)),
        size=str(draw(_SIZES)),
        outcome=draw(_OUTCOMES),
        outcome_index=draw(_OUTCOME_INDEXES),
    )


def _assert_observations_equal(v1_row: dict[str, object]) -> None:
    expected = parse_trade_row(v1_row, now=NOW)
    actual = parse_v2_trade_row(_as_v2(v1_row), now=NOW)
    assert isinstance(expected, TradeObservation)
    assert isinstance(actual, TradeObservation)
    assert actual.event == expected.event
    assert actual.identity == expected.identity
    assert actual.outcome_resolution is expected.outcome_resolution


def _assert_rejected_alike(v1_row: dict[str, object]) -> None:
    expected = parse_trade_row(v1_row, now=NOW)
    actual = parse_v2_trade_row(_as_v2(v1_row), now=NOW)
    assert isinstance(expected, InvalidRow)
    assert isinstance(actual, InvalidRow)
    assert actual.disposition is expected.disposition
    assert actual.field == expected.field


@given(_valid_rows())
def test_valid_v2_rows_match_the_v1_parser_oracle(row: dict[str, object]) -> None:
    _assert_observations_equal(row)


@given(_valid_rows(), st.sampled_from(IDENTITY_FIELDS))
def test_missing_field_rejects_with_the_same_class(row: dict[str, object], field: str) -> None:
    del row[field]

    _assert_rejected_alike(row)


@given(
    _valid_rows(),
    st.sampled_from(["price", "size", "timestamp"]),
    st.sampled_from(["not-a-number", "NaN", "Infinity", None, True]),
)
def test_malformed_numeric_rejects_with_the_same_class(
    row: dict[str, object], field: str, value: object
) -> None:
    row[field] = value

    _assert_rejected_alike(row)


@given(st.lists(st.integers(min_value=0, max_value=10**12), min_size=2, max_size=25, unique=True))
def test_identity_uniqueness_across_a_generated_batch(transactions: list[int]) -> None:
    identities = [
        _decode_identity(trade_row(timestamp=NOW, transaction=transaction))
        for transaction in transactions
    ]

    assert len(set(identities)) == len(transactions)


def _decode_identity(row: dict[str, object]) -> str:
    decoded = parse_v2_trade_row(_as_v2(row), now=NOW)
    assert isinstance(decoded, TradeObservation)
    return decoded.identity


@given(_valid_rows())
def test_exact_repeat_keeps_an_equal_identity(row: dict[str, object]) -> None:
    first = parse_v2_trade_row(_as_v2(row), now=NOW)
    second = parse_v2_trade_row(_as_v2(dict(row)), now=NOW)

    assert isinstance(first, TradeObservation)
    assert isinstance(second, TradeObservation)
    assert first.identity == second.identity
