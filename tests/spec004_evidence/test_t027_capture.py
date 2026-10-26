"""T027 Part B capture records: identity contract and line shapes (no network, no Redis)."""

from __future__ import annotations

import io
import json

import pytest
from scripts.spec004_evidence import t027_capture as tc

from polymarket_insider_tracker.ingestor.trade_row_v2 import parse_v2_trade_row
from polymarket_insider_tracker.ingestor.trade_rows import TradeObservation, parse_trade_row
from tests.fakes.trades import FakeClock, trade_row
from tests.fakes.v2_walk import v2_row

ROWS = [
    trade_row(timestamp=1_700_000_000, transaction=1, wallet=3, price="0.50", size="2000"),
    trade_row(timestamp=1_700_000_001, transaction=2, wallet=4, price=0.987, size=150.0),
    trade_row(timestamp=1_700_000_002, transaction=3, wallet=5, side="SELL", outcome=None),
]


def observation(raw: object) -> TradeObservation:
    parsed = parse_trade_row(raw, now=1_800_000_000)
    assert isinstance(parsed, TradeObservation)
    return parsed


@pytest.mark.parametrize("raw", ROWS)
def test_the_event_identity_equals_the_ingestion_observation_identity(raw: object) -> None:
    parsed = observation(raw)
    assert tc.event_identity(parsed.event) == parsed.identity


@pytest.mark.parametrize("raw", ROWS)
def test_a_v2_row_has_the_same_identity_as_its_v1_spelling(raw: dict[str, object]) -> None:
    v2 = parse_v2_trade_row(v2_row(raw), now=1_800_000_000)
    assert isinstance(v2, TradeObservation)
    assert v2.identity == observation(raw).identity == tc.event_identity(v2.event)


def test_a_trade_record_carries_identity_lowercased_wallet_and_notional() -> None:
    parsed = observation(ROWS[0])
    record = tc.trade_record(parsed.event, 1_700_000_005.1234)
    assert record == {
        "identity": parsed.identity,
        "wallet": parsed.event.wallet_address.lower(),
        "notional": "1000",
        "ts": 1_700_000_000,
        "captured_at": 1_700_000_005.123,
    }


async def test_the_recorder_appends_one_json_line_per_delivered_event() -> None:
    out = io.StringIO()
    recorder = tc.make_recorder(out, FakeClock(5.0))
    for raw in ROWS[:2]:
        await recorder(observation(raw).event)
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    assert [line["ts"] for line in lines] == [1_700_000_000, 1_700_000_001]
    assert all(line["captured_at"] == 5.0 for line in lines)


def test_the_capture_runs_the_documented_supported_ingestion_settings() -> None:
    assert tc.CAPTURE_SETTINGS == {
        "POLYMARKET_TRADES_URL": "https://data-api.polymarket.com/v2/trades",
        "POLYMARKET_TRADES_COVERAGE": "all",
        "POLYMARKET_TRADES_POLL_INTERVAL_SECONDS": 5,
        "POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS": 600,
    }
