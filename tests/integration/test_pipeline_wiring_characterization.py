"""Characterization of the pipeline-wiring test data the integration and unit suites share.

The sample trade, the single-token market built from it, the two-row seeded provider history
and the wallet-bearing provider row are the same facts in every suite that wires a pipeline
(``test_end_to_end``, ``test_pipeline``, ``test_pipeline_persistence``, ``test_persist_assessment``
and ``test_postgres_journey``). These tests pin those facts so any single home must reproduce
them exactly.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal

import tests.integration.test_end_to_end as end_to_end
import tests.integration.test_postgres_journey as postgres_journey
import tests.test_persist_assessment as persist_assessment
import tests.test_pipeline_persistence as pipeline_persistence
import tests.test_pipeline as unit_pipeline
from tests.fakes import FakeClock, trade_row

from polymarket_insider_tracker.ingestor.models import TradeEvent

POLL_START = 1_788_983_720.0


def _sample_fields(trade: TradeEvent) -> dict[str, object]:
    fields = asdict(trade)
    assert abs(fields.pop("timestamp") - datetime.now(UTC)).total_seconds() < 60
    return fields


def test_sample_trade_event_values_are_pinned() -> None:
    expected = {
        "trade_id": "0x" + "a" * 64,
        "wallet_address": "0x" + "b" * 40,
        "market_id": "0x" + "c" * 64,
        "asset_id": "asset_123",
        "side": "BUY",
        "price": Decimal("0.65"),
        "size": Decimal("5000"),
        "outcome": "Yes",
        "outcome_index": 0,
        "event_title": "Test Market",
        "market_slug": "test-market",
        "event_slug": "",
        "trader_name": "",
        "trader_pseudonym": "",
    }
    assert _sample_fields(end_to_end.sample_trade_event.__wrapped__()) == expected
    assert _sample_fields(unit_pipeline.sample_trade_event.__wrapped__()) == expected
    assert _sample_fields(postgres_journey._sample_trade()) == expected
    assert _sample_fields(pipeline_persistence.sample_trade.__wrapped__()) == expected


def test_persist_assessment_trade_is_a_different_trade() -> None:
    """Its asset, price, size and title differ on purpose; the other fields match."""
    fields = _sample_fields(persist_assessment.sample_trade.__wrapped__())
    assert (fields["asset_id"], fields["price"], fields["size"], fields["event_title"]) == (
        "asset_xyz",
        Decimal("0.42"),
        Decimal("1000"),
        "Test Event",
    )


def test_market_for_trade_values_are_pinned() -> None:
    trade = end_to_end.sample_trade_event.__wrapped__()
    for build in (end_to_end._market, unit_pipeline._market):
        market = build(trade, "science")
        assert market.condition_id == trade.market_id
        assert market.question == "Test market?"
        assert market.description == ""
        assert market.category == "science"
        (token,) = market.tokens
        assert (token.token_id, token.outcome, token.price) == ("asset_123", "Yes", Decimal("0.65"))


def test_seeded_server_history_is_pinned() -> None:
    expected = [
        trade_row(timestamp=int(POLL_START) - 1, transaction=1, wallet=1),
        trade_row(timestamp=int(POLL_START) - 61, transaction=2, wallet=2),
    ]
    for build in (end_to_end._seeded_server, unit_pipeline._seeded_server):
        server = build(FakeClock(POLL_START))
        assert [entry.row for entry in server._ledger] == expected


def test_provider_row_for_the_sample_trade_is_pinned() -> None:
    trade = end_to_end.sample_trade_event.__wrapped__()
    clock = FakeClock(POLL_START)
    expected = trade_row(
        timestamp=int(POLL_START) + 1,
        transaction=9,
        market=trade.market_id,
        asset=trade.asset_id,
        price="0.65",
        size="5000",
    )
    expected["proxyWallet"] = trade.wallet_address
    assert end_to_end._pipeline_row(trade, clock) == expected
    assert unit_pipeline._pipeline_row(trade, clock) == expected
