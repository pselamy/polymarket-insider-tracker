"""Characterization of the downstream trade-consumer double shared by the poller suites.

``RecordingTradeCallback`` is the knowledge #151 copied into ``test_v2_poller``.
This pins its behaviour at the pre-refactor home (``test_endpoint_switch_resume``)
before the two copies are given one home: deliveries are kept in order and mapped
to integer epoch seconds.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from polymarket_insider_tracker.ingestor.models import TradeEvent
from tests.ingestor._poller_harness import RecordingTradeCallback


def _at(epoch: float) -> TradeEvent:
    """A minimal real trade; only its timestamp matters to the callback under test."""
    return TradeEvent(
        market_id="market",
        trade_id="trade",
        wallet_address="0x" + "0" * 40,
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.5"),
        size=Decimal("1"),
        timestamp=datetime.fromtimestamp(epoch, UTC),
        asset_id="asset",
    )


async def test_recording_trade_callback_keeps_deliveries_in_order() -> None:
    callback = RecordingTradeCallback()
    assert callback.trades == []
    assert callback.timestamps() == []

    await callback(_at(100.0))
    await callback(_at(200.0))

    assert [trade.timestamp for trade in callback.trades] == [
        datetime.fromtimestamp(100.0, UTC),
        datetime.fromtimestamp(200.0, UTC),
    ]
    assert callback.timestamps() == [100, 200]
