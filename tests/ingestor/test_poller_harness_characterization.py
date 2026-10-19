"""Characterization of the downstream trade-consumer double shared by the poller suites.

``RecordingTradeCallback`` is the knowledge #151 copied into ``test_v2_poller``.
This pins its behaviour at the pre-refactor home (``test_endpoint_switch_resume``)
before the two copies are given one home: deliveries are kept in order and mapped
to integer epoch seconds.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from tests.ingestor.test_endpoint_switch_resume import RecordingTradeCallback


@dataclass
class _StubTrade:
    """The only surface ``timestamps`` touches; avoids duplicating a TradeEvent literal."""

    timestamp: datetime


def _at(epoch: float) -> _StubTrade:
    return _StubTrade(timestamp=datetime.fromtimestamp(epoch, UTC))


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
