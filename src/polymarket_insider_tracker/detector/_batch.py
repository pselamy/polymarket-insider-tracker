"""Shared per-trade batch gathering for detector signal pipelines.

``FreshWalletDetector`` and ``SizeAnomalyDetector`` each run one analysis per
trade concurrently and keep only the signals that come back. That gather, zip,
and failure-dropping knowledge has this single home so the two detectors cannot
drift apart on how a failed or non-signal trade is handled.
"""

import asyncio
import logging
from collections.abc import Awaitable, Sequence
from typing import TypeVar

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.redaction import redact_exception_message

SignalT = TypeVar("SignalT")


async def gather_signals(
    trades: Sequence[TradeEvent],
    tasks: Sequence[Awaitable[SignalT | None]],
    logger: logging.Logger,
) -> list[SignalT]:
    """Await one analysis per trade and collect the non-``None`` signals.

    Results are matched to ``trades`` positionally. A per-trade failure is
    logged with its message redacted and dropped, so a single bad trade neither
    aborts the batch nor disappears without a trace. ``logger`` is the caller's
    module logger so log provenance stays with the detector that ran the batch.
    """
    results = await asyncio.gather(*tasks, return_exceptions=True)

    signals: list[SignalT] = []
    for trade, result in zip(trades, results, strict=True):
        if isinstance(result, BaseException):
            logger.warning(
                "Failed to analyze trade %s: %s",
                trade.trade_id,
                redact_exception_message(result),
            )
        elif result is not None:
            signals.append(result)

    return signals
