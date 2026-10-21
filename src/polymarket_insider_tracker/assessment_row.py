"""Project a ``RiskAssessment`` onto its persisted row (``RiskAssessmentDTO``).

The ``scoring_config`` column is the version-dispatched document of
``specs/004-truthful-detection-contract/contracts/scoring-config.md``: a 004.1 assessment is
written as its 004.1 document, composed here from the captured assessment and evidence.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from decimal import Decimal
from functools import partial

from polymarket_insider_tracker.detector.evidence_document import row_scoring_config
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.detector.scorer import quantize_score_value
from polymarket_insider_tracker.detector.scoring_config import DetectorConfig
from polymarket_insider_tracker.redaction import redact_exception_message
from polymarket_insider_tracker.shutdown import DEFAULT_SHUTDOWN_TIMEOUT, wait_bounded
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.repos import RiskAssessmentDTO, RiskAssessmentRepository


def assessment_dto(
    assessment: RiskAssessment, *, alert_threshold: float, detector_config: DetectorConfig
) -> RiskAssessmentDTO:
    """The row of ``assessment``; ``detector_config`` describes an evidence-less 004.1 row."""
    trade = assessment.trade_event
    fresh = assessment.fresh_wallet_signal
    size_sig = assessment.size_anomaly_signal
    wallet_age: Decimal | None = None
    if fresh is not None and fresh.wallet_profile.age_hours is not None:
        wallet_age = Decimal(str(round(float(fresh.wallet_profile.age_hours), 2)))
    return RiskAssessmentDTO(
        assessment_id=assessment.assessment_id,
        trade_id=trade.trade_id,
        wallet_address=assessment.wallet_address.lower(),
        market_id=assessment.market_id,
        asset_id=getattr(trade, "asset_id", None) or None,
        side=trade.side,
        outcome=getattr(trade, "outcome", None) or None,
        outcome_index=getattr(trade, "outcome_index", None),
        price=trade.price,
        size=trade.size,
        notional_usdc=trade.notional_value,
        trade_timestamp=trade.timestamp,
        # The scorer quantizes with the same function, so the stored score,
        # confidences, and threshold replay the decision exactly (FR-012).
        weighted_score=quantize_score_value(assessment.weighted_score),
        signals_triggered=assessment.signals_triggered,
        fresh_wallet_confidence=(
            quantize_score_value(fresh.confidence) if fresh is not None else None
        ),
        size_anomaly_confidence=(
            quantize_score_value(size_sig.confidence) if size_sig is not None else None
        ),
        is_niche_market=size_sig.is_niche_market if size_sig is not None else None,
        volume_impact=(
            Decimal(str(round(size_sig.volume_impact, 4))) if size_sig is not None else None
        ),
        book_impact=(
            Decimal(str(round(size_sig.book_impact, 4))) if size_sig is not None else None
        ),
        wallet_age_hours=wallet_age,
        should_alert=assessment.should_alert,
        threshold_at_eval=quantize_score_value(alert_threshold),
        scoring_algorithm_version=assessment.scoring_algorithm_version,
        scoring_config=row_scoring_config(assessment, detector_config),
        delivery_disposition=assessment.delivery_disposition,
        delivery_channels=assessment.delivery_channels,
        dry_run=assessment.dry_run,
        volume_available=assessment.volume_available,
        market_daily_volume=assessment.market_daily_volume,
        book_depth_available=assessment.book_depth_available,
        wallet_tx_count=assessment.wallet_tx_count,
        wallet_age_known=assessment.wallet_age_known,
    )


async def insert_despite_cancellation(
    db_manager: DatabaseManager, dto: RiskAssessmentDTO, on_error: Callable[[str], None]
) -> None:
    """Insert one assessment row; a cancelled caller still gets the row (bounded wait)."""
    await complete_despite_cancellation(_insert_assessment(db_manager, dto), on_error)


async def _insert_assessment(db_manager: DatabaseManager, dto: RiskAssessmentDTO) -> None:
    """Insert one assessment row in its own committed session."""
    async with db_manager.get_async_session() as session:
        await RiskAssessmentRepository(session).insert(dto)


async def complete_despite_cancellation(
    write: Awaitable[None],
    on_error: Callable[[str], None],
    timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
) -> None:
    """Await ``write`` to completion even if the awaiting task is cancelled meanwhile.

    The write runs as its own shielded task. A cancellation of the caller waits at most
    ``timeout`` seconds (the graceful-shutdown bound) for the write, reports a late failure
    or an overrun through ``on_error``, and is then re-raised: deferred, never swallowed.
    A repeated cancellation during that wait stops waiting at once and detaches the write,
    whose late failure is still reported through ``on_error``.
    """
    task = asyncio.ensure_future(write)
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        await _finish_cancelled_write(task, on_error, timeout)
        raise


async def _finish_cancelled_write(
    task: asyncio.Future[None], on_error: Callable[[str], None], timeout: float
) -> None:
    """Let the shielded write finish within ``timeout``; report what cannot be raised."""
    try:
        finished = await wait_bounded(task, timeout)
    except asyncio.CancelledError:
        _detach_write(task, on_error)
        raise
    except Exception as e:
        on_error(f"Durable write failed after cancellation: {redact_exception_message(e)}")
        return
    if not finished:
        on_error(f"Durable write abandoned after the {timeout:g}s cancellation bound")


_DETACHED_WRITES: set[asyncio.Future[None]] = set()


def _detach_write(task: asyncio.Future[None], on_error: Callable[[str], None]) -> None:
    """Keep a write nobody awaits alive and report its failure when it finishes."""
    _DETACHED_WRITES.add(task)
    task.add_done_callback(_DETACHED_WRITES.discard)
    task.add_done_callback(partial(_report_detached_write, on_error))


def _report_detached_write(on_error: Callable[[str], None], task: asyncio.Future[None]) -> None:
    """Report the outcome of a detached write that did not succeed."""
    if task.cancelled():
        on_error("Durable write cancelled after a repeated cancellation")
        return
    failure = task.exception()
    if failure is not None:
        on_error(f"Durable write failed after cancellation: {redact_exception_message(failure)}")
