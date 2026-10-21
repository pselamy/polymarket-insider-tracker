"""Evaluation results: the evidence of one trade evaluation, signal or not (task T011, minimal).

Contract: ``specs/004-truthful-detection-contract/contracts/detector-api.md`` section 2. The
detectors' ``evaluate`` methods return these frozen snapshots; the signal-returning ``analyze``
wrappers share the same acquisition and calculation. Only what the 004.1 writer needs is here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING

from polymarket_insider_tracker.detector.scoring_config import DetectorConfig, DetectorFailure

if TYPE_CHECKING:
    from polymarket_insider_tracker.detector.models import FreshWalletSignal, SizeAnomalySignal
    from polymarket_insider_tracker.detector.volume_state import SizeVolumeEvidence
    from polymarket_insider_tracker.profiler.models import WalletProfile


_ERROR_CLASS_LIMIT = 128
_MESSAGE_LIMIT = 200


class WalletClassification(StrEnum):
    """The wallet classifications of data-model section 2."""

    FRESH = "fresh"
    LOW_ACTIVITY_UNKNOWN_AGE = "low_activity_unknown_age"
    KNOWN_NONQUALIFYING = "known_nonqualifying"
    NOT_REQUESTED = "not_requested"
    RETRIEVAL_FAILED = "retrieval_failed"


@dataclass(frozen=True)
class WalletEvaluation:
    """The wallet side of one evaluation: the classification and the profile behind it."""

    evaluated_at: datetime
    classification: WalletClassification
    boundary: int
    profile: WalletProfile | None
    signal: FreshWalletSignal | None


@dataclass(frozen=True)
class SizeEvaluation:
    """The size side of one evaluation: the volume evidence, recorded with or without a signal."""

    evaluated_at: datetime
    notional: Decimal
    volume: SizeVolumeEvidence
    signal: SizeAnomalySignal | None


@dataclass(frozen=True)
class DetectorError:
    """A detector run that raised: the redacted operational message and the exception class."""

    detector: str
    error_class: str
    message: str

    def recorded(self) -> DetectorFailure:
        """The bounded form stored in a 004.1 document (scoring-config.md section 2)."""
        return DetectorFailure(
            self.detector, self.error_class[:_ERROR_CLASS_LIMIT], self.message[:_MESSAGE_LIMIT]
        )


@dataclass(frozen=True)
class TradeEvidence:
    """Everything one trade's evaluation observed, carried from the bundle to the persisted row.

    ``wallet``/``size`` are None when that detector was not run or failed; its failure is listed
    in ``failures``. ``detector_config`` is the captured static configuration of both detectors.
    """

    evaluated_at: datetime
    detector_config: DetectorConfig
    wallet: WalletEvaluation | None = None
    size: SizeEvaluation | None = None
    failures: tuple[DetectorFailure, ...] = ()
