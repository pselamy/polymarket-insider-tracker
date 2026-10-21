"""The default alert-path report (FR-022a, T029).

Whether the configured live sources can produce any assessment that reaches the alert threshold
is a deliberate, disclosed posture, not a fault. The report is computed from a declared live
evidence capability (code constants, never an environment variable) and the effective threshold,
through the real scorer combination, and is shown on the startup log, ``--config-check`` and
``/health``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from polymarket_insider_tracker.detector.scorer import (
    DEFAULT_WEIGHTS,
    quantize_score_value,
    size_group_score,
)
from polymarket_insider_tracker.detector.scoring_config import (
    DEFAULT_WALLET_GROUP_STATUS,
    WalletGroupStatus,
)
from polymarket_insider_tracker.detector.size_anomaly import (
    MAX_NICHE_CONFIDENCE,
    MAX_VOLUME_CONFIDENCE,
)

WALLET_AGE_SOURCE_NONE = "none"
LIVE_WALLET_AGE_SOURCE = WALLET_AGE_SOURCE_NONE
AlertPathStatus = Literal["reachable", "unreachable"]


@dataclass(frozen=True)
class LiveEvidenceCapability:
    """Which evidence classes the configured live sources can produce."""

    wallet_group_status: WalletGroupStatus = DEFAULT_WALLET_GROUP_STATUS
    wallet_age_source: str = LIVE_WALLET_AGE_SOURCE


LIVE_CAPABILITY = LiveEvidenceCapability()


@dataclass(frozen=True)
class AlertPathReport:
    """Whether a default-configuration assessment can reach the alert threshold."""

    status: AlertPathStatus
    threshold: Decimal
    ceiling: Decimal
    wallet_group_status: str
    wallet_age_source: str

    def fields(self) -> dict[str, str]:
        """The decimal-string fields shared by the log record and the ``/health`` object."""
        return {
            "status": self.status,
            "threshold": str(self.threshold),
            "ceiling": str(self.ceiling),
            "wallet_group_status": self.wallet_group_status,
            "wallet_age_source": self.wallet_age_source,
        }

    def log_message(self) -> str:
        """The startup record text (the WARNING when unreachable, an INFO otherwise)."""
        return (
            f"default alert path {self.status}: threshold={self.threshold} "
            f"ceiling={self.ceiling} wallet_group_status={self.wallet_group_status} "
            f"wallet_age_source={self.wallet_age_source}"
        )

    def console_line(self) -> str:
        """The ``Alert Path:`` line of the configuration summary."""
        return (
            f"Alert Path: {self.status} (threshold {self.threshold}, "
            f"best-case score {self.ceiling}, wallet group {self.wallet_group_status})"
        )


def _best_case_ceiling() -> Decimal:
    """The best score any producible class reaches, via the real scorer combination.

    While the wallet group is unqualified it contributes nothing, so the producible classes are
    the size group with and without the niche modifier at their saturated confidences.
    """
    niche, _ = size_group_score(DEFAULT_WEIGHTS, MAX_NICHE_CONFIDENCE, niche=True)
    plain, _ = size_group_score(DEFAULT_WEIGHTS, MAX_VOLUME_CONFIDENCE, niche=False)
    return quantize_score_value(max(niche, plain))


def alert_path_report(
    threshold: float, capability: LiveEvidenceCapability = LIVE_CAPABILITY
) -> AlertPathReport:
    """The report for ``threshold``; unreachable when the ceiling is below it (scorer's ``>=``)."""
    ceiling = _best_case_ceiling()
    quantized = quantize_score_value(threshold)
    status: AlertPathStatus = "reachable" if ceiling >= quantized else "unreachable"
    return AlertPathReport(
        status,
        quantized,
        ceiling,
        capability.wallet_group_status.value,
        capability.wallet_age_source,
    )


def log_alert_path(logger: logging.Logger, report: AlertPathReport) -> None:
    """Emit the startup record: a WARNING when the path is unreachable, an INFO otherwise."""
    level = logging.WARNING if report.status == "unreachable" else logging.INFO
    logger.log(level, "%s", report.log_message())
