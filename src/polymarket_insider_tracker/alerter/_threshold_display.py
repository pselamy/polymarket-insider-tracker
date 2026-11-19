"""The effective alert threshold shown on every alert (SC-005).

The value is read from the assessment's own ``scoring_config``, never from the global setting,
so an override is shown as the threshold the assessment was actually scored against.
"""

from __future__ import annotations

from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.detector.scoring_config import read_pure_config

UNAVAILABLE_THRESHOLD = "unavailable"


def effective_threshold_text(assessment: RiskAssessment) -> str:
    """The threshold the assessment was scored against, to three decimals."""
    if assessment.scoring_config is None:
        return UNAVAILABLE_THRESHOLD
    try:
        threshold = read_pure_config(assessment.scoring_config).alert_threshold
    except ValueError:
        return UNAVAILABLE_THRESHOLD
    return f"{threshold:.3f}"


def score_with_threshold(assessment: RiskAssessment, risk_level: str) -> str:
    """The score, its risk level and the effective alert threshold as one value."""
    return (
        f"{assessment.weighted_score:.2f} ({risk_level}); "
        f"alert threshold {effective_threshold_text(assessment)}"
    )
