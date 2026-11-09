"""Formatting helpers for the size vs 24h volume alert line."""

from __future__ import annotations

from decimal import Decimal

from polymarket_insider_tracker.detector.models import RiskAssessment, SizeAnomalySignal


def _resolve_daily_volume(
    assessment: RiskAssessment, size_sig: SizeAnomalySignal
) -> Decimal | None:
    if (
        size_sig.volume_evidence is not None
        and size_sig.volume_evidence.verdict.daily_volume is not None
    ):
        return size_sig.volume_evidence.verdict.daily_volume
    if assessment.market_daily_volume is not None:
        return assessment.market_daily_volume
    return None


def _format_unmeasured_volume_state(size_sig: SizeAnomalySignal) -> str:
    if size_sig.volume_evidence is not None:
        state = size_sig.volume_evidence.verdict.state.value
    else:
        state = "unavailable"
    return f"unavailable ({state})"


def _format_measured_volume_impact(assessment: RiskAssessment, size_sig: SizeAnomalySignal) -> str:
    assert size_sig.volume_impact_measured is not None
    pct = size_sig.volume_impact_measured * Decimal("100")
    pct_str = f"{pct:.2f}%"
    daily_volume = _resolve_daily_volume(assessment, size_sig)
    if daily_volume is None:
        return pct_str
    if daily_volume == daily_volume.to_integral_value():
        vol_str = f"${daily_volume:,.0f}"
    else:
        vol_str = f"${daily_volume:,.2f}"
    return f"{pct_str} of {vol_str}"


def format_volume_comparison_value(assessment: RiskAssessment) -> str:
    """Format the value part of the 'Size vs 24h volume' alert line."""
    size_sig = assessment.size_anomaly_signal
    if size_sig is None:
        return ""
    if size_sig.volume_impact_measured is not None:
        return _format_measured_volume_impact(assessment, size_sig)
    return _format_unmeasured_volume_state(size_sig)


def format_volume_comparison_line(assessment: RiskAssessment) -> str:
    """Format the full 'Size vs 24h volume: ...' alert line."""
    return f"Size vs 24h volume: {format_volume_comparison_value(assessment)}"


def format_discord_volume_field(
    verbosity: str, assessment: RiskAssessment
) -> dict[str, object] | None:
    """Build the Discord embed field for 'Size vs 24h volume' if detailed."""
    if verbosity != "detailed":
        return None
    if assessment.size_anomaly_signal is None:
        return None
    return {
        "name": "Size vs 24h volume",
        "value": format_volume_comparison_value(assessment),
        "inline": False,
    }
