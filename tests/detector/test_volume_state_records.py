"""The recorded facts of a volume verdict, and the edges of the volume-state helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.volume_state import (
    NOT_A_NUMBER,
    SizeVolumeEvidence,
    VolumeState,
    VolumeVerdict,
    classify_volume,
    exact_seconds,
    measured_impact,
    size_volume_evidence,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token
from polymarket_insider_tracker.ingestor.volume_observation import GAMMA_SOURCE, VolumeObservation

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
STAMP = (NOW - timedelta(seconds=30)).isoformat()


def _metadata(volume: VolumeObservation | None = None, category: str = "science") -> MarketMetadata:
    return MarketMetadata(
        condition_id="0xa",
        question="Q?",
        description="",
        tokens=(Token("t", "Yes", Decimal("0.5")),),
        category=category,
        volume=volume,
    )


def _evidence(
    metadata: MarketMetadata, category_source: str, threshold: Decimal | None = None
) -> SizeVolumeEvidence:
    if threshold is None:
        return size_volume_evidence(metadata, category_source, Decimal(100), lambda: NOW)
    return size_volume_evidence(
        metadata, category_source, Decimal(100), lambda: NOW, niche_threshold=threshold
    )


def test_an_observed_verdict_records_what_was_observed() -> None:
    observation = VolumeObservation(True, "30000", STAMP, GAMMA_SOURCE)

    verdict = classify_volume(observation, evaluated_at=NOW)

    assert verdict == VolumeVerdict(
        VolumeState.AVAILABLE, "gamma", Decimal(30000), True, "30000", GAMMA_SOURCE, STAMP, "30"
    )


def test_an_absent_observation_records_nothing_observed() -> None:
    verdict = classify_volume(None, evaluated_at=NOW)

    assert verdict == VolumeVerdict(
        VolumeState.UNAVAILABLE, "no_volume_record", None, False, None, None, None, None
    )


def test_an_explicit_volume_is_recorded_as_a_caller_assertion_at_age_zero() -> None:
    verdict = classify_volume(None, evaluated_at=NOW, explicit=Decimal("5000"))

    assert verdict == VolumeVerdict(
        VolumeState.AVAILABLE, "caller", Decimal(5000), True, "5000", "caller", NOW.isoformat(), "0"
    )


@pytest.mark.parametrize("raw", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_float_is_not_a_number(raw: float) -> None:
    verdict = classify_volume(VolumeObservation(True, raw, STAMP, GAMMA_SOURCE), evaluated_at=NOW)

    assert (verdict.state, verdict.reason) == (VolumeState.INVALID, NOT_A_NUMBER)


def test_exact_seconds_stays_exact_beyond_float_precision() -> None:
    assert exact_seconds(timedelta(days=100_000_000, microseconds=1)) == Decimal(
        "8640000000000.000001"
    )


def test_the_impact_needs_both_the_available_state_and_a_volume() -> None:
    def verdict(state: VolumeState, volume: Decimal | None) -> VolumeVerdict:
        return VolumeVerdict(state, "r", volume, True, None, None, None, None)

    assert measured_impact(Decimal(1), verdict(VolumeState.AVAILABLE, Decimal(4))) == Decimal(
        "0.2500"
    )
    assert measured_impact(Decimal(1), verdict(VolumeState.AVAILABLE, None)) is None
    assert measured_impact(Decimal(1), verdict(VolumeState.STALE, Decimal(4))) is None


def test_a_failed_metadata_lookup_is_the_unavailable_reason() -> None:
    evidence = _evidence(_metadata(category="other"), "minimal")

    assert (evidence.verdict.state, evidence.verdict.reason) == (
        VolumeState.UNAVAILABLE,
        "metadata_lookup_failed",
    )


def test_a_real_lookup_without_a_volume_has_no_failure_reason() -> None:
    assert _evidence(_metadata(), "metadata").verdict.reason == "no_volume_record"


def test_the_niche_threshold_decides_the_numeric_basis() -> None:
    metadata = _metadata(VolumeObservation(True, "30000", STAMP, GAMMA_SOURCE))

    assert _evidence(metadata, "metadata").niche_basis == "numeric"
    assert _evidence(metadata, "metadata", Decimal(20000)).niche_basis == "none"


def test_the_evidence_records_the_category_and_its_source() -> None:
    evidence = _evidence(_metadata(category="tech"), "metadata")

    assert (evidence.category, evidence.category_source) == ("tech", "metadata")
