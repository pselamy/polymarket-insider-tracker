"""``assessment_dto``: the persisted row carries the wallet age and signal evidence it scored."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import cast

import pytest

from polymarket_insider_tracker.assessment_row import assessment_dto
from polymarket_insider_tracker.detector.models import RiskAssessment, SizeAnomalySignal
from polymarket_insider_tracker.detector.scoring_config import DetectorConfig
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token
from polymarket_insider_tracker.profiler.models import WalletProfile

_CONFIG = DetectorConfig(
    fresh_transaction_boundary=5,
    size_volume_threshold=Decimal("0.02"),
    fresh_min_notional=Decimal("1000"),
    known_age_ceiling_hours=Decimal("48"),
    niche_volume_threshold=Decimal("50000"),
    niche_min_notional=Decimal("1000"),
)


@pytest.fixture
def sample_metadata() -> MarketMetadata:
    return MarketMetadata(
        condition_id="market_abc123",
        question="Test market?",
        description="Test",
        tokens=(Token(token_id="token_123", outcome="Yes", price=Decimal("0.65")),),
        category="other",
    )


def test_known_wallet_age_is_stored_rounded_to_two_places(
    high_risk_assessment: RiskAssessment,
) -> None:
    assert high_risk_assessment.fresh_wallet_signal is not None
    profile = replace(high_risk_assessment.fresh_wallet_signal.wallet_profile, age_hours=1.2345)
    signal = replace(high_risk_assessment.fresh_wallet_signal, wallet_profile=profile)
    assessment = replace(high_risk_assessment, fresh_wallet_signal=signal)

    row = assessment_dto(assessment, alert_threshold=0.8, detector_config=_CONFIG)

    assert row.wallet_age_hours == Decimal("1.23")
    assert row.wallet_address == assessment.wallet_address.lower()


def test_unknown_wallet_age_is_stored_as_none(high_risk_assessment: RiskAssessment) -> None:
    assert high_risk_assessment.fresh_wallet_signal is not None
    profile: WalletProfile = replace(
        high_risk_assessment.fresh_wallet_signal.wallet_profile, age_hours=None
    )
    signal = replace(high_risk_assessment.fresh_wallet_signal, wallet_profile=profile)
    assessment = replace(high_risk_assessment, fresh_wallet_signal=signal)

    row = assessment_dto(assessment, alert_threshold=0.8, detector_config=_CONFIG)

    assert row.wallet_age_hours is None


def test_assessment_dto_writes_measured_volume_impact(
    high_risk_assessment: RiskAssessment,
) -> None:
    # Arrange
    size_sig = replace(
        cast(SizeAnomalySignal, high_risk_assessment.size_anomaly_signal),
        volume_impact_measured=Decimal("0.3250"),
        book_impact_measured=None,
    )
    assessment = replace(high_risk_assessment, size_anomaly_signal=size_sig)

    # Act
    row = assessment_dto(assessment, alert_threshold=0.8, detector_config=_CONFIG)

    # Assert
    assert row.volume_impact == Decimal("0.3250")


def test_assessment_dto_writes_none_book_impact_beside_measured_volume(
    high_risk_assessment: RiskAssessment,
) -> None:
    # Arrange
    size_sig = replace(
        cast(SizeAnomalySignal, high_risk_assessment.size_anomaly_signal),
        volume_impact_measured=Decimal("0.3250"),
        book_impact_measured=None,
    )
    assessment = replace(high_risk_assessment, size_anomaly_signal=size_sig)

    # Act
    row = assessment_dto(assessment, alert_threshold=0.8, detector_config=_CONFIG)

    # Assert
    assert row.book_impact is None


def test_assessment_dto_writes_none_volume_impact_for_unmeasured_signal(
    high_risk_assessment: RiskAssessment,
) -> None:
    # Arrange
    size_sig = replace(
        cast(SizeAnomalySignal, high_risk_assessment.size_anomaly_signal),
        volume_impact=0.0,
        book_impact=0.0,
        volume_impact_measured=None,
        book_impact_measured=None,
    )
    assessment = replace(high_risk_assessment, size_anomaly_signal=size_sig)

    # Act
    row = assessment_dto(assessment, alert_threshold=0.8, detector_config=_CONFIG)

    # Assert
    assert row.volume_impact is None


def test_assessment_dto_writes_none_book_impact_for_unmeasured_signal(
    high_risk_assessment: RiskAssessment,
) -> None:
    # Arrange
    size_sig = replace(
        cast(SizeAnomalySignal, high_risk_assessment.size_anomaly_signal),
        volume_impact=0.0,
        book_impact=0.0,
        volume_impact_measured=None,
        book_impact_measured=None,
    )
    assessment = replace(high_risk_assessment, size_anomaly_signal=size_sig)

    # Act
    row = assessment_dto(assessment, alert_threshold=0.8, detector_config=_CONFIG)

    # Assert
    assert row.book_impact is None


def test_assessment_dto_writes_none_volume_impact_when_no_size_signal(
    high_risk_assessment: RiskAssessment,
) -> None:
    # Arrange
    assessment = replace(high_risk_assessment, size_anomaly_signal=None)

    # Act
    row = assessment_dto(assessment, alert_threshold=0.8, detector_config=_CONFIG)

    # Assert
    assert row.volume_impact is None


def test_assessment_dto_writes_none_book_impact_when_no_size_signal(
    high_risk_assessment: RiskAssessment,
) -> None:
    # Arrange
    assessment = replace(high_risk_assessment, size_anomaly_signal=None)

    # Act
    row = assessment_dto(assessment, alert_threshold=0.8, detector_config=_CONFIG)

    # Assert
    assert row.book_impact is None
