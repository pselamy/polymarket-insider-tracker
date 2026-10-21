"""``assessment_dto``: the persisted row carries the wallet age and signal evidence it scored."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from polymarket_insider_tracker.assessment_row import assessment_dto
from polymarket_insider_tracker.detector.models import RiskAssessment
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
