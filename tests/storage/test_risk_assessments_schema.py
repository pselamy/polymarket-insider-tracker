"""Tests for updated risk_assessments table schema and RiskAssessmentModel."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import Text, create_engine, select
from sqlalchemy.orm import Session

from polymarket_insider_tracker.detector.models import SCORING_ALGORITHM_VERSION, RiskAssessment
from polymarket_insider_tracker.storage.models import Base, RiskAssessmentModel


def test_risk_assessment_model_has_slice003_columns() -> None:
    """Verify that RiskAssessmentModel defines the unified slice003/004 storage columns."""
    columns = {col.name: col for col in RiskAssessmentModel.__table__.columns}

    assert "delivery_disposition" in columns
    assert "delivery_channels" in columns
    assert "dry_run" in columns
    assert "volume_available" in columns
    assert "market_daily_volume" in columns
    assert "book_depth_available" in columns
    assert "wallet_tx_count" in columns
    assert "wallet_age_known" in columns
    assert "scoring_algorithm_version" in columns
    assert "scoring_config" in columns


def test_scoring_identity_columns_have_truthful_shapes() -> None:
    """Every new row must supply its algorithm version explicitly (no insert default
    that could silently mislabel it), while the exact configuration is nullable only
    because legacy rows' configurations are unknowable."""
    version = RiskAssessmentModel.__table__.columns["scoring_algorithm_version"]
    config = RiskAssessmentModel.__table__.columns["scoring_config"]

    assert version.nullable is False
    assert version.default is None
    assert version.server_default is None
    assert config.nullable is True
    assert isinstance(config.type, Text)


def test_delivery_channels_column_is_text() -> None:
    """The assessment-storage contract requires TEXT for the channel status JSON map."""
    column = RiskAssessmentModel.__table__.columns["delivery_channels"]
    assert isinstance(column.type, Text)


def test_risk_assessment_domain_model_has_slice003_fields() -> None:
    """Verify that RiskAssessment dataclass supports the new fields with backward-compatible defaults."""
    assessment = RiskAssessment(
        trade_event=None,  # type: ignore[arg-type]
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        market_id="market-test-1",
        weighted_score=0.85,
        signals_triggered=2,
        should_alert=True,
    )
    assert assessment.delivery_disposition == "unrecorded"
    assert assessment.delivery_channels is None
    assert assessment.dry_run is False
    assert assessment.volume_available is None
    assert assessment.market_daily_volume is None
    assert assessment.book_depth_available is None
    assert assessment.wallet_tx_count is None
    assert assessment.wallet_age_known is None
    assert assessment.scoring_algorithm_version == SCORING_ALGORITHM_VERSION
    assert assessment.scoring_config is None


def test_delivery_disposition_default_never_claims_a_dry_run() -> None:
    """Rows without a recorded delivery outcome must not be labeled ``dry_run`` while
    ``dry_run`` stays false; the honest default for an unrecorded outcome is ``unrecorded``."""
    column = RiskAssessmentModel.__table__.columns["delivery_disposition"]

    assert column.default is not None and column.default.arg == "unrecorded"
    assert column.server_default is not None
    assert getattr(column.server_default, "arg", None) == "unrecorded"


def test_risk_assessment_model_sqlite_round_trip() -> None:
    """Verify that SQLite can create the table and round-trip a complete assessment row."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    row = RiskAssessmentModel(
        assessment_id=str(uuid.uuid4()),
        trade_id="trade-123",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        market_id="market-xyz",
        side="BUY",
        price=Decimal("0.55"),
        size=Decimal("1000"),
        notional_usdc=Decimal("550.00"),
        trade_timestamp=datetime.now(UTC),
        weighted_score=Decimal("0.850"),
        signals_triggered=2,
        should_alert=True,
        threshold_at_eval=Decimal("0.800"),
        scoring_algorithm_version=SCORING_ALGORITHM_VERSION,
        scoring_config='{"alert_threshold":"0.800","weights":{"fresh_wallet":"0.4"}}',
        delivery_disposition="delivered",
        delivery_channels='{"discord": "delivered", "telegram": "delivered"}',
        dry_run=False,
        volume_available=True,
        market_daily_volume=Decimal("50000.00"),
        book_depth_available=False,
        wallet_tx_count=2,
        wallet_age_known=True,
    )

    with Session(engine) as session:
        session.add(row)
        session.commit()

        saved = session.scalar(
            select(RiskAssessmentModel).where(RiskAssessmentModel.trade_id == "trade-123")
        )
        assert saved is not None
        assert saved.delivery_disposition == "delivered"
        assert saved.delivery_channels == '{"discord": "delivered", "telegram": "delivered"}'
        assert saved.dry_run is False
        assert saved.volume_available is True
        assert saved.market_daily_volume == Decimal("50000.00")
        assert saved.wallet_tx_count == 2
        assert saved.wallet_age_known is True
        assert saved.scoring_algorithm_version == SCORING_ALGORITHM_VERSION
        assert saved.scoring_config is not None
        assert '"alert_threshold":"0.800"' in saved.scoring_config
