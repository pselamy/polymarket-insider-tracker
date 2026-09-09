"""Tests for RiskAssessment persistence inside Pipeline._score_and_alert.

Verifies:
  1. Every signal-bearing assessment is written to risk_assessments, even
     when ``should_alert`` is False (i.e. below the alert threshold).
  2. A DB failure during persistence never blocks alert dispatching.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import Base, RiskAssessmentModel
from tests.fakes.pipeline import (
    BrokenDatabaseManager,
    FakeAlertDispatcher,
    FakeAlertFormatter,
    FakeRiskScorer,
    make_test_settings,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def test_settings() -> Settings:
    """Concrete Settings instance for testing."""
    return make_test_settings(persist_assessments=True, alert_threshold=0.8)


@pytest.fixture
async def async_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_manager(async_engine):
    manager = DatabaseManager.__new__(DatabaseManager)
    manager.database_url = "sqlite+aiosqlite:///:memory:"
    manager.async_mode = True
    manager._pool_size = 5
    manager._max_overflow = 10
    manager._echo = False
    manager._sync_engine = None
    manager._async_engine = async_engine
    manager._sync_session_factory = None
    manager._async_session_factory = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    return manager


@pytest.fixture
def sample_trade() -> TradeEvent:
    return TradeEvent(
        trade_id="0x" + "a" * 64,
        wallet_address="0x" + "b" * 40,
        market_id="0x" + "c" * 64,
        asset_id="asset_xyz",
        side="BUY",
        price=Decimal("0.42"),
        size=Decimal("1000"),
        timestamp=datetime.now(UTC),
        outcome="Yes",
        outcome_index=0,
        event_title="Test Event",
        market_slug="test-market",
    )


def _make_assessment(trade: TradeEvent, *, should_alert: bool, score: float) -> RiskAssessment:
    return RiskAssessment(
        trade_event=trade,
        wallet_address=trade.wallet_address,
        market_id=trade.market_id,
        fresh_wallet_signal=None,
        size_anomaly_signal=None,
        signals_triggered=1,
        weighted_score=score,
        should_alert=should_alert,
    )


def _build_pipeline(
    settings: Settings,
    *,
    db_manager: Any = None,
    assessment: RiskAssessment,
    dispatcher: FakeAlertDispatcher | None = None,
) -> tuple[Pipeline, FakeAlertDispatcher]:
    """Construct a Pipeline with concrete fakes wired in."""
    pipeline = Pipeline(settings)
    pipeline._db_manager = db_manager
    pipeline._risk_scorer = FakeRiskScorer(assessment)  # type: ignore[assignment]
    pipeline._alert_formatter = FakeAlertFormatter()  # type: ignore[assignment]
    alert_dispatcher = dispatcher if dispatcher is not None else FakeAlertDispatcher()
    pipeline._alert_dispatcher = alert_dispatcher  # type: ignore[assignment]
    pipeline._dry_run = False
    return pipeline, alert_dispatcher


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPersistAssessment:
    @pytest.mark.asyncio
    async def test_below_threshold_assessment_is_persisted(
        self,
        test_settings: Settings,
        db_manager: DatabaseManager,
        sample_trade: TradeEvent,
        async_engine: Any,
    ) -> None:
        """Assessments with should_alert=False must still hit the DB; no dispatch."""
        assessment = _make_assessment(sample_trade, should_alert=False, score=0.45)
        pipeline, dispatcher = _build_pipeline(
            test_settings, db_manager=db_manager, assessment=assessment
        )

        await pipeline._score_and_alert(SignalBundle(trade_event=sample_trade))

        # Row landed in risk_assessments
        async with async_sessionmaker(bind=async_engine, expire_on_commit=False)() as session:
            rows = (await session.execute(select(RiskAssessmentModel))).scalars().all()
            assert len(rows) == 1
            row = rows[0]
            assert row.assessment_id == assessment.assessment_id
            assert row.should_alert is False
            assert float(row.weighted_score) == pytest.approx(0.45, abs=1e-3)
            assert row.wallet_address == sample_trade.wallet_address.lower()

        # No alert dispatched for sub-threshold assessments
        assert len(dispatcher.dispatched) == 0
        assert pipeline.stats.alerts_sent == 0

    @pytest.mark.asyncio
    async def test_persistence_failure_does_not_block_dispatch(
        self, test_settings: Settings, sample_trade: TradeEvent
    ) -> None:
        """If repo.insert blows up, the alert pipeline still ships the alert."""
        assessment = _make_assessment(sample_trade, should_alert=True, score=0.92)

        broken_db = BrokenDatabaseManager()
        pipeline, dispatcher = _build_pipeline(
            test_settings, db_manager=broken_db, assessment=assessment
        )

        await pipeline._score_and_alert(SignalBundle(trade_event=sample_trade))

        # DB write was attempted and failed silently
        assert broken_db.calls == 1

        # Dispatcher still ran and the stats counter incremented
        assert len(dispatcher.dispatched) == 1
        assert pipeline.stats.alerts_sent == 1
