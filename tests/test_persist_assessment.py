"""Tests for RiskAssessment persistence inside Pipeline._score_and_alert.

Verifies:
  1. Every signal-bearing assessment is written to risk_assessments, even
     when ``should_alert`` is False (i.e. below the alert threshold).
  2. A DB failure during persistence never blocks alert dispatching.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_settings():
    """Settings stub with the attributes Pipeline reaches for at runtime."""
    detector = MagicMock()
    detector.persist_assessments = True
    detector.alert_threshold = 0.8

    settings = MagicMock(spec=Settings)
    settings.detector = detector
    settings.dry_run = False
    return settings


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
    manager._async_session_factory = async_sessionmaker(
        bind=async_engine, expire_on_commit=False
    )
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
    mock_settings,
    *,
    db_manager=None,
    assessment: RiskAssessment,
    dispatcher: MagicMock | None = None,
) -> Pipeline:
    """Construct a Pipeline with the minimum collaborators wired in."""
    pipeline = Pipeline(mock_settings)
    pipeline._db_manager = db_manager

    pipeline._risk_scorer = MagicMock()
    pipeline._risk_scorer.assess = AsyncMock(return_value=assessment)

    pipeline._alert_formatter = MagicMock()
    pipeline._alert_formatter.format = MagicMock(return_value=MagicMock())

    if dispatcher is None:
        dispatcher = MagicMock()
        dispatcher.dispatch = AsyncMock(
            return_value=MagicMock(all_succeeded=True, success_count=1, failure_count=0)
        )
    pipeline._alert_dispatcher = dispatcher
    pipeline._dry_run = False
    return pipeline


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPersistAssessment:
    @pytest.mark.asyncio
    async def test_below_threshold_assessment_is_persisted(
        self, mock_settings, db_manager, sample_trade, async_engine
    ):
        """Assessments with should_alert=False must still hit the DB; no dispatch."""
        assessment = _make_assessment(sample_trade, should_alert=False, score=0.45)
        pipeline = _build_pipeline(
            mock_settings, db_manager=db_manager, assessment=assessment
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
        pipeline._alert_dispatcher.dispatch.assert_not_called()
        assert pipeline.stats.alerts_sent == 0

    @pytest.mark.asyncio
    async def test_persistence_failure_does_not_block_dispatch(
        self, mock_settings, sample_trade
    ):
        """If repo.insert blows up, the alert pipeline still ships the alert."""
        assessment = _make_assessment(sample_trade, should_alert=True, score=0.92)

        # db_manager whose get_async_session raises -> _persist_assessment swallows it
        broken_db = MagicMock()
        broken_db.get_async_session = MagicMock(
            side_effect=RuntimeError("DB connection failed")
        )

        pipeline = _build_pipeline(
            mock_settings, db_manager=broken_db, assessment=assessment
        )

        await pipeline._score_and_alert(SignalBundle(trade_event=sample_trade))

        # DB write was attempted and failed silently
        broken_db.get_async_session.assert_called_once()

        # Dispatcher still ran and the stats counter incremented
        pipeline._alert_dispatcher.dispatch.assert_awaited_once()
        assert pipeline.stats.alerts_sent == 1
