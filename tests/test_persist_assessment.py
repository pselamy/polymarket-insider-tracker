"""Tests for risk-assessment persistence in the pipeline.

Every assessment is persisted through the real ``RiskScorer`` and ``RiskAssessmentRepository``
regardless of whether it alerts, and a persistence failure never blocks an authorized alert.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.models import FreshWalletSignal, SizeAnomalySignal
from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel
from tests.fakes import FakeAlertChannel, FakeEth, make_test_settings, wire_pipeline

UNREACHABLE_DATABASE_URL = "postgresql+psycopg://tracker:unused@127.0.0.1:1/unreachable"


@pytest.fixture
def test_settings() -> Settings:
    """Settings that persist assessments and alert at 0.8."""
    return make_test_settings(persist_assessments=True, alert_threshold=0.8)


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


@pytest.fixture
def fresh_profile(sample_trade: TradeEvent) -> WalletProfile:
    return WalletProfile(
        address=sample_trade.wallet_address,
        nonce=0,
        first_seen=None,
        age_hours=None,
        is_fresh=True,
        total_tx_count=0,
        matic_balance=Decimal("0"),
        usdc_balance=Decimal("0"),
        fresh_threshold=5,
    )


def _fresh_signal(
    trade: TradeEvent, profile: WalletProfile, confidence: float
) -> FreshWalletSignal:
    return FreshWalletSignal(
        trade_event=trade, wallet_profile=profile, confidence=confidence, factors={}
    )


def _niche_signal(trade: TradeEvent, confidence: float) -> SizeAnomalySignal:
    metadata = MarketMetadata(
        condition_id=trade.market_id,
        question="Test Event",
        description="",
        tokens=(Token(token_id=trade.asset_id, outcome=trade.outcome, price=trade.price),),
        category="science",
    )
    return SizeAnomalySignal(
        trade_event=trade,
        market_metadata=metadata,
        volume_impact=0.0,
        book_impact=0.0,
        is_niche_market=True,
        confidence=confidence,
        factors={},
    )


async def _persisted_rows(engine: AsyncEngine) -> list[RiskAssessmentModel]:
    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(RiskAssessmentModel))).scalars().all())


class TestPersistAssessment:
    async def test_below_threshold_assessment_is_persisted(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade: TradeEvent,
        fresh_profile: WalletProfile,
    ) -> None:
        """Assessments with should_alert=False must still hit the DB; nothing is delivered."""
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=db_manager,
            channels=[channel],
        )
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=_fresh_signal(sample_trade, fresh_profile, 0.7),
        )

        await pipeline._score_and_alert(bundle)

        rows = await _persisted_rows(async_engine)
        assert len(rows) == 1
        row = rows[0]
        assert row.trade_id == sample_trade.trade_id
        assert row.wallet_address == sample_trade.wallet_address.lower()
        assert row.should_alert is False
        assert float(row.weighted_score) == pytest.approx(0.28, abs=1e-3)
        assert float(row.fresh_wallet_confidence) == pytest.approx(0.7, abs=1e-3)
        assert float(row.threshold_at_eval) == pytest.approx(0.8, abs=1e-3)
        assert channel.deliveries == []
        assert pipeline.stats.alerts_sent == 0

    async def test_persistence_failure_does_not_block_dispatch(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_profile: WalletProfile,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """If the database is unreachable, the alert still ships and the failure is logged."""
        unreachable_db = DatabaseManager(UNREACHABLE_DATABASE_URL, async_mode=True)
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=FakeEth(),
            db_manager=unreachable_db,
            channels=[channel],
        )
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=_fresh_signal(sample_trade, fresh_profile, 1.0),
            size_anomaly_signal=_niche_signal(sample_trade, 1.0),
        )

        try:
            with caplog.at_level(logging.WARNING):
                await pipeline._score_and_alert(bundle)
        finally:
            await unreachable_db.dispose_async()

        assert "Failed to persist risk assessment" in caplog.text
        assert [alert.links["wallet"] for alert in channel.deliveries] == [
            f"https://polygonscan.com/address/{sample_trade.wallet_address}"
        ]
        assert "Risk Score: 1.00" in channel.deliveries[0].body
        assert pipeline.stats.alerts_sent == 1
