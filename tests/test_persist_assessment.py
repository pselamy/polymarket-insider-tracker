"""Tests for risk-assessment persistence in the pipeline.

Every assessment is persisted through the real ``RiskScorer`` and ``RiskAssessmentRepository``
regardless of whether it alerts, and a persistence failure never blocks an authorized alert.
"""

from __future__ import annotations

import logging

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.storage.database import DatabaseManager
from tests.fakes import FakeAlertChannel, FakeEth, make_test_settings, wire_pipeline
from tests.integration._journey_harness import (
    make_sample_trade_event,
    market_for_trade,
)
from tests.integration._journey_harness import persisted_assessments as _persisted_rows

UNREACHABLE_DATABASE_URL = "postgresql+psycopg://tracker:unused@127.0.0.1:1/unreachable"


@pytest.fixture
def test_settings() -> Settings:
    """Settings that persist assessments and alert at 0.8."""
    return make_test_settings(persist_assessments=True, alert_threshold=0.8)


@pytest.fixture
def sample_trade() -> TradeEvent:
    return make_sample_trade_event()


class TestPersistAssessment:
    async def test_below_threshold_assessment_is_persisted(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade: TradeEvent,
    ) -> None:
        """Assessments with should_alert=False must still hit the DB; nothing is delivered."""
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=FakeEth(transaction_count=2),
            market=market_for_trade(sample_trade, "politics"),
            db_manager=db_manager,
            channels=[channel],
        )
        await pipeline.process_trade(sample_trade)

        rows = await _persisted_rows(async_engine)
        assert len(rows) == 1
        row = rows[0]
        assert row.trade_id == sample_trade.trade_id
        assert row.wallet_address == sample_trade.wallet_address.lower()
        assert row.should_alert is False
        # G1: the recorded wallet confidence below does not score (was 0.2 = 0.5 x 0.4).
        assert float(row.weighted_score) == 0.0
        assert row.fresh_wallet_confidence is not None
        assert abs(float(row.fresh_wallet_confidence) - 0.5) < 1e-3
        assert abs(float(row.threshold_at_eval) - 0.8) < 1e-3
        assert channel.deliveries == []
        assert pipeline.stats.alerts_sent == 0

    async def test_persistence_failure_does_not_block_dispatch(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """If the database is unreachable, the alert still ships and the failure is logged."""
        unreachable_db = DatabaseManager(UNREACHABLE_DATABASE_URL, async_mode=True)
        channel = FakeAlertChannel("discord")
        pipeline = await wire_pipeline(
            make_test_settings(persist_assessments=True, alert_threshold=0.1),
            redis=fake_redis,
            eth=FakeEth(transaction_count=2),
            market=market_for_trade(sample_trade, "science"),
            db_manager=unreachable_db,
            channels=[channel],
        )
        try:
            with caplog.at_level(logging.WARNING):
                await pipeline.process_trade(sample_trade)
        finally:
            await unreachable_db.dispose_async()

        assert "Failed to persist risk assessment" in caplog.text
        assert [alert.links["wallet"] for alert in channel.deliveries] == [
            f"https://polygonscan.com/address/{sample_trade.wallet_address}"
        ]
        # size-only 0.2 x (0.35 + 0.25); it was 0.38 with the wallet term
        assert "Risk Score: 0.12" in channel.deliveries[0].body
        assert pipeline.stats.alerts_sent == 1
