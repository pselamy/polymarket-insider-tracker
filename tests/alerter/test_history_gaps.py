"""Behaviour of the alert-history records not pinned elsewhere."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.alerter.history import (
    AlertHistory,
    AlertRecord,
    _get_signals_from_assessment,
)
from polymarket_insider_tracker.detector.models import RiskAssessment, SizeAnomalySignal
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent


def test_from_dict_keeps_a_datetime_created_at() -> None:
    """A datetime ``created_at`` is used as-is rather than parsed or replaced."""
    moment = datetime(2026, 1, 4, 16, 0, tzinfo=UTC)
    record = AlertRecord.from_dict(
        {
            "alert_id": "a",
            "wallet_address": "0x1",
            "market_id": "m",
            "risk_score": 0.5,
            "dedup_key": "k",
            "created_at": moment,
        }
    )

    assert record.created_at == moment


def test_size_anomaly_without_niche_market_omits_niche_signal(sample_trade: TradeEvent) -> None:
    """A size anomaly outside a niche market is reported without ``niche_market``."""
    metadata = MarketMetadata(
        condition_id="market_abc123",
        question="Test market?",
        description="Test",
        tokens=(Token(token_id="token_123", outcome="Yes", price=Decimal("0.65")),),
        category="other",
    )
    signal = SizeAnomalySignal(
        trade_event=sample_trade,
        market_metadata=metadata,
        volume_impact=0.1,
        book_impact=0.1,
        is_niche_market=False,
        confidence=0.5,
        factors={},
    )
    assessment = RiskAssessment(
        trade_event=sample_trade,
        wallet_address=sample_trade.wallet_address,
        market_id=sample_trade.market_id,
        fresh_wallet_signal=None,
        size_anomaly_signal=signal,
        signals_triggered=1,
        weighted_score=0.4,
        should_alert=False,
    )

    assert _get_signals_from_assessment(assessment) == ["size_anomaly"]


@pytest.mark.asyncio
async def test_feedback_on_a_key_without_expiry_applies_the_retention_ttl(
    fake_redis: FakeAsyncRedis,
) -> None:
    """A stored alert with no TTL gets the configured retention TTL on feedback."""
    history = AlertHistory(fake_redis)
    key = f"{history.KEY_PREFIX_ALERT}no-ttl"
    await fake_redis.set(key, json.dumps({"alert_id": "no-ttl", "feedback_useful": None}))

    assert await history.record_feedback("no-ttl", useful=False) is True

    assert json.loads(await fake_redis.get(key))["feedback_useful"] is False
    assert await fake_redis.ttl(key) > 0
