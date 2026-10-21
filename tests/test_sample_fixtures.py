"""Characterization of the sample fixtures shared through ``tests/conftest.py``."""

from datetime import UTC
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.models import (
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile
from tests.batch_trades import indexed_trades


def test_sample_fixtures_pin_shared_trade_and_profile(
    sample_trade: TradeEvent, sample_wallet_profile: WalletProfile
) -> None:
    """Pin the sample trade/profile values that the alerter-history and risk-scorer tests share."""
    assert (sample_trade.market_id, sample_trade.trade_id) == ("market_abc123", "tx_001")
    assert sample_trade.wallet_address == "0x1234567890abcdef1234567890abcdef12345678"
    assert (sample_trade.side, sample_trade.outcome, sample_trade.outcome_index) == (
        "BUY",
        "Yes",
        0,
    )
    assert (sample_trade.price, sample_trade.size) == (Decimal("0.65"), Decimal("10000"))
    assert (sample_trade.asset_id, sample_trade.event_title) == ("token_123", "Test Market")
    assert sample_trade.timestamp.tzinfo is UTC
    assert sample_wallet_profile.address == sample_trade.wallet_address
    assert (sample_wallet_profile.nonce, sample_wallet_profile.age_hours) == (2, 1.0)
    assert (sample_wallet_profile.is_fresh, sample_wallet_profile.total_tx_count) == (True, 2)
    assert sample_wallet_profile.matic_balance == Decimal("1000000000000000000")
    assert sample_wallet_profile.usdc_balance == Decimal("1000000")


def test_shared_signal_fixtures_pin_the_high_risk_assessment(
    fresh_wallet_signal: FreshWalletSignal,
    size_anomaly_signal: SizeAnomalySignal,
    high_risk_assessment: RiskAssessment,
    sample_trade: TradeEvent,
) -> None:
    """Pin the signals and assessment the formatter, history and scorer tests share."""
    assert fresh_wallet_signal.trade_event == sample_trade
    assert fresh_wallet_signal.confidence == 0.8
    assert fresh_wallet_signal.factors == {
        "base": 0.5,
        "brand_new_bonus": 0.2,
        "large_trade_bonus": 0.1,
    }
    assert (size_anomaly_signal.volume_impact, size_anomaly_signal.book_impact) == (0.10, 0.15)
    assert (size_anomaly_signal.is_niche_market, size_anomaly_signal.confidence) == (True, 0.7)
    assert size_anomaly_signal.factors == {"volume_impact": 0.4, "book_impact": 0.3}
    assert high_risk_assessment.fresh_wallet_signal == fresh_wallet_signal
    assert high_risk_assessment.size_anomaly_signal == size_anomaly_signal
    assert high_risk_assessment.wallet_address == sample_trade.wallet_address
    assert high_risk_assessment.market_id == sample_trade.market_id
    assert (high_risk_assessment.signals_triggered, high_risk_assessment.weighted_score) == (
        2,
        0.82,
    )
    assert high_risk_assessment.should_alert is True


def test_signal_fixtures_take_the_modules_own_market_metadata(
    size_anomaly_signal: SizeAnomalySignal, sample_metadata: MarketMetadata
) -> None:
    """The size signal is built on whichever ``sample_metadata`` the requesting module defines."""
    assert size_anomaly_signal.market_metadata == sample_metadata


def test_indexed_trades_carry_their_index() -> None:
    """Each indexed trade has its own market, trade id and wallet and the shared constants."""
    trades = indexed_trades(2)

    assert [(t.market_id, t.trade_id, t.wallet_address) for t in trades] == [
        ("market_0", "tx_0", "0xwallet0"),
        ("market_1", "tx_1", "0xwallet1"),
    ]
    assert {(t.side, t.outcome, t.price, t.size, t.asset_id) for t in trades} == {
        ("BUY", "Yes", Decimal("0.50"), Decimal("10000"), "token_123")
    }
    assert indexed_trades(0) == []


@pytest.fixture
def sample_metadata() -> MarketMetadata:
    """The module's own market metadata, as each consuming test module defines."""
    return MarketMetadata(
        condition_id="market_abc123",
        question="Pinned?",
        description="Pinned",
        tokens=(Token(token_id="token_123", outcome="Yes", price=Decimal("0.65")),),
        category="other",
    )
