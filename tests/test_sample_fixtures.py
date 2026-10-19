"""Characterization of the sample fixtures shared through ``tests/conftest.py``."""

from datetime import UTC
from decimal import Decimal

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile


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
