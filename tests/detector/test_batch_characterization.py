"""Characterization tests for detector batch gather/zip handling (dedup cluster 22).

These pin the behaviour shared by ``FreshWalletDetector.analyze_batch`` and
``SizeAnomalyDetector.analyze_batch`` before their duplicated gather/zip/exception
knowledge gets a single home: results are matched to trades positionally, ``None``
results are dropped, and a per-trade failure is logged with a redacted message and
dropped without aborting the batch.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile
from tests.fakes.metadata import FakeMetadataSync


class BatchAbort(BaseException):
    """A per-trade failure that ``except Exception`` must not swallow."""


class FakeWalletAnalyzer:
    """Wallet profiles keyed by address; one address can be made to raise."""

    def __init__(self) -> None:
        self.profiles: dict[str, WalletProfile] = {}
        self.failures: dict[str, BaseException] = {}

    def register_all(self, trades: list[TradeEvent]) -> None:
        """Give every trade's wallet a fresh profile."""
        for trade in trades:
            self.profiles[trade.wallet_address.lower()] = fresh_profile(trade.wallet_address)

    async def analyze(self, address: str) -> WalletProfile:
        wallet = address.lower()
        if wallet in self.failures:
            raise self.failures[wallet]
        return self.profiles[wallet]


class AbortingMetadataSync:
    """Serves metadata for every market except one, which raises ``BatchAbort``."""

    def __init__(self, aborted_market: str) -> None:
        self._aborted = aborted_market

    async def get_market(self, condition_id: str) -> MarketMetadata | None:
        if condition_id == self._aborted:
            raise BatchAbort(f"metadata aborted for {condition_id}")
        return market_metadata("science")


def make_trade(
    trade_id: str,
    *,
    wallet: str | None = None,
    market: str = "market",
    size: str = "10000",
) -> TradeEvent:
    return TradeEvent(
        market_id=market,
        trade_id=trade_id,
        wallet_address=wallet or f"0x{trade_id}",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.5"),
        size=Decimal(size),
        timestamp=datetime.now(UTC),
        asset_id="asset",
    )


def fresh_profile(address: str) -> WalletProfile:
    return WalletProfile(
        address=address.lower(),
        nonce=2,
        first_seen=datetime.now(UTC),
        age_hours=24.0,
        is_fresh=True,
        total_tx_count=2,
        matic_balance=Decimal("0"),
        usdc_balance=Decimal("0"),
    )


def market_metadata(category: str) -> MarketMetadata:
    return MarketMetadata(
        condition_id="market",
        question="Will it rain?",
        description="",
        tokens=(Token(token_id="asset", outcome="Yes", price=Decimal("0.5")),),
        category=category,
    )


class TestFreshWalletBatchContract:
    """The gather/zip/exception contract as seen through ``FreshWalletDetector``."""

    @pytest.mark.parametrize(
        ("sizes", "expected"),
        [
            (("10000", "10000", "10000"), ["t1", "t2", "t3"]),
            (("10000", "100", "10000"), ["t1", "t3"]),
        ],
    )
    async def test_keeps_input_order_and_drops_none(self, sizes, expected) -> None:
        analyzer = FakeWalletAnalyzer()
        trades = [make_trade(f"t{i}", size=size) for i, size in enumerate(sizes, start=1)]
        analyzer.register_all(trades)

        signals = await FreshWalletDetector(analyzer).analyze_batch(trades)

        assert [signal.trade_event.trade_id for signal in signals] == expected

    async def test_failed_trade_is_dropped_and_logged(self, caplog) -> None:
        analyzer = FakeWalletAnalyzer()
        analyzer.profiles["0xgood"] = fresh_profile("0xgood")
        analyzer.failures["0xbad"] = RuntimeError("rpc https://secret.internal?k=abc123")
        trades = [make_trade("good", wallet="0xgood"), make_trade("bad", wallet="0xbad")]

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            signals = await FreshWalletDetector(analyzer).analyze_batch(trades)

        assert [signal.trade_event.trade_id for signal in signals] == ["good"]
        assert "Failed to analyze trade bad" in caplog.text
        assert "abc123" not in caplog.text

    async def test_empty_batch_is_empty(self) -> None:
        signals = await FreshWalletDetector(FakeWalletAnalyzer()).analyze_batch([])

        assert signals == []


class TestSizeAnomalyBatchContract:
    """The gather/zip/exception contract as seen through ``SizeAnomalyDetector``."""

    async def test_preserves_input_order(self) -> None:
        sync = FakeMetadataSync()
        sync.fallback = market_metadata("science")
        trades = [make_trade("t0"), make_trade("t1"), make_trade("t2")]

        signals = await SizeAnomalyDetector(sync).analyze_batch(trades)

        assert [signal.trade_event.trade_id for signal in signals] == ["t0", "t1", "t2"]

    async def test_drops_none_results(self) -> None:
        sync = FakeMetadataSync()
        sync.fallback = market_metadata("politics")
        trades = [make_trade("a"), make_trade("b")]

        signals = await SizeAnomalyDetector(sync).analyze_batch(trades)

        assert signals == []

    async def test_failed_trade_is_dropped_and_logged(self, caplog) -> None:
        sync = AbortingMetadataSync("market_bad")
        trades = [make_trade("good", market="market_ok"), make_trade("bad", market="market_bad")]

        with caplog.at_level(logging.WARNING):
            caplog.clear()
            signals = await SizeAnomalyDetector(sync).analyze_batch(trades)

        assert [signal.trade_event.trade_id for signal in signals] == ["good"]
        assert "Failed to analyze trade bad" in caplog.text

    async def test_empty_batch_is_empty(self) -> None:
        sync = FakeMetadataSync()

        signals = await SizeAnomalyDetector(sync).analyze_batch([])

        assert signals == []
