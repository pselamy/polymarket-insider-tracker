"""Tests for the SniperDetector module."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

from polymarket_insider_tracker.detector.models import SniperClusterSignal
from polymarket_insider_tracker.detector.sniper import (
    ClusterInfo,
    MarketEntry,
    SniperDetector,
)


def create_mock_trade(
    wallet_address: str,
    market_id: str,
    timestamp: datetime,
    notional_value: Decimal = Decimal("1000"),
) -> MagicMock:
    """Create a mock TradeEvent for testing."""
    trade = MagicMock()
    trade.wallet_address = wallet_address
    trade.market_id = market_id
    trade.timestamp = timestamp
    trade.notional_value = notional_value
    return trade


class TestSniperDetectorInit:
    """Tests for SniperDetector initialization."""

    def test_default_parameters(self) -> None:
        """Test initialization with default parameters."""
        detector = SniperDetector()

        assert detector.entry_threshold_seconds == 300
        assert detector.min_cluster_size == 3
        assert detector.eps == 0.5
        assert detector.min_samples == 2
        assert detector.min_entries_per_wallet == 2

    def test_custom_parameters(self) -> None:
        """Test initialization with custom parameters."""
        detector = SniperDetector(
            entry_threshold_seconds=600,
            min_cluster_size=5,
            eps=0.3,
            min_samples=3,
            min_entries_per_wallet=4,
        )

        assert detector.entry_threshold_seconds == 600
        assert detector.min_cluster_size == 5
        assert detector.eps == 0.3
        assert detector.min_samples == 3
        assert detector.min_entries_per_wallet == 4

    def test_empty_initial_state(self) -> None:
        """Test that detector starts with empty state."""
        detector = SniperDetector()

        assert detector.get_entry_count() == 0
        assert detector.get_wallet_count() == 0
        assert detector.get_cluster_count() == 0


class TestRecordEntry:
    """Tests for record_entry method."""

    def test_records_entry_within_threshold(self) -> None:
        """Test that entries within threshold are recorded."""
        detector = SniperDetector(entry_threshold_seconds=300)
        market_created = datetime.now(UTC)
        trade_time = market_created + timedelta(seconds=60)

        trade = create_mock_trade(
            wallet_address="0x1111111111111111111111111111111111111111",
            market_id="market_001",
            timestamp=trade_time,
        )

        detector.record_entry(trade, market_created)

        assert detector.get_entry_count() == 1
        assert detector.get_wallet_count() == 1

    def test_ignores_entry_after_threshold(self) -> None:
        """Test that entries after threshold are ignored."""
        detector = SniperDetector(entry_threshold_seconds=300)
        market_created = datetime.now(UTC)
        trade_time = market_created + timedelta(seconds=400)  # 400s > 300s threshold

        trade = create_mock_trade(
            wallet_address="0x1111111111111111111111111111111111111111",
            market_id="market_001",
            timestamp=trade_time,
        )

        detector.record_entry(trade, market_created)

        assert detector.get_entry_count() == 0

    def test_ignores_entry_before_market_creation(self) -> None:
        """Test that entries before market creation are ignored."""
        detector = SniperDetector()
        market_created = datetime.now(UTC)
        trade_time = market_created - timedelta(seconds=60)  # Before creation

        trade = create_mock_trade(
            wallet_address="0x1111111111111111111111111111111111111111",
            market_id="market_001",
            timestamp=trade_time,
        )

        detector.record_entry(trade, market_created)

        assert detector.get_entry_count() == 0

    def test_tracks_multiple_wallets(self) -> None:
        """Test tracking entries from multiple wallets."""
        detector = SniperDetector()
        market_created = datetime.now(UTC)

        for i in range(5):
            trade = create_mock_trade(
                wallet_address=f"0x{i:040x}",
                market_id="market_001",
                timestamp=market_created + timedelta(seconds=30 * i),
            )
            detector.record_entry(trade, market_created)

        assert detector.get_entry_count() == 5
        assert detector.get_wallet_count() == 5

    def test_tracks_wallet_across_markets(self) -> None:
        """Test tracking one wallet across multiple markets."""
        detector = SniperDetector()
        wallet = "0x1111111111111111111111111111111111111111"

        for i in range(3):
            market_created = datetime.now(UTC)
            trade = create_mock_trade(
                wallet_address=wallet,
                market_id=f"market_{i:03d}",
                timestamp=market_created + timedelta(seconds=60),
            )
            detector.record_entry(trade, market_created)

        assert detector.get_entry_count() == 3
        assert detector.get_wallet_count() == 1


class TestRunClustering:
    """Tests for run_clustering method."""

    def test_returns_empty_with_insufficient_wallets(self) -> None:
        """Test that clustering returns empty with too few wallets."""
        detector = SniperDetector(min_cluster_size=3)

        # Add entries for only 2 wallets
        for i in range(2):
            market_created = datetime.now(UTC)
            for j in range(3):  # Multiple entries per wallet
                trade = create_mock_trade(
                    wallet_address=f"0x{i:040x}",
                    market_id=f"market_{j:03d}",
                    timestamp=market_created + timedelta(seconds=30),
                )
                detector.record_entry(trade, market_created)

        signals = detector.run_clustering()
        assert signals == []

    def test_returns_empty_with_insufficient_entries_per_wallet(self) -> None:
        """Test that wallets with few entries are excluded."""
        detector = SniperDetector(
            min_cluster_size=2,
            min_entries_per_wallet=3,
        )

        # Add only 2 entries per wallet
        for i in range(5):
            for j in range(2):  # Only 2 entries
                market_created = datetime.now(UTC)
                trade = create_mock_trade(
                    wallet_address=f"0x{i:040x}",
                    market_id=f"market_{j:03d}",
                    timestamp=market_created + timedelta(seconds=30),
                )
                detector.record_entry(trade, market_created)

        signals = detector.run_clustering()
        assert signals == []

    def test_detects_sniper_cluster(self) -> None:
        """Test detection of a cluster of snipers with similar patterns."""
        detector = SniperDetector(
            min_cluster_size=3,
            min_entries_per_wallet=2,
            eps=1.0,  # Larger eps for easier clustering
            min_samples=2,
        )

        # Create 5 wallets that all enter markets within 30 seconds
        wallets = [f"0x{i:040x}" for i in range(5)]
        markets = ["market_001", "market_002", "market_003"]

        for market in markets:
            market_created = datetime.now(UTC)
            for wallet in wallets:
                trade = create_mock_trade(
                    wallet_address=wallet,
                    market_id=market,
                    timestamp=market_created + timedelta(seconds=30),
                    notional_value=Decimal("1000"),
                )
                detector.record_entry(trade, market_created)

        signals = detector.run_clustering()

        # Should detect at least one cluster
        assert len(signals) > 0
        assert all(isinstance(s, SniperClusterSignal) for s in signals)

    def test_does_not_duplicate_signals(self) -> None:
        """Test that same wallet isn't signaled twice."""
        detector = SniperDetector(
            min_cluster_size=3,
            min_entries_per_wallet=2,
            eps=1.0,
            min_samples=2,
        )

        # Create cluster
        wallets = [f"0x{i:040x}" for i in range(5)]
        markets = ["market_001", "market_002"]

        for market in markets:
            market_created = datetime.now(UTC)
            for wallet in wallets:
                trade = create_mock_trade(
                    wallet_address=wallet,
                    market_id=market,
                    timestamp=market_created + timedelta(seconds=30),
                )
                detector.record_entry(trade, market_created)

        # Run clustering twice
        signals1 = detector.run_clustering()
        signals2 = detector.run_clustering()

        # Second run should return empty (no new signals)
        assert len(signals2) == 0
        # All wallets from signals1 should be unique
        signaled_wallets = [s.wallet_address for s in signals1]
        assert len(signaled_wallets) == len(set(signaled_wallets))


class TestIsSniper:
    """Tests for is_sniper method."""

    def test_returns_false_for_unknown_wallet(self) -> None:
        """Test that unknown wallets return False."""
        detector = SniperDetector()
        assert detector.is_sniper("0x1111111111111111111111111111111111111111") is False

    def test_returns_true_for_cluster_member(self) -> None:
        """Test that cluster members return True."""
        detector = SniperDetector(
            min_cluster_size=3,
            min_entries_per_wallet=2,
            eps=1.0,
            min_samples=2,
        )

        wallets = [f"0x{i:040x}" for i in range(5)]
        for market in ["market_001", "market_002"]:
            market_created = datetime.now(UTC)
            for wallet in wallets:
                trade = create_mock_trade(
                    wallet_address=wallet,
                    market_id=market,
                    timestamp=market_created + timedelta(seconds=30),
                )
                detector.record_entry(trade, market_created)

        signals = detector.run_clustering()

        # All signaled wallets should be snipers
        for signal in signals:
            assert detector.is_sniper(signal.wallet_address) is True


class TestGetClusterForWallet:
    """Tests for get_cluster_for_wallet method."""

    def test_returns_none_for_unknown_wallet(self) -> None:
        """Test that unknown wallets return None."""
        detector = SniperDetector()
        result = detector.get_cluster_for_wallet("0x1111111111111111111111111111111111111111")
        assert result is None

    def test_returns_cluster_info_for_member(self) -> None:
        """Test that cluster members get ClusterInfo."""
        detector = SniperDetector(
            min_cluster_size=3,
            min_entries_per_wallet=2,
            eps=1.0,
            min_samples=2,
        )

        wallets = [f"0x{i:040x}" for i in range(5)]
        for market in ["market_001", "market_002"]:
            market_created = datetime.now(UTC)
            for wallet in wallets:
                trade = create_mock_trade(
                    wallet_address=wallet,
                    market_id=market,
                    timestamp=market_created + timedelta(seconds=30),
                )
                detector.record_entry(trade, market_created)

        signals = detector.run_clustering()

        if signals:
            wallet = signals[0].wallet_address
            cluster = detector.get_cluster_for_wallet(wallet)
            assert cluster is not None
            assert isinstance(cluster, ClusterInfo)
            assert wallet in cluster.wallet_addresses


class TestClearEntries:
    """Tests for clear_entries method."""

    def test_clears_all_entries(self) -> None:
        """Test that clear removes all entries."""
        detector = SniperDetector()
        market_created = datetime.now(UTC)

        for i in range(5):
            trade = create_mock_trade(
                wallet_address=f"0x{i:040x}",
                market_id="market_001",
                timestamp=market_created + timedelta(seconds=30),
            )
            detector.record_entry(trade, market_created)

        assert detector.get_entry_count() == 5

        detector.clear_entries()

        assert detector.get_entry_count() == 0
        assert detector.get_wallet_count() == 0


class TestMarketEntryDataclass:
    """Tests for MarketEntry dataclass."""

    def test_creation(self) -> None:
        """Test MarketEntry creation."""
        entry = MarketEntry(
            wallet_address="0x1111",
            market_id="market_001",
            entry_delta_seconds=30.5,
            position_size=Decimal("1000"),
            timestamp=datetime.now(UTC),
        )

        assert entry.wallet_address == "0x1111"
        assert entry.market_id == "market_001"
        assert entry.entry_delta_seconds == 30.5
        assert entry.position_size == Decimal("1000")


class TestClusterInfoDataclass:
    """Tests for ClusterInfo dataclass."""

    def test_creation(self) -> None:
        """Test ClusterInfo creation."""
        wallets = {"0x1111", "0x2222", "0x3333"}
        cluster = ClusterInfo(
            cluster_id="cluster_001",
            wallet_addresses=wallets,
            avg_entry_delta=45.0,
            markets_in_common=3,
        )

        assert cluster.cluster_id == "cluster_001"
        assert cluster.wallet_addresses == wallets
        assert cluster.avg_entry_delta == 45.0
        assert cluster.markets_in_common == 3
        assert cluster.created_at is not None


class TestSniperClusterSignalModel:
    """Tests for SniperClusterSignal dataclass."""

    def test_creation(self) -> None:
        """Test SniperClusterSignal creation."""
        signal = SniperClusterSignal(
            wallet_address="0x1111",
            cluster_id="cluster_001",
            cluster_size=5,
            avg_entry_delta_seconds=30.0,
            markets_in_common=3,
            confidence=0.85,
        )

        assert signal.wallet_address == "0x1111"
        assert signal.cluster_id == "cluster_001"
        assert signal.cluster_size == 5
        assert signal.avg_entry_delta_seconds == 30.0
        assert signal.markets_in_common == 3
        assert signal.confidence == 0.85

    def test_is_high_confidence(self) -> None:
        """Test is_high_confidence property."""
        high = SniperClusterSignal(
            wallet_address="0x1111",
            cluster_id="c1",
            cluster_size=5,
            avg_entry_delta_seconds=30.0,
            markets_in_common=3,
            confidence=0.75,
        )
        low = SniperClusterSignal(
            wallet_address="0x2222",
            cluster_id="c2",
            cluster_size=5,
            avg_entry_delta_seconds=30.0,
            markets_in_common=3,
            confidence=0.65,
        )

        assert high.is_high_confidence is True
        assert low.is_high_confidence is False

    def test_is_very_high_confidence(self) -> None:
        """Test is_very_high_confidence property."""
        very_high = SniperClusterSignal(
            wallet_address="0x1111",
            cluster_id="c1",
            cluster_size=5,
            avg_entry_delta_seconds=30.0,
            markets_in_common=3,
            confidence=0.90,
        )
        high = SniperClusterSignal(
            wallet_address="0x2222",
            cluster_id="c2",
            cluster_size=5,
            avg_entry_delta_seconds=30.0,
            markets_in_common=3,
            confidence=0.80,
        )

        assert very_high.is_very_high_confidence is True
        assert high.is_very_high_confidence is False

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""
        signal = SniperClusterSignal(
            wallet_address="0x1111",
            cluster_id="cluster_001",
            cluster_size=5,
            avg_entry_delta_seconds=30.0,
            markets_in_common=3,
            confidence=0.85,
        )

        result = signal.to_dict()

        assert result["wallet_address"] == "0x1111"
        assert result["cluster_id"] == "cluster_001"
        assert result["cluster_size"] == 5
        assert result["avg_entry_delta_seconds"] == 30.0
        assert result["markets_in_common"] == 3
        assert result["confidence"] == 0.85
        assert "timestamp" in result


class TestConfidenceCalculation:
    """Tests for confidence calculation logic."""

    def test_higher_confidence_with_larger_cluster(self) -> None:
        """Test that larger clusters get higher confidence."""
        detector = SniperDetector(
            min_cluster_size=2,
            min_entries_per_wallet=2,
            eps=1.0,
            min_samples=2,
        )

        # Create a cluster
        stats = {
            "avg_delta": 30.0,
            "markets_in_common": 3,
        }

        small_cluster = {"0x1", "0x2", "0x3"}
        large_cluster = {"0x1", "0x2", "0x3", "0x4", "0x5", "0x6", "0x7", "0x8"}

        small_conf = detector._calculate_confidence(small_cluster, stats)
        large_conf = detector._calculate_confidence(large_cluster, stats)

        assert large_conf > small_conf

    def test_higher_confidence_with_faster_entries(self) -> None:
        """Test that faster entries get higher confidence."""
        detector = SniperDetector()

        cluster = {"0x1", "0x2", "0x3"}

        fast_stats = {"avg_delta": 10.0, "markets_in_common": 3}
        slow_stats = {"avg_delta": 200.0, "markets_in_common": 3}

        fast_conf = detector._calculate_confidence(cluster, fast_stats)
        slow_conf = detector._calculate_confidence(cluster, slow_stats)

        assert fast_conf > slow_conf

    def test_higher_confidence_with_more_overlap(self) -> None:
        """Test that more market overlap gets higher confidence."""
        detector = SniperDetector()

        cluster = {"0x1", "0x2", "0x3"}

        high_overlap = {"avg_delta": 30.0, "markets_in_common": 5}
        low_overlap = {"avg_delta": 30.0, "markets_in_common": 1}

        high_conf = detector._calculate_confidence(cluster, high_overlap)
        low_conf = detector._calculate_confidence(cluster, low_overlap)

        assert high_conf > low_conf


class TestIntegration:
    """Integration tests for the sniper detection workflow."""

    def test_end_to_end_sniper_detection(self) -> None:
        """Test complete workflow from entry recording to signal generation."""
        detector = SniperDetector(
            entry_threshold_seconds=300,
            min_cluster_size=3,
            min_entries_per_wallet=2,
            eps=1.0,
            min_samples=2,
        )

        # Simulate 5 snipers hitting 3 markets in rapid succession
        sniper_wallets = [f"0x{'a' * 38}{i:02d}" for i in range(5)]
        markets = ["market_001", "market_002", "market_003"]

        for market in markets:
            market_created = datetime.now(UTC)

            for i, wallet in enumerate(sniper_wallets):
                # Each sniper enters within 10-60 seconds
                trade = create_mock_trade(
                    wallet_address=wallet,
                    market_id=market,
                    timestamp=market_created + timedelta(seconds=10 + i * 10),
                    notional_value=Decimal("5000"),
                )
                detector.record_entry(trade, market_created)

        # Also add some normal traders (enter after threshold)
        for market in markets:
            market_created = datetime.now(UTC)
            for i in range(3):
                trade = create_mock_trade(
                    wallet_address=f"0x{'b' * 38}{i:02d}",
                    market_id=market,
                    timestamp=market_created + timedelta(seconds=400),  # After threshold
                )
                detector.record_entry(trade, market_created)

        # Run clustering
        signals = detector.run_clustering()

        # Should detect the sniper cluster
        assert len(signals) > 0

        # All signals should be from sniper wallets
        for signal in signals:
            assert signal.wallet_address in [w.lower() for w in sniper_wallets]
            assert signal.cluster_size >= 3
            assert signal.confidence > 0

        # Verify snipers are marked
        for wallet in sniper_wallets:
            # May or may not be in cluster depending on clustering
            if detector.is_sniper(wallet.lower()):
                cluster = detector.get_cluster_for_wallet(wallet)
                assert cluster is not None
