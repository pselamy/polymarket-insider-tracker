"""Tests for the profiler data models."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polymarket_insider_tracker.profiler.models import Transaction, WalletInfo, WalletProfile


class TestTransaction:
    """Tests for the Transaction dataclass."""

    @pytest.fixture
    def sample_transaction(self) -> Transaction:
        """Create a sample transaction."""
        return Transaction(
            hash="0xabc123",
            block_number=50000000,
            timestamp=datetime(2026, 1, 4, 12, 0, 0, tzinfo=UTC),
            from_address="0xsender",
            to_address="0xreceiver",
            value=Decimal("1000000000000000000"),  # 1 MATIC
            gas_used=21000,
            gas_price=Decimal("50000000000"),  # 50 Gwei
        )

    def test_transaction_creation(self, sample_transaction: Transaction) -> None:
        """Test creating a transaction."""
        assert sample_transaction.hash == "0xabc123"
        assert sample_transaction.block_number == 50000000
        assert sample_transaction.from_address == "0xsender"
        assert sample_transaction.to_address == "0xreceiver"

    def test_value_matic(self, sample_transaction: Transaction) -> None:
        """Test value in MATIC conversion."""
        assert sample_transaction.value_matic == Decimal("1")

    def test_value_matic_fractional(self) -> None:
        """Test fractional MATIC value."""
        tx = Transaction(
            hash="0x123",
            block_number=1,
            timestamp=datetime.now(UTC),
            from_address="0x1",
            to_address="0x2",
            value=Decimal("500000000000000000"),  # 0.5 MATIC
            gas_used=21000,
            gas_price=Decimal("50000000000"),
        )

        assert tx.value_matic == Decimal("0.5")

    def test_gas_cost_wei(self, sample_transaction: Transaction) -> None:
        """Test gas cost in Wei."""
        expected = 21000 * 50000000000
        assert sample_transaction.gas_cost_wei == Decimal(expected)

    def test_gas_cost_matic(self, sample_transaction: Transaction) -> None:
        """Test gas cost in MATIC."""
        # 21000 * 50 Gwei = 1050000 Gwei = 0.00105 MATIC
        expected = Decimal("21000") * Decimal("50000000000") / Decimal("1000000000000000000")
        assert sample_transaction.gas_cost_matic == expected

    def test_transaction_frozen(self, sample_transaction: Transaction) -> None:
        """Test that transaction is immutable."""
        with pytest.raises(AttributeError):
            sample_transaction.hash = "0xnew"  # type: ignore[misc]

    def test_transaction_no_recipient(self) -> None:
        """Test transaction with no recipient (contract creation)."""
        tx = Transaction(
            hash="0x123",
            block_number=1,
            timestamp=datetime.now(UTC),
            from_address="0x1",
            to_address=None,
            value=Decimal("0"),
            gas_used=100000,
            gas_price=Decimal("50000000000"),
        )

        assert tx.to_address is None


class TestWalletInfo:
    """Tests for the WalletInfo dataclass."""

    @pytest.fixture
    def sample_wallet(self) -> WalletInfo:
        """Create a sample wallet info."""
        return WalletInfo(
            address="0xwallet123",
            transaction_count=100,
            balance_wei=Decimal("5000000000000000000"),  # 5 MATIC
            first_transaction=None,
        )

    @pytest.fixture
    def wallet_with_transaction(self) -> WalletInfo:
        """Create a wallet with first transaction."""
        first_tx = Transaction(
            hash="0xfirst",
            block_number=1000000,
            timestamp=datetime.now(UTC) - timedelta(days=365),
            from_address="0xfaucet",
            to_address="0xwallet123",
            value=Decimal("1000000000000000000"),
            gas_used=21000,
            gas_price=Decimal("50000000000"),
        )
        return WalletInfo(
            address="0xwallet123",
            transaction_count=100,
            balance_wei=Decimal("5000000000000000000"),
            first_transaction=first_tx,
        )

    def test_wallet_creation(self, sample_wallet: WalletInfo) -> None:
        """Test creating wallet info."""
        assert sample_wallet.address == "0xwallet123"
        assert sample_wallet.transaction_count == 100
        assert sample_wallet.balance_wei == Decimal("5000000000000000000")

    def test_balance_matic(self, sample_wallet: WalletInfo) -> None:
        """Test balance in MATIC conversion."""
        assert sample_wallet.balance_matic == Decimal("5")

    def test_is_fresh_false(self, sample_wallet: WalletInfo) -> None:
        """Test that wallet with many transactions is not fresh."""
        assert sample_wallet.is_fresh is False

    def test_is_fresh_true(self) -> None:
        """Test that wallet with few transactions is fresh."""
        wallet = WalletInfo(
            address="0xnewwallet",
            transaction_count=5,
            balance_wei=Decimal("1000000000000000000"),
        )

        assert wallet.is_fresh is True

    def test_is_fresh_boundary(self) -> None:
        """Test fresh wallet boundary (10 transactions)."""
        wallet_9 = WalletInfo(
            address="0x1",
            transaction_count=9,
            balance_wei=Decimal("0"),
        )
        wallet_10 = WalletInfo(
            address="0x2",
            transaction_count=10,
            balance_wei=Decimal("0"),
        )

        assert wallet_9.is_fresh is True
        assert wallet_10.is_fresh is False

    def test_wallet_age_days_no_transaction(self, sample_wallet: WalletInfo) -> None:
        """Test wallet age when no first transaction."""
        assert sample_wallet.wallet_age_days is None

    def test_wallet_age_days_with_transaction(self, wallet_with_transaction: WalletInfo) -> None:
        """Test wallet age calculation."""
        age = wallet_with_transaction.wallet_age_days

        assert age is not None
        # Should be approximately 365 days
        assert 364 < age < 366

    def test_wallet_frozen(self, sample_wallet: WalletInfo) -> None:
        """Test that wallet info is immutable."""
        with pytest.raises(AttributeError):
            sample_wallet.address = "0xnew"  # type: ignore[misc]

    def test_wallet_zero_balance(self) -> None:
        """Test wallet with zero balance."""
        wallet = WalletInfo(
            address="0xempty",
            transaction_count=0,
            balance_wei=Decimal("0"),
        )

        assert wallet.balance_matic == Decimal("0")
        assert wallet.is_fresh is True

    def test_wallet_very_small_balance(self) -> None:
        """Test wallet with very small balance."""
        wallet = WalletInfo(
            address="0xdust",
            transaction_count=1,
            balance_wei=Decimal("1"),  # 1 Wei
        )

        # Should be a very small fraction
        expected = Decimal("1") / Decimal("1000000000000000000")
        assert wallet.balance_matic == expected


class TestTransactionEquality:
    """Tests for transaction equality and hashing."""

    def test_equal_transactions(self) -> None:
        """Test that identical transactions are equal."""
        tx1 = Transaction(
            hash="0xabc",
            block_number=1,
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            from_address="0x1",
            to_address="0x2",
            value=Decimal("1000"),
            gas_used=21000,
            gas_price=Decimal("50"),
        )
        tx2 = Transaction(
            hash="0xabc",
            block_number=1,
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            from_address="0x1",
            to_address="0x2",
            value=Decimal("1000"),
            gas_used=21000,
            gas_price=Decimal("50"),
        )

        assert tx1 == tx2

    def test_different_transactions(self) -> None:
        """Test that different transactions are not equal."""
        tx1 = Transaction(
            hash="0xabc",
            block_number=1,
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            from_address="0x1",
            to_address="0x2",
            value=Decimal("1000"),
            gas_used=21000,
            gas_price=Decimal("50"),
        )
        tx2 = Transaction(
            hash="0xdef",  # Different hash
            block_number=1,
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            from_address="0x1",
            to_address="0x2",
            value=Decimal("1000"),
            gas_used=21000,
            gas_price=Decimal("50"),
        )

        assert tx1 != tx2

    def test_transaction_hashable(self) -> None:
        """Test that transactions can be used in sets."""
        tx = Transaction(
            hash="0xabc",
            block_number=1,
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            from_address="0x1",
            to_address="0x2",
            value=Decimal("1000"),
            gas_used=21000,
            gas_price=Decimal("50"),
        )

        tx_set = {tx}
        assert tx in tx_set


class TestWalletProfile:
    """Tests for the WalletProfile dataclass."""

    @pytest.fixture
    def fresh_profile(self) -> WalletProfile:
        """Create a fresh wallet profile."""
        return WalletProfile(
            address="0xfresh",
            nonce=2,
            first_seen=datetime.now(UTC) - timedelta(hours=6),
            age_hours=6.0,
            is_fresh=True,
            total_tx_count=2,
            matic_balance=Decimal("1000000000000000000"),  # 1 MATIC
            usdc_balance=Decimal("1000000"),  # 1 USDC
            fresh_threshold=5,
        )

    @pytest.fixture
    def old_profile(self) -> WalletProfile:
        """Create an old wallet profile."""
        return WalletProfile(
            address="0xold",
            nonce=500,
            first_seen=datetime.now(UTC) - timedelta(days=365),
            age_hours=365 * 24,
            is_fresh=False,
            total_tx_count=500,
            matic_balance=Decimal("100000000000000000000"),  # 100 MATIC
            usdc_balance=Decimal("10000000000"),  # 10000 USDC
            fresh_threshold=5,
        )

    def test_profile_creation(self, fresh_profile: WalletProfile) -> None:
        """Test creating a wallet profile."""
        assert fresh_profile.address == "0xfresh"
        assert fresh_profile.nonce == 2
        assert fresh_profile.is_fresh is True
        assert fresh_profile.age_hours == 6.0

    def test_age_days(self, fresh_profile: WalletProfile) -> None:
        """Test age_days property."""
        assert fresh_profile.age_days == 0.25  # 6 hours = 0.25 days

    def test_age_days_none(self) -> None:
        """Test age_days when age_hours is None."""
        profile = WalletProfile(
            address="0xnew",
            nonce=0,
            first_seen=None,
            age_hours=None,
            is_fresh=True,
            total_tx_count=0,
            matic_balance=Decimal("0"),
            usdc_balance=Decimal("0"),
        )
        assert profile.age_days is None

    def test_matic_balance_formatted(self, fresh_profile: WalletProfile) -> None:
        """Test MATIC balance formatting."""
        assert fresh_profile.matic_balance_formatted == Decimal("1")

    def test_usdc_balance_formatted(self, fresh_profile: WalletProfile) -> None:
        """Test USDC balance formatting."""
        assert fresh_profile.usdc_balance_formatted == Decimal("1")

    def test_is_brand_new(self) -> None:
        """Test is_brand_new property."""
        brand_new = WalletProfile(
            address="0xnew",
            nonce=0,
            first_seen=None,
            age_hours=None,
            is_fresh=True,
            total_tx_count=0,
            matic_balance=Decimal("0"),
            usdc_balance=Decimal("0"),
        )
        assert brand_new.is_brand_new is True

        not_brand_new = WalletProfile(
            address="0xold",
            nonce=1,
            first_seen=datetime.now(UTC),
            age_hours=1.0,
            is_fresh=True,
            total_tx_count=1,
            matic_balance=Decimal("0"),
            usdc_balance=Decimal("0"),
        )
        assert not_brand_new.is_brand_new is False

    def test_freshness_score_brand_new(self) -> None:
        """Test freshness score for brand new wallet."""
        profile = WalletProfile(
            address="0xnew",
            nonce=0,
            first_seen=None,
            age_hours=None,
            is_fresh=True,
            total_tx_count=0,
            matic_balance=Decimal("0"),
            usdc_balance=Decimal("0"),
            fresh_threshold=5,
        )
        # nonce_score = 1.0 (0/5 = 0, 1-0 = 1)
        # age_score = 1.0 (None = assumed new)
        # score = 0.6 * 1.0 + 0.4 * 1.0 = 1.0
        assert profile.freshness_score == 1.0

    def test_freshness_score_old_wallet(self, old_profile: WalletProfile) -> None:
        """Test freshness score for old wallet."""
        # nonce_score = max(0, 1 - 500/5) = 0
        # age_score = max(0, 1 - 8760/48) = 0
        # score = 0
        assert old_profile.freshness_score == 0.0

    def test_freshness_score_moderate(self) -> None:
        """Test freshness score for moderately fresh wallet."""
        profile = WalletProfile(
            address="0xmoderate",
            nonce=2,
            first_seen=datetime.now(UTC) - timedelta(hours=24),
            age_hours=24.0,
            is_fresh=True,
            total_tx_count=2,
            matic_balance=Decimal("0"),
            usdc_balance=Decimal("0"),
            fresh_threshold=5,
        )
        # nonce_score = 1 - 2/5 = 0.6
        # age_score = 1 - 24/48 = 0.5
        # score = 0.6 * 0.6 + 0.4 * 0.5 = 0.36 + 0.2 = 0.56
        assert profile.freshness_score == pytest.approx(0.56)

    def test_profile_frozen(self, fresh_profile: WalletProfile) -> None:
        """Test that wallet profile is immutable."""
        with pytest.raises(AttributeError):
            fresh_profile.nonce = 100  # type: ignore[misc]

    def test_analyzed_at_default(self) -> None:
        """Test that analyzed_at has a default."""
        before = datetime.now(UTC)
        profile = WalletProfile(
            address="0x1",
            nonce=0,
            first_seen=None,
            age_hours=None,
            is_fresh=True,
            total_tx_count=0,
            matic_balance=Decimal("0"),
            usdc_balance=Decimal("0"),
        )
        after = datetime.now(UTC)

        assert before <= profile.analyzed_at <= after
