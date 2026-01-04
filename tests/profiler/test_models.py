"""Tests for the profiler data models."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polymarket_insider_tracker.profiler.models import Transaction, WalletInfo


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

    def test_wallet_age_days_with_transaction(
        self, wallet_with_transaction: WalletInfo
    ) -> None:
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
