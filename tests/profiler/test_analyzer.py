"""Tests for the wallet analyzer."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from polymarket_insider_tracker.profiler.analyzer import (
    DEFAULT_FRESH_THRESHOLD,
    USDC_POLYGON_ADDRESS,
    WalletAnalyzer,
)
from polymarket_insider_tracker.profiler.models import Transaction, WalletInfo

# Valid Ethereum addresses for testing
VALID_ADDRESS = "0x742d35Cc6634C0532925a3b844Bc9e7595f5eaE2"
VALID_ADDRESS_2 = "0x8ba1f109551bD432803012645Ac136ddd64DBA72"


class TestWalletAnalyzerInit:
    """Tests for WalletAnalyzer initialization."""

    def test_init_default(self) -> None:
        """Test initialization with defaults."""
        client = AsyncMock()
        analyzer = WalletAnalyzer(client)

        assert analyzer._client is client
        assert analyzer._redis is None
        assert analyzer._fresh_threshold == DEFAULT_FRESH_THRESHOLD
        assert analyzer._usdc_address == USDC_POLYGON_ADDRESS

    def test_init_with_redis(self) -> None:
        """Test initialization with Redis."""
        client = AsyncMock()
        redis = AsyncMock()
        analyzer = WalletAnalyzer(client, redis=redis)

        assert analyzer._redis is redis

    def test_init_custom_threshold(self) -> None:
        """Test initialization with custom threshold."""
        client = AsyncMock()
        analyzer = WalletAnalyzer(client, fresh_threshold=10)

        assert analyzer._fresh_threshold == 10


class TestWalletAnalyzerAnalyze:
    """Tests for the analyze method."""

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        """Create a mock PolygonClient."""
        client = AsyncMock()
        client.get_wallet_info = AsyncMock()
        client.get_token_balance = AsyncMock(return_value=Decimal("1000000"))
        return client

    @pytest.fixture
    def mock_redis(self) -> AsyncMock:
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock()
        return redis

    @pytest.mark.asyncio
    async def test_analyze_fresh_wallet(self, mock_client: AsyncMock) -> None:
        """Test analyzing a fresh wallet."""
        first_tx = Transaction(
            hash="0xabc",
            block_number=1000,
            timestamp=datetime.now(UTC) - timedelta(hours=12),
            from_address="0xfaucet",
            to_address=VALID_ADDRESS.lower(),
            value=Decimal("1000000000000000000"),
            gas_used=21000,
            gas_price=Decimal("50000000000"),
        )
        mock_client.get_wallet_info.return_value = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=3,
            balance_wei=Decimal("5000000000000000000"),
            first_transaction=first_tx,
        )

        analyzer = WalletAnalyzer(mock_client)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.address == VALID_ADDRESS.lower()
        assert profile.nonce == 3
        assert profile.is_fresh is True
        assert profile.first_seen is not None
        assert profile.age_hours is not None
        assert 11 < profile.age_hours < 13

    @pytest.mark.asyncio
    async def test_analyze_old_wallet(self, mock_client: AsyncMock) -> None:
        """Test analyzing an old wallet with many transactions."""
        first_tx = Transaction(
            hash="0xabc",
            block_number=1000,
            timestamp=datetime.now(UTC) - timedelta(days=365),
            from_address="0xfaucet",
            to_address=VALID_ADDRESS.lower(),
            value=Decimal("1000000000000000000"),
            gas_used=21000,
            gas_price=Decimal("50000000000"),
        )
        mock_client.get_wallet_info.return_value = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=500,
            balance_wei=Decimal("100000000000000000000"),
            first_transaction=first_tx,
        )

        analyzer = WalletAnalyzer(mock_client)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.nonce == 500
        assert profile.is_fresh is False
        assert profile.age_hours is not None
        assert profile.age_hours > 8000  # Over 365 days in hours

    @pytest.mark.asyncio
    async def test_analyze_brand_new_wallet(self, mock_client: AsyncMock) -> None:
        """Test analyzing a wallet with no transactions."""
        mock_client.get_wallet_info.return_value = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=0,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=None,
        )

        analyzer = WalletAnalyzer(mock_client)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.nonce == 0
        assert profile.is_fresh is True
        assert profile.is_brand_new is True
        assert profile.first_seen is None
        assert profile.age_hours is None

    @pytest.mark.asyncio
    async def test_analyze_uses_cache(self, mock_client: AsyncMock, mock_redis: AsyncMock) -> None:
        """Test that analyze uses cached data."""
        cached_data = {
            "address": VALID_ADDRESS.lower(),
            "nonce": 2,
            "first_seen": datetime.now(UTC).isoformat(),
            "age_hours": 6.0,
            "is_fresh": True,
            "total_tx_count": 2,
            "matic_balance": "1000000000000000000",
            "usdc_balance": "500000",
            "analyzed_at": datetime.now(UTC).isoformat(),
            "fresh_threshold": 5,
        }
        mock_redis.get = AsyncMock(return_value=str(cached_data).replace("'", '"').encode())

        # Actually mock it properly with json
        import json

        mock_redis.get = AsyncMock(return_value=json.dumps(cached_data).encode())

        analyzer = WalletAnalyzer(mock_client, redis=mock_redis)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.address == VALID_ADDRESS.lower()
        assert profile.nonce == 2
        mock_client.get_wallet_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_analyze_force_refresh(
        self, mock_client: AsyncMock, mock_redis: AsyncMock
    ) -> None:
        """Test that force_refresh bypasses cache."""
        import json

        cached_data = {
            "address": VALID_ADDRESS.lower(),
            "nonce": 1,
            "first_seen": None,
            "age_hours": None,
            "is_fresh": True,
            "total_tx_count": 1,
            "matic_balance": "1000",
            "usdc_balance": "0",
            "analyzed_at": datetime.now(UTC).isoformat(),
            "fresh_threshold": 5,
        }
        mock_redis.get = AsyncMock(return_value=json.dumps(cached_data).encode())

        mock_client.get_wallet_info.return_value = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=10,
            balance_wei=Decimal("5000000000000000000"),
            first_transaction=None,
        )

        analyzer = WalletAnalyzer(mock_client, redis=mock_redis)
        profile = await analyzer.analyze(VALID_ADDRESS, force_refresh=True)

        assert profile.nonce == 10  # From fresh query, not cache
        mock_client.get_wallet_info.assert_called_once()

    @pytest.mark.asyncio
    async def test_analyze_handles_usdc_error(self, mock_client: AsyncMock) -> None:
        """Test that USDC balance error is handled gracefully."""
        mock_client.get_wallet_info.return_value = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=1,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=None,
        )
        mock_client.get_token_balance.side_effect = Exception("Token query failed")

        analyzer = WalletAnalyzer(mock_client)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.usdc_balance == Decimal(0)


class TestWalletAnalyzerIsFresh:
    """Tests for the is_fresh method."""

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        """Create a mock PolygonClient."""
        client = AsyncMock()
        client.get_wallet_info = AsyncMock()
        client.get_token_balance = AsyncMock(return_value=Decimal("0"))
        return client

    @pytest.mark.asyncio
    async def test_is_fresh_true(self, mock_client: AsyncMock) -> None:
        """Test is_fresh returns True for fresh wallet."""
        mock_client.get_wallet_info.return_value = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=2,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=None,
        )

        analyzer = WalletAnalyzer(mock_client)
        result = await analyzer.is_fresh(VALID_ADDRESS)

        assert result is True

    @pytest.mark.asyncio
    async def test_is_fresh_false(self, mock_client: AsyncMock) -> None:
        """Test is_fresh returns False for old wallet."""
        mock_client.get_wallet_info.return_value = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=100,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=None,
        )

        analyzer = WalletAnalyzer(mock_client)
        result = await analyzer.is_fresh(VALID_ADDRESS)

        assert result is False


class TestWalletAnalyzerFreshnessLogic:
    """Tests for freshness determination logic."""

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        """Create a mock PolygonClient."""
        return AsyncMock()

    def test_is_wallet_fresh_low_nonce_no_age(self, mock_client: AsyncMock) -> None:
        """Test fresh wallet with low nonce and unknown age."""
        analyzer = WalletAnalyzer(mock_client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=2, age_hours=None)
        assert result is True

    def test_is_wallet_fresh_low_nonce_young_age(self, mock_client: AsyncMock) -> None:
        """Test fresh wallet with low nonce and young age."""
        analyzer = WalletAnalyzer(mock_client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=2, age_hours=12.0)
        assert result is True

    def test_is_wallet_fresh_low_nonce_old_age(self, mock_client: AsyncMock) -> None:
        """Test not fresh when nonce is low but age is old."""
        analyzer = WalletAnalyzer(mock_client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=2, age_hours=100.0)
        assert result is False

    def test_is_wallet_fresh_high_nonce(self, mock_client: AsyncMock) -> None:
        """Test not fresh when nonce is high."""
        analyzer = WalletAnalyzer(mock_client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=10, age_hours=12.0)
        assert result is False

    def test_is_wallet_fresh_at_threshold(self, mock_client: AsyncMock) -> None:
        """Test not fresh when nonce equals threshold."""
        analyzer = WalletAnalyzer(mock_client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=5, age_hours=12.0)
        assert result is False

    def test_is_wallet_fresh_at_age_boundary(self, mock_client: AsyncMock) -> None:
        """Test at 48 hour boundary."""
        analyzer = WalletAnalyzer(mock_client, fresh_threshold=5)

        result_under = analyzer._is_wallet_fresh(nonce=2, age_hours=47.9)
        assert result_under is True

        result_over = analyzer._is_wallet_fresh(nonce=2, age_hours=48.1)
        assert result_over is False


class TestWalletAnalyzerBatch:
    """Tests for batch analysis."""

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        """Create a mock PolygonClient."""
        client = AsyncMock()
        client.get_wallet_info = AsyncMock()
        client.get_token_balance = AsyncMock(return_value=Decimal("0"))
        return client

    @pytest.mark.asyncio
    async def test_analyze_batch(self, mock_client: AsyncMock) -> None:
        """Test batch analysis."""
        mock_client.get_wallet_info.side_effect = [
            WalletInfo(
                address=VALID_ADDRESS.lower(),
                transaction_count=2,
                balance_wei=Decimal("1000"),
                first_transaction=None,
            ),
            WalletInfo(
                address=VALID_ADDRESS_2.lower(),
                transaction_count=100,
                balance_wei=Decimal("2000"),
                first_transaction=None,
            ),
        ]

        analyzer = WalletAnalyzer(mock_client)
        profiles = await analyzer.analyze_batch([VALID_ADDRESS, VALID_ADDRESS_2])

        assert len(profiles) == 2
        assert profiles[VALID_ADDRESS.lower()].is_fresh is True
        assert profiles[VALID_ADDRESS_2.lower()].is_fresh is False

    @pytest.mark.asyncio
    async def test_analyze_batch_handles_errors(self, mock_client: AsyncMock) -> None:
        """Test batch analysis handles individual failures."""
        mock_client.get_wallet_info.side_effect = [
            WalletInfo(
                address=VALID_ADDRESS.lower(),
                transaction_count=2,
                balance_wei=Decimal("1000"),
                first_transaction=None,
            ),
            Exception("RPC error"),
        ]

        analyzer = WalletAnalyzer(mock_client)
        profiles = await analyzer.analyze_batch([VALID_ADDRESS, VALID_ADDRESS_2])

        assert len(profiles) == 1
        assert VALID_ADDRESS.lower() in profiles

    @pytest.mark.asyncio
    async def test_get_fresh_wallets(self, mock_client: AsyncMock) -> None:
        """Test filtering to only fresh wallets."""
        mock_client.get_wallet_info.side_effect = [
            WalletInfo(
                address=VALID_ADDRESS.lower(),
                transaction_count=2,
                balance_wei=Decimal("1000"),
                first_transaction=None,
            ),
            WalletInfo(
                address=VALID_ADDRESS_2.lower(),
                transaction_count=100,
                balance_wei=Decimal("2000"),
                first_transaction=None,
            ),
        ]

        analyzer = WalletAnalyzer(mock_client)
        fresh = await analyzer.get_fresh_wallets([VALID_ADDRESS, VALID_ADDRESS_2])

        assert len(fresh) == 1
        assert VALID_ADDRESS.lower() in fresh
