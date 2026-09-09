"""Tests for the wallet analyzer."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.profiler.analyzer import (
    DEFAULT_FRESH_THRESHOLD,
    USDC_POLYGON_ADDRESS,
    WalletAnalyzer,
)
from polymarket_insider_tracker.profiler.models import Transaction, WalletInfo

# Valid Ethereum addresses for testing
VALID_ADDRESS = "0x742d35Cc6634C0532925a3b844Bc9e7595f5eaE2"
VALID_ADDRESS_2 = "0x8ba1f109551bD432803012645Ac136ddd64DBA72"


class FakePolygonClient:
    """Fake Polygon client for wallet analyzer tests."""

    def __init__(
        self,
        wallet_infos: dict[str, WalletInfo] | None = None,
        token_balance: Decimal = Decimal("1000000"),
        raise_token_error: Exception | None = None,
        error_on_addresses: dict[str, Exception] | None = None,
    ) -> None:
        self.wallet_infos = {k.lower(): v for k, v in (wallet_infos or {}).items()}
        self.token_balance = token_balance
        self.raise_token_error = raise_token_error
        self.error_on_addresses = {k.lower(): v for k, v in (error_on_addresses or {}).items()}
        self.wallet_info_queries: list[str] = []

    async def get_wallet_info(self, address: str) -> WalletInfo:
        addr = address.lower()
        self.wallet_info_queries.append(addr)
        if addr in self.error_on_addresses:
            raise self.error_on_addresses[addr]
        if addr in self.wallet_infos:
            return self.wallet_infos[addr]
        return WalletInfo(
            address=addr,
            transaction_count=0,
            balance_wei=Decimal("0"),
            first_transaction=None,
        )

    async def get_token_balance(self, contract_address: str, wallet_address: str) -> Decimal:
        _ = (contract_address, wallet_address)
        if self.raise_token_error is not None:
            raise self.raise_token_error
        return self.token_balance


class TestWalletAnalyzerInit:
    """Tests for WalletAnalyzer initialization."""

    def test_init_default(self) -> None:
        """Test initialization with defaults."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client)

        assert analyzer._client is client
        assert analyzer._redis is None
        assert analyzer._fresh_threshold == DEFAULT_FRESH_THRESHOLD
        assert analyzer._usdc_address == USDC_POLYGON_ADDRESS

    def test_init_with_redis(self, fake_redis: FakeAsyncRedis) -> None:
        """Test initialization with Redis."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, redis=fake_redis)

        assert analyzer._redis is fake_redis

    def test_init_custom_threshold(self) -> None:
        """Test initialization with custom threshold."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, fresh_threshold=10)

        assert analyzer._fresh_threshold == 10


class TestWalletAnalyzerAnalyze:
    """Tests for the analyze method."""

    @pytest.mark.asyncio
    async def test_analyze_fresh_wallet(self) -> None:
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
        info = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=3,
            balance_wei=Decimal("5000000000000000000"),
            first_transaction=first_tx,
        )
        client = FakePolygonClient(wallet_infos={VALID_ADDRESS: info})
        analyzer = WalletAnalyzer(client)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.address == VALID_ADDRESS.lower()
        assert profile.nonce == 3
        assert profile.is_fresh is True
        assert profile.first_seen is not None
        assert profile.age_hours is not None
        assert 11 < profile.age_hours < 13

    @pytest.mark.asyncio
    async def test_analyze_old_wallet(self) -> None:
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
        info = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=500,
            balance_wei=Decimal("100000000000000000000"),
            first_transaction=first_tx,
        )
        client = FakePolygonClient(wallet_infos={VALID_ADDRESS: info})
        analyzer = WalletAnalyzer(client)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.nonce == 500
        assert profile.is_fresh is False
        assert profile.age_hours is not None
        assert profile.age_hours > 8000  # Over 365 days in hours

    @pytest.mark.asyncio
    async def test_analyze_brand_new_wallet(self) -> None:
        """Test analyzing a wallet with no transactions."""
        info = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=0,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=None,
        )
        client = FakePolygonClient(wallet_infos={VALID_ADDRESS: info})
        analyzer = WalletAnalyzer(client)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.nonce == 0
        assert profile.is_fresh is True
        assert profile.is_brand_new is True
        assert profile.first_seen is None
        assert profile.age_hours is None

    @pytest.mark.asyncio
    async def test_analyze_uses_cache(self, fake_redis: FakeAsyncRedis) -> None:
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
        await fake_redis.set(
            f"wallet_profile:{VALID_ADDRESS.lower()}",
            json.dumps(cached_data).encode(),
        )

        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, redis=fake_redis)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.address == VALID_ADDRESS.lower()
        assert profile.nonce == 2
        assert len(client.wallet_info_queries) == 0

    @pytest.mark.asyncio
    async def test_analyze_force_refresh(self, fake_redis: FakeAsyncRedis) -> None:
        """Test that force_refresh bypasses cache."""
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
        await fake_redis.set(
            f"wallet_profile:{VALID_ADDRESS.lower()}",
            json.dumps(cached_data).encode(),
        )

        info = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=10,
            balance_wei=Decimal("5000000000000000000"),
            first_transaction=None,
        )
        client = FakePolygonClient(wallet_infos={VALID_ADDRESS: info})
        analyzer = WalletAnalyzer(client, redis=fake_redis)
        profile = await analyzer.analyze(VALID_ADDRESS, force_refresh=True)

        assert profile.nonce == 10  # From fresh query, not cache
        assert client.wallet_info_queries == [VALID_ADDRESS.lower()]

    @pytest.mark.asyncio
    async def test_analyze_handles_usdc_error(self) -> None:
        """Test that USDC balance error is handled gracefully."""
        info = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=1,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=None,
        )
        client = FakePolygonClient(
            wallet_infos={VALID_ADDRESS: info},
            raise_token_error=Exception("Token query failed"),
        )
        analyzer = WalletAnalyzer(client)
        profile = await analyzer.analyze(VALID_ADDRESS)

        assert profile.usdc_balance == Decimal(0)


class TestWalletAnalyzerIsFresh:
    """Tests for the is_fresh method."""

    @pytest.mark.asyncio
    async def test_is_fresh_true(self) -> None:
        """Test is_fresh returns True for fresh wallet."""
        info = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=2,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=None,
        )
        client = FakePolygonClient(wallet_infos={VALID_ADDRESS: info})
        analyzer = WalletAnalyzer(client)
        result = await analyzer.is_fresh(VALID_ADDRESS)

        assert result is True

    @pytest.mark.asyncio
    async def test_is_fresh_false(self) -> None:
        """Test is_fresh returns False for old wallet."""
        info = WalletInfo(
            address=VALID_ADDRESS.lower(),
            transaction_count=100,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=None,
        )
        client = FakePolygonClient(wallet_infos={VALID_ADDRESS: info})
        analyzer = WalletAnalyzer(client)
        result = await analyzer.is_fresh(VALID_ADDRESS)

        assert result is False


class TestWalletAnalyzerFreshnessLogic:
    """Tests for freshness determination logic."""

    def test_is_wallet_fresh_low_nonce_no_age(self) -> None:
        """Test fresh wallet with low nonce and unknown age."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=2, age_hours=None)
        assert result is True

    def test_is_wallet_fresh_low_nonce_young_age(self) -> None:
        """Test fresh wallet with low nonce and young age."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=2, age_hours=12.0)
        assert result is True

    def test_is_wallet_fresh_low_nonce_old_age(self) -> None:
        """Test not fresh when nonce is low but age is old."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=2, age_hours=100.0)
        assert result is False

    def test_is_wallet_fresh_high_nonce(self) -> None:
        """Test not fresh when nonce is high."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=10, age_hours=12.0)
        assert result is False

    def test_is_wallet_fresh_at_threshold(self) -> None:
        """Test not fresh when nonce equals threshold."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, fresh_threshold=5)
        result = analyzer._is_wallet_fresh(nonce=5, age_hours=12.0)
        assert result is False

    def test_is_wallet_fresh_at_age_boundary(self) -> None:
        """Test at 48 hour boundary."""
        client = FakePolygonClient()
        analyzer = WalletAnalyzer(client, fresh_threshold=5)

        result_under = analyzer._is_wallet_fresh(nonce=2, age_hours=47.9)
        assert result_under is True

        result_over = analyzer._is_wallet_fresh(nonce=2, age_hours=48.1)
        assert result_over is False


class TestWalletAnalyzerBatch:
    """Tests for batch analysis."""

    @pytest.mark.asyncio
    async def test_analyze_batch(self) -> None:
        """Test batch analysis."""
        infos = {
            VALID_ADDRESS: WalletInfo(
                address=VALID_ADDRESS.lower(),
                transaction_count=2,
                balance_wei=Decimal("1000"),
                first_transaction=None,
            ),
            VALID_ADDRESS_2: WalletInfo(
                address=VALID_ADDRESS_2.lower(),
                transaction_count=100,
                balance_wei=Decimal("2000"),
                first_transaction=None,
            ),
        }
        client = FakePolygonClient(wallet_infos=infos)
        analyzer = WalletAnalyzer(client)
        profiles = await analyzer.analyze_batch([VALID_ADDRESS, VALID_ADDRESS_2])

        assert len(profiles) == 2
        assert profiles[VALID_ADDRESS.lower()].is_fresh is True
        assert profiles[VALID_ADDRESS_2.lower()].is_fresh is False

    @pytest.mark.asyncio
    async def test_analyze_batch_handles_errors(self) -> None:
        """Test batch analysis handles individual failures."""
        infos = {
            VALID_ADDRESS: WalletInfo(
                address=VALID_ADDRESS.lower(),
                transaction_count=2,
                balance_wei=Decimal("1000"),
                first_transaction=None,
            )
        }
        client = FakePolygonClient(
            wallet_infos=infos,
            error_on_addresses={VALID_ADDRESS_2: Exception("RPC error")},
        )
        analyzer = WalletAnalyzer(client)
        profiles = await analyzer.analyze_batch([VALID_ADDRESS, VALID_ADDRESS_2])

        assert len(profiles) == 1
        assert VALID_ADDRESS.lower() in profiles

    @pytest.mark.asyncio
    async def test_get_fresh_wallets(self) -> None:
        """Test filtering to only fresh wallets."""
        infos = {
            VALID_ADDRESS: WalletInfo(
                address=VALID_ADDRESS.lower(),
                transaction_count=2,
                balance_wei=Decimal("1000"),
                first_transaction=None,
            ),
            VALID_ADDRESS_2: WalletInfo(
                address=VALID_ADDRESS_2.lower(),
                transaction_count=100,
                balance_wei=Decimal("2000"),
                first_transaction=None,
            ),
        }
        client = FakePolygonClient(wallet_infos=infos)
        analyzer = WalletAnalyzer(client)
        fresh = await analyzer.get_fresh_wallets([VALID_ADDRESS, VALID_ADDRESS_2])

        assert len(fresh) == 1
        assert VALID_ADDRESS.lower() in fresh
