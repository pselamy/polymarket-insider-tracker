"""Wallet analysis for fresh wallet detection.

This module provides wallet analysis capabilities to identify potentially
suspicious wallets based on their on-chain activity patterns.
"""

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal

from redis.asyncio import Redis

from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.models import WalletProfile

logger = logging.getLogger(__name__)

# USDC contract address on Polygon
USDC_POLYGON_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# Default configuration
DEFAULT_FRESH_THRESHOLD = 5  # Max nonce to be considered fresh
DEFAULT_PROFILE_CACHE_TTL = 300  # 5 minutes


class WalletAnalyzer:
    """Analyzes wallets to detect fresh wallet patterns.

    This class provides wallet analysis functionality including:
    - Fresh wallet detection based on transaction count
    - Wallet age calculation from first transaction
    - Balance queries for MATIC and USDC
    - Caching of analysis results

    Example:
        ```python
        client = PolygonClient("https://polygon-rpc.com", redis=redis)
        analyzer = WalletAnalyzer(client, redis=redis)

        # Full analysis
        profile = await analyzer.analyze("0x...")
        print(f"Fresh: {profile.is_fresh}, Score: {profile.freshness_score}")

        # Quick check
        is_fresh = await analyzer.is_fresh("0x...")
        ```
    """

    def __init__(
        self,
        polygon_client: PolygonClient,
        *,
        redis: Redis | None = None,
        fresh_threshold: int = DEFAULT_FRESH_THRESHOLD,
        cache_ttl_seconds: int = DEFAULT_PROFILE_CACHE_TTL,
        usdc_address: str = USDC_POLYGON_ADDRESS,
    ) -> None:
        """Initialize the wallet analyzer.

        Args:
            polygon_client: PolygonClient for blockchain queries.
            redis: Optional Redis client for caching profiles.
            fresh_threshold: Maximum nonce to be considered fresh.
            cache_ttl_seconds: How long to cache analysis results.
            usdc_address: USDC token contract address on Polygon.
        """
        self._client = polygon_client
        self._redis = redis
        self._fresh_threshold = fresh_threshold
        self._cache_ttl = cache_ttl_seconds
        self._usdc_address = usdc_address
        self._cache_prefix = "wallet_profile:"

    def _cache_key(self, address: str) -> str:
        """Generate cache key for wallet profile."""
        return f"{self._cache_prefix}{address.lower()}"

    async def _get_cached_profile(self, address: str) -> WalletProfile | None:
        """Get cached profile if available."""
        if not self._redis:
            return None

        try:
            key = self._cache_key(address)
            cached = await self._redis.get(key)
            if cached is None:
                return None

            data = json.loads(cached if isinstance(cached, str) else cached.decode())
            return WalletProfile(
                address=data["address"],
                nonce=data["nonce"],
                first_seen=datetime.fromisoformat(data["first_seen"]) if data["first_seen"] else None,
                age_hours=data["age_hours"],
                is_fresh=data["is_fresh"],
                total_tx_count=data["total_tx_count"],
                matic_balance=Decimal(data["matic_balance"]),
                usdc_balance=Decimal(data["usdc_balance"]),
                analyzed_at=datetime.fromisoformat(data["analyzed_at"]),
                fresh_threshold=data["fresh_threshold"],
            )
        except Exception as e:
            logger.warning("Failed to get cached profile for %s: %s", address, e)
            return None

    async def _cache_profile(self, profile: WalletProfile) -> None:
        """Cache a wallet profile."""
        if not self._redis:
            return

        try:
            key = self._cache_key(profile.address)
            data = {
                "address": profile.address,
                "nonce": profile.nonce,
                "first_seen": profile.first_seen.isoformat() if profile.first_seen else None,
                "age_hours": profile.age_hours,
                "is_fresh": profile.is_fresh,
                "total_tx_count": profile.total_tx_count,
                "matic_balance": str(profile.matic_balance),
                "usdc_balance": str(profile.usdc_balance),
                "analyzed_at": profile.analyzed_at.isoformat(),
                "fresh_threshold": profile.fresh_threshold,
            }
            await self._redis.set(key, json.dumps(data), ex=self._cache_ttl)
        except Exception as e:
            logger.warning("Failed to cache profile for %s: %s", profile.address, e)

    async def analyze(
        self,
        address: str,
        *,
        force_refresh: bool = False,
    ) -> WalletProfile:
        """Analyze a wallet and return its profile.

        This method performs a comprehensive analysis of the wallet including:
        - Transaction count (nonce)
        - First transaction timestamp and wallet age
        - MATIC and USDC balances
        - Fresh wallet determination

        Args:
            address: Wallet address to analyze.
            force_refresh: If True, bypass cache and re-analyze.

        Returns:
            WalletProfile with analysis results.
        """
        address = address.lower()

        # Check cache unless force refresh
        if not force_refresh:
            cached = await self._get_cached_profile(address)
            if cached is not None:
                logger.debug("Using cached profile for %s", address)
                return cached

        # Get wallet info from blockchain
        wallet_info = await self._client.get_wallet_info(address)

        # Get USDC balance
        try:
            usdc_balance = await self._client.get_token_balance(address, self._usdc_address)
        except Exception as e:
            logger.warning("Failed to get USDC balance for %s: %s", address, e)
            usdc_balance = Decimal(0)

        # Calculate age from first transaction
        first_seen: datetime | None = None
        age_hours: float | None = None

        if wallet_info.first_transaction is not None:
            first_seen = wallet_info.first_transaction.timestamp
            now = datetime.now(UTC)
            delta = now - first_seen
            age_hours = delta.total_seconds() / 3600

        # Determine if fresh
        is_fresh = self._is_wallet_fresh(wallet_info.transaction_count, age_hours)

        # Build profile
        profile = WalletProfile(
            address=address,
            nonce=wallet_info.transaction_count,
            first_seen=first_seen,
            age_hours=age_hours,
            is_fresh=is_fresh,
            total_tx_count=wallet_info.transaction_count,
            matic_balance=wallet_info.balance_wei,
            usdc_balance=usdc_balance,
            fresh_threshold=self._fresh_threshold,
        )

        # Cache the result
        await self._cache_profile(profile)

        return profile

    def _is_wallet_fresh(self, nonce: int, age_hours: float | None) -> bool:
        """Determine if wallet should be considered fresh.

        A wallet is fresh if:
        - Transaction count (nonce) is below the threshold
        - AND either age is unknown OR age is less than 48 hours

        Args:
            nonce: Transaction count.
            age_hours: Wallet age in hours, or None if unknown.

        Returns:
            True if wallet is fresh.
        """
        # Must have few transactions
        if nonce >= self._fresh_threshold:
            return False

        # If age is known, must be recent (within 48 hours)
        return not (age_hours is not None and age_hours > 48)

    async def is_fresh(self, address: str) -> bool:
        """Quick check if wallet is fresh.

        This is a convenience method that returns just the freshness status.
        It uses cached data if available.

        Args:
            address: Wallet address to check.

        Returns:
            True if wallet is fresh.
        """
        profile = await self.analyze(address)
        return profile.is_fresh

    async def analyze_batch(
        self,
        addresses: list[str],
        *,
        force_refresh: bool = False,
    ) -> dict[str, WalletProfile]:
        """Analyze multiple wallets.

        Analyzes wallets in parallel for efficiency.

        Args:
            addresses: List of wallet addresses to analyze.
            force_refresh: If True, bypass cache for all wallets.

        Returns:
            Dictionary mapping address (lowercase) to WalletProfile.
        """
        import asyncio

        results: dict[str, WalletProfile] = {}

        # Analyze all in parallel
        tasks = [self.analyze(addr, force_refresh=force_refresh) for addr in addresses]
        profiles = await asyncio.gather(*tasks, return_exceptions=True)

        for addr, profile in zip(addresses, profiles, strict=True):
            if isinstance(profile, BaseException):
                logger.warning("Failed to analyze %s: %s", addr, profile)
                continue
            results[addr.lower()] = profile

        return results

    async def get_fresh_wallets(
        self,
        addresses: list[str],
    ) -> list[str]:
        """Filter addresses to only return fresh wallets.

        Args:
            addresses: List of wallet addresses to check.

        Returns:
            List of addresses that are fresh wallets.
        """
        profiles = await self.analyze_batch(addresses)
        return [addr for addr, profile in profiles.items() if profile.is_fresh]
