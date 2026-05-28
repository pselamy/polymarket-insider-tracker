"""Market metadata synchronizer with Redis caching.

This module provides a background sync service that keeps market metadata
up-to-date in Redis, with cache-first lookups for fast access.
"""

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum

from redis.asyncio import Redis

from .clob_client import ClobClient
from .gamma_client import GammaClient, GammaClientError, GammaMarketStats
from .models import MarketMetadata

logger = logging.getLogger(__name__)


# Default configuration
DEFAULT_SYNC_INTERVAL_SECONDS = 300  # 5 minutes
# A full CLOB sync over 89k markets takes 600-900s on the live deployment.
# A 600s TTL means rows written early in one sync cycle expire before the
# next cycle even starts, leaving a large window where get_market() falls
# back to single-market CLOB fetches that don't carry gamma volume — every
# size_anomaly evaluated in that window scores volume_impact=0. 1800s
# safely covers worst-case sync_duration + sync_interval + jitter.
DEFAULT_CACHE_TTL_SECONDS = 1800  # 30 minutes
DEFAULT_REDIS_KEY_PREFIX = "polymarket:market:"


class SyncState(StrEnum):
    """State of the metadata synchronizer."""

    STOPPED = "stopped"
    STARTING = "starting"
    SYNCING = "syncing"
    IDLE = "idle"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class SyncStats:
    """Statistics for the metadata sync process."""

    total_syncs: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0
    markets_cached: int = 0
    last_sync_time: datetime | None = None
    last_sync_duration_seconds: float = 0.0
    last_error: str | None = None


# Type aliases for callbacks
StateCallback = Callable[[SyncState], None]
SyncCallback = Callable[[SyncStats], None]


class MetadataSyncError(Exception):
    """Base exception for metadata sync errors."""

    pass


class MarketMetadataSync:
    """Background service that syncs market metadata to Redis.

    This service:
    - Fetches all markets from the CLOB API on startup
    - Refreshes the cache every sync_interval_seconds (default: 5 minutes)
    - Stores market metadata in Redis with TTL-based expiration
    - Provides cache-first lookups via get_market()

    Example:
        ```python
        redis = Redis.from_url("redis://localhost:6379")
        clob = ClobClient()

        sync = MarketMetadataSync(redis=redis, clob_client=clob)
        await sync.start()

        # Get market metadata (cache-first)
        metadata = await sync.get_market("0x1234...")

        await sync.stop()
        ```
    """

    def __init__(
        self,
        redis: Redis,
        clob_client: ClobClient,
        *,
        gamma_client: GammaClient | None = None,
        sync_interval_seconds: int = DEFAULT_SYNC_INTERVAL_SECONDS,
        cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
        key_prefix: str = DEFAULT_REDIS_KEY_PREFIX,
        on_state_change: StateCallback | None = None,
        on_sync_complete: SyncCallback | None = None,
    ) -> None:
        """Initialize the metadata sync service.

        Args:
            redis: Redis async client for caching.
            clob_client: CLOB API client for fetching markets.
            gamma_client: Optional gamma-api client for volume/liquidity
                enrichment. Defaults to a fresh GammaClient() instance.
            sync_interval_seconds: Interval between syncs (default: 300 / 5 min).
            cache_ttl_seconds: TTL for cached entries (default: 600 / 10 min).
            key_prefix: Redis key prefix for market data.
            on_state_change: Callback for state changes.
            on_sync_complete: Callback after each sync completes.
        """
        self._redis = redis
        self._clob = clob_client
        self._gamma = gamma_client or GammaClient()
        self._sync_interval = sync_interval_seconds
        self._cache_ttl = cache_ttl_seconds
        self._key_prefix = key_prefix
        self._on_state_change = on_state_change
        self._on_sync_complete = on_sync_complete

        self._state = SyncState.STOPPED
        self._stats = SyncStats()
        self._sync_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._initial_sync_done = asyncio.Event()

    @property
    def state(self) -> SyncState:
        """Current sync state."""
        return self._state

    @property
    def stats(self) -> SyncStats:
        """Current sync statistics."""
        return self._stats

    def _set_state(self, new_state: SyncState) -> None:
        """Update state and notify callback."""
        old_state = self._state
        self._state = new_state
        if self._on_state_change and old_state != new_state:
            try:
                self._on_state_change(new_state)
            except Exception as e:
                logger.warning(f"State change callback failed: {e}")

    async def start(self, initial_sync_timeout: float | None = 30.0) -> None:
        """Start the background sync service.

        The initial sync runs in a background task. ``start()`` waits up to
        ``initial_sync_timeout`` seconds for the first sync to populate
        Redis before returning, so callers that need at least *some* market
        metadata (size-anomaly / tail-bet detectors) don't immediately face
        a cold cache. After the timeout the sync keeps running in the
        background and the rest of the pipeline can boot.

        Args:
            initial_sync_timeout: Max seconds to wait for the first sync.
                ``None`` = wait indefinitely (legacy behavior). Use ``0`` to
                skip the wait entirely.
        """
        if self._state != SyncState.STOPPED:
            logger.warning(f"Cannot start sync: already in state {self._state}")
            return

        self._set_state(SyncState.STARTING)
        self._stop_event.clear()

        # Sync loop runs initial sync as its first iteration, then loops.
        self._sync_task = asyncio.create_task(self._sync_loop(initial_sync=True))

        if initial_sync_timeout is None:
            await self._initial_sync_done.wait()
        elif initial_sync_timeout > 0:
            try:
                await asyncio.wait_for(
                    self._initial_sync_done.wait(),
                    timeout=initial_sync_timeout,
                )
            except TimeoutError:
                logger.info(
                    "Initial market sync still running after %.0fs; "
                    "continuing pipeline startup, sync will populate cache shortly",
                    initial_sync_timeout,
                )

        if self._state == SyncState.STARTING:
            self._set_state(SyncState.IDLE)
        logger.info("Market metadata sync started")

    async def stop(self) -> None:
        """Stop the background sync service."""
        if self._state == SyncState.STOPPED:
            return

        self._set_state(SyncState.STOPPING)
        self._stop_event.set()

        if self._sync_task:
            self._sync_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sync_task
            self._sync_task = None

        self._set_state(SyncState.STOPPED)
        self._initial_sync_done.clear()
        logger.info("Market metadata sync stopped")

    async def _sync_loop(self, initial_sync: bool = False) -> None:
        """Background loop that periodically syncs markets."""
        if initial_sync:
            try:
                await self._sync_all_markets()
            except asyncio.CancelledError:
                self._initial_sync_done.set()
                raise
            except Exception as e:
                # _sync_all_markets already updates stats / state on failure;
                # we just log here so we don't double-count failed_syncs.
                logger.error(f"Initial sync failed: {e}")
            finally:
                self._initial_sync_done.set()

        while not self._stop_event.is_set():
            try:
                # Wait for next sync interval or stop event
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._sync_interval,
                    )
                    # Stop event was set
                    break
                except TimeoutError:
                    # Timeout - time to sync
                    pass

                if self._stop_event.is_set():
                    break

                await self._sync_all_markets()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sync loop error: {e}")
                self._stats.failed_syncs += 1
                self._stats.last_error = str(e)
                self._set_state(SyncState.ERROR)
                # Continue running - will retry on next interval

    async def _fetch_gamma_stats(self) -> dict[str, GammaMarketStats]:
        """Fetch volume/liquidity stats from gamma-api.

        Returns an empty dict on failure so a degraded gamma endpoint
        does not stop CLOB metadata from being cached.
        """
        try:
            return await self._gamma.get_active_market_stats()
        except (GammaClientError, Exception) as e:
            logger.warning("gamma stats fetch failed (continuing without volume): %s", e)
            return {}

    async def _sync_all_markets(self) -> None:
        """Fetch all markets and cache them in Redis."""
        self._set_state(SyncState.SYNCING)
        start_time = datetime.now(UTC)
        self._stats.total_syncs += 1

        try:
            # Fetch CLOB markets and gamma volume snapshot in parallel
            markets, gamma_stats = await asyncio.gather(
                asyncio.to_thread(self._clob.get_markets, True),
                self._fetch_gamma_stats(),
            )

            # Cache each market in Redis, enriched with gamma volume/liquidity
            cached_count = 0
            enriched_count = 0
            for market in markets:
                try:
                    metadata = MarketMetadata.from_market(market)
                    stats = gamma_stats.get(metadata.condition_id)
                    if stats is not None:
                        metadata = replace(
                            metadata,
                            daily_volume=stats.daily_volume,
                            weekly_volume=stats.weekly_volume,
                            liquidity=stats.liquidity,
                        )
                        enriched_count += 1
                    await self._cache_market(metadata)
                    cached_count += 1
                except Exception as e:
                    logger.warning(f"Failed to cache market {market.condition_id}: {e}")

            # Update stats
            end_time = datetime.now(UTC)
            self._stats.successful_syncs += 1
            self._stats.markets_cached = cached_count
            self._stats.last_sync_time = end_time
            self._stats.last_sync_duration_seconds = (end_time - start_time).total_seconds()
            self._stats.last_error = None

            self._set_state(SyncState.IDLE)
            logger.info(
                "Synced %d markets (%d enriched with gamma volume) in %.2fs",
                cached_count,
                enriched_count,
                self._stats.last_sync_duration_seconds,
            )

            # Notify callback
            if self._on_sync_complete:
                try:
                    self._on_sync_complete(self._stats)
                except Exception as e:
                    logger.warning(f"Sync complete callback failed: {e}")

        except Exception as e:
            self._stats.failed_syncs += 1
            self._stats.last_error = str(e)
            self._set_state(SyncState.ERROR)
            logger.error(f"Market sync failed: {e}")
            raise

    async def _cache_market(self, metadata: MarketMetadata) -> None:
        """Cache a single market metadata in Redis.

        Args:
            metadata: The market metadata to cache.
        """
        key = f"{self._key_prefix}{metadata.condition_id}"
        value = json.dumps(metadata.to_dict())
        await self._redis.setex(key, self._cache_ttl, value)

    async def get_market(self, condition_id: str) -> MarketMetadata | None:
        """Get market metadata with cache-first lookup.

        This first checks Redis cache. If not found or expired,
        it fetches from the CLOB API and caches the result.

        Args:
            condition_id: The market condition ID.

        Returns:
            MarketMetadata if found, None otherwise.
        """
        # WebSocket payloads occasionally arrive without a conditionId (e.g.
        # subscription acks, reward/order events that share fields with trades).
        # Querying CLOB with an empty id resolves to ``/markets/`` and returns
        # 301, so short-circuit before retrying through the rate limiter.
        if not condition_id:
            return None

        # Try cache first
        key = f"{self._key_prefix}{condition_id}"
        cached = await self._redis.get(key)

        if cached:
            try:
                data = json.loads(cached)
                return MarketMetadata.from_dict(data)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to parse cached market {condition_id}: {e}")

        # Cache miss - fetch from API
        try:
            market = await asyncio.to_thread(self._clob.get_market, condition_id)
            if market:
                metadata = MarketMetadata.from_market(market)
                await self._cache_market(metadata)
                return metadata
        except Exception as e:
            logger.warning(f"Failed to fetch market {condition_id}: {e}")

        return None

    async def get_markets_by_category(self, category: str) -> list[MarketMetadata]:
        """Get all cached markets of a specific category.

        Note: This scans all cached markets. For large datasets,
        consider using a Redis set or secondary index.

        Args:
            category: The category to filter by.

        Returns:
            List of matching MarketMetadata.
        """
        results: list[MarketMetadata] = []
        pattern = f"{self._key_prefix}*"

        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(cursor, match=pattern, count=100)
            for key in keys:
                cached = await self._redis.get(key)
                if cached:
                    try:
                        data = json.loads(cached)
                        if data.get("category") == category:
                            results.append(MarketMetadata.from_dict(data))
                    except (json.JSONDecodeError, KeyError):
                        pass
            if cursor == 0:
                break

        return results

    async def invalidate_market(self, condition_id: str) -> bool:
        """Invalidate (delete) a cached market.

        Args:
            condition_id: The market condition ID to invalidate.

        Returns:
            True if the key was deleted, False if it didn't exist.
        """
        key = f"{self._key_prefix}{condition_id}"
        deleted = await self._redis.delete(key)
        return int(deleted) > 0

    async def force_sync(self) -> None:
        """Force an immediate sync of all markets.

        This can be called to refresh the cache outside of the
        normal sync interval.
        """
        await self._sync_all_markets()
