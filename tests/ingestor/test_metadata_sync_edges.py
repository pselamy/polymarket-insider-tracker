"""Failure handling of the metadata synchronizer: callbacks, loop errors, bad cache entries."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.ingestor.metadata_sync import (
    DEFAULT_REDIS_KEY_PREFIX,
    MarketMetadataSync,
    MetadataSyncError,
    SyncState,
    SyncStats,
)
from polymarket_insider_tracker.ingestor.models import Market, MarketMetadata, Token
from tests.fakes import FakeGammaClient
from tests.fakes.redis_faults import fail_commands
from tests.ingestor.test_metadata_sync import FakeClobClient

PREFIX = DEFAULT_REDIS_KEY_PREFIX


def _market(condition_id: str = "cond123", question: str = "Will Bitcoin exceed $100k?") -> Market:
    return Market(
        condition_id=condition_id,
        question=question,
        description="desc",
        tokens=(Token(token_id="t1", outcome="Yes", price=Decimal("0.5")),),
        end_date=datetime(2026, 12, 31, tzinfo=UTC),
        active=True,
        closed=False,
    )


def _sync(
    redis: FakeAsyncRedis,
    clob: FakeClobClient | None = None,
    *,
    sync_interval_seconds: int = 300,
    on_state_change: Callable[[SyncState], None] | None = None,
    on_sync_complete: Callable[[SyncStats], None] | None = None,
) -> MarketMetadataSync:
    return MarketMetadataSync(
        redis=redis,
        clob_client=clob or FakeClobClient([_market()]),
        gamma_client=FakeGammaClient(),
        sync_interval_seconds=sync_interval_seconds,
        on_state_change=on_state_change,
        on_sync_complete=on_sync_complete,
    )


class FailingAfterFirstSyncClob(FakeClobClient):
    """Serves the initial listing, then fails every later listing."""

    def __init__(self, markets: list[Market]) -> None:
        super().__init__(markets)
        self.listings = 0

    def get_markets(self, active_only: bool = True) -> list[Market]:
        self.listings += 1
        if self.listings > 1:
            raise RuntimeError("clob listing down")
        return super().get_markets(active_only)


class BlockingSecondListingClob(FakeClobClient):
    """Serves the initial listing at once, then holds the next listing until released."""

    def __init__(self, markets: list[Market]) -> None:
        super().__init__(markets)
        self.listings = 0
        self.second_listing_started = threading.Event()
        self.release = threading.Event()

    def get_markets(self, active_only: bool = True) -> list[Market]:
        self.listings += 1
        if self.listings > 1:
            self.second_listing_started.set()
            self.release.wait(timeout=10)
        return super().get_markets(active_only)


async def _until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.02)


async def test_failing_state_callback_is_logged_and_the_state_still_changes(
    fake_redis: FakeAsyncRedis, caplog: pytest.LogCaptureFixture
) -> None:
    def explode(_state: SyncState) -> None:
        raise RuntimeError("state hook boom")

    sync = _sync(fake_redis, on_state_change=explode)

    with caplog.at_level(logging.WARNING):
        await sync.start()
        await sync.stop()

    assert "State change callback failed: state hook boom" in caplog.text
    assert sync.state == SyncState.STOPPED


async def test_failing_sync_complete_callback_does_not_fail_the_sync(
    fake_redis: FakeAsyncRedis, caplog: pytest.LogCaptureFixture
) -> None:
    def explode(_stats: SyncStats) -> None:
        raise RuntimeError("complete hook boom")

    sync = _sync(fake_redis, on_sync_complete=explode)

    with caplog.at_level(logging.WARNING):
        await sync.start()
        await sync.stop()

    assert "Sync complete callback failed: complete hook boom" in caplog.text
    assert sync.stats.successful_syncs == 1


async def test_stop_after_a_failed_start_resets_to_stopped(fake_redis: FakeAsyncRedis) -> None:
    sync = _sync(fake_redis, FakeClobClient(raise_error=RuntimeError("clob down")))
    with pytest.raises(MetadataSyncError):
        await sync.start()

    await sync.stop()

    assert sync.state == SyncState.STOPPED


async def test_periodic_sync_failure_is_recorded_and_the_loop_keeps_running(
    fake_redis: FakeAsyncRedis,
) -> None:
    clob = FailingAfterFirstSyncClob([_market()])
    sync = _sync(fake_redis, clob, sync_interval_seconds=1)
    await sync.start()
    try:
        await _until(lambda: clob.listings >= 3)
    finally:
        await sync.stop()

    assert sync.stats.successful_syncs == 1
    assert sync.stats.failed_syncs >= 2
    assert sync.stats.last_error == "clob listing down"


async def test_periodic_sync_runs_again_after_the_interval(fake_redis: FakeAsyncRedis) -> None:
    sync = _sync(fake_redis, sync_interval_seconds=1)
    await sync.start()
    try:
        await _until(lambda: sync.stats.successful_syncs >= 2)
    finally:
        await sync.stop()

    assert sync.stats.last_error is None


async def test_unwritable_market_is_skipped_and_counted_out(
    fake_redis: FakeAsyncRedis, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    fail_commands(monkeypatch, fake_redis, setex=ConnectionError("write refused"))
    sync = _sync(fake_redis)

    with caplog.at_level(logging.WARNING):
        await sync.start()
        await sync.stop()

    assert "Failed to cache market cond123: write refused" in caplog.text
    assert sync.stats.markets_cached == 0
    assert sync.stats.successful_syncs == 1


async def test_cached_lookup_misses_for_absent_and_corrupt_entries(
    fake_redis: FakeAsyncRedis, caplog: pytest.LogCaptureFixture
) -> None:
    sync = _sync(fake_redis)
    await fake_redis.set(f"{PREFIX}corrupt", "{not json")
    await fake_redis.set(f"{PREFIX}incomplete", json.dumps({}))

    with caplog.at_level(logging.WARNING):
        assert await sync.get_cached_market("absent") is None
        assert await sync.get_cached_market("corrupt") is None
        assert await sync.get_cached_market("incomplete") is None

    assert caplog.text.count("Failed to parse cached market") == 2


async def test_corrupt_cache_entry_falls_back_to_the_clob_and_repairs_nothing_it_owns(
    fake_redis: FakeAsyncRedis, caplog: pytest.LogCaptureFixture
) -> None:
    sync = _sync(fake_redis)
    await fake_redis.set(f"{PREFIX}cond123", "{not json")

    with caplog.at_level(logging.WARNING):
        metadata = await sync.get_market("cond123")

    assert metadata is not None
    assert metadata.condition_id == "cond123"
    assert "Failed to parse cached market cond123" in caplog.text


async def test_unknown_market_is_none_after_a_failed_clob_lookup(
    fake_redis: FakeAsyncRedis, caplog: pytest.LogCaptureFixture
) -> None:
    sync = _sync(fake_redis)

    with caplog.at_level(logging.WARNING):
        assert await sync.get_market("0xunknown") is None

    assert "Failed to fetch market 0xunknown" in caplog.text


async def test_category_scan_skips_unreadable_empty_and_other_category_entries(
    fake_redis: FakeAsyncRedis,
) -> None:
    sync = _sync(fake_redis)
    crypto = MarketMetadata.from_market(_market("crypto1"))
    politics = MarketMetadata.from_market(_market("pol1", "Will Trump win the election?"))
    await fake_redis.set(f"{PREFIX}crypto1", json.dumps(crypto.to_dict()))
    await fake_redis.set(f"{PREFIX}pol1", json.dumps(politics.to_dict()))
    await fake_redis.set(f"{PREFIX}empty", "")
    await fake_redis.set(f"{PREFIX}corrupt", "{not json")
    await fake_redis.set(f"{PREFIX}partial", json.dumps({"category": "crypto"}))

    results = await sync.get_markets_by_category("crypto")

    assert [m.condition_id for m in results] == ["crypto1"]


async def test_category_scan_covers_every_page_of_a_large_keyspace(
    fake_redis: FakeAsyncRedis,
) -> None:
    sync = _sync(fake_redis)
    for index in range(350):
        metadata = MarketMetadata.from_market(_market(f"bulk{index}"))
        await fake_redis.set(f"{PREFIX}bulk{index}", json.dumps(metadata.to_dict()))

    results = await sync.get_markets_by_category("crypto")

    assert len(results) == 350


async def test_stop_during_a_periodic_sync_cancels_the_loop_without_recording_a_failure(
    fake_redis: FakeAsyncRedis,
) -> None:
    clob = BlockingSecondListingClob([_market()])
    sync = _sync(fake_redis, clob, sync_interval_seconds=1)
    await sync.start()
    try:
        await asyncio.to_thread(clob.second_listing_started.wait, 5)
        await sync.stop()
    finally:
        clob.release.set()

    assert sync.state == SyncState.STOPPED
    assert sync.stats.failed_syncs == 0
