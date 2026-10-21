"""The Gamma refresh runs on its own cadence, independent of the CLOB crawl (T012b, T012d)."""

from __future__ import annotations

import asyncio
import logging
import time

import pytest
from fakeredis import FakeAsyncRedis
from redis.typing import EncodableT, ExpiryT, KeyT

from polymarket_insider_tracker.ingestor.gamma_client import GammaCrawl
from polymarket_insider_tracker.ingestor.gamma_volume_refresh import GammaVolumeRefresh
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync, MetadataSyncError
from polymarket_insider_tracker.ingestor.models import Market
from polymarket_insider_tracker.ingestor.volume_store import VolumeStore
from tests.fakes import FakeGammaClient
from tests.ingestor.test_metadata_sync import FakeClobClient
from tests.ingestor.test_volume_store import CID, MARKET, MARKET_METADATA, _stats


class CountingGamma(FakeGammaClient):
    def __init__(self) -> None:
        super().__init__({CID: _stats()})
        self.crawls = 0

    async def crawl_active_markets(self) -> GammaCrawl:
        self.crawls += 1
        return await super().crawl_active_markets()


class SlowCountingClob(FakeClobClient):
    """A CLOB crawl that takes a measurable time and counts how often it runs."""

    def __init__(self, markets: list[Market], seconds: float) -> None:
        super().__init__(markets)
        self.calls = 0
        self._seconds = seconds

    def get_markets(self, active_only: bool = True) -> list[Market]:
        self.calls += 1
        time.sleep(self._seconds)
        return super().get_markets(active_only)


class BrokenRedis(FakeAsyncRedis):
    async def setex(self, name: KeyT, time: ExpiryT, value: EncodableT) -> bool:
        _ = (name, time, value)
        raise ConnectionError("redis is down")


@pytest.mark.asyncio
async def test_gamma_refreshes_repeatedly_while_the_slow_clob_sync_runs_once(
    fake_redis: FakeAsyncRedis,
) -> None:
    gamma, clob = CountingGamma(), SlowCountingClob([MARKET], 0.05)
    sync = MarketMetadataSync(
        redis=fake_redis, clob_client=clob, gamma_client=gamma, gamma_refresh_interval_seconds=0.01
    )
    await sync.start()
    try:
        await asyncio.sleep(0.2)
    finally:
        await sync.stop()

    assert clob.calls == 1
    assert gamma.crawls >= 4


@pytest.mark.asyncio
async def test_stop_ends_the_refresh_loop(fake_redis: FakeAsyncRedis) -> None:
    gamma = CountingGamma()
    sync = MarketMetadataSync(
        redis=fake_redis,
        clob_client=FakeClobClient([MARKET]),
        gamma_client=gamma,
        gamma_refresh_interval_seconds=0.01,
    )
    await sync.start()
    await sync.stop()
    seen = gamma.crawls

    await asyncio.sleep(0.05)

    assert gamma.crawls == seen


@pytest.mark.asyncio
async def test_a_gamma_failure_records_the_outcome_and_does_not_raise(
    fake_redis: FakeAsyncRedis, caplog: pytest.LogCaptureFixture
) -> None:
    store = VolumeStore(fake_redis)
    refresh = GammaVolumeRefresh(FakeGammaClient(raise_error=RuntimeError("down")), store)

    with caplog.at_level(logging.WARNING):
        await refresh.refresh_once()

    assert "gamma stats fetch failed" in caplog.text
    merged = await store.merge(MARKET_METADATA)
    assert merged.volume_unavailable_reason == "gamma_fetch_failed"


@pytest.mark.asyncio
async def test_a_store_failure_is_logged_and_never_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    refresh = GammaVolumeRefresh(CountingGamma(), VolumeStore(BrokenRedis()))

    with caplog.at_level(logging.WARNING):
        await refresh.refresh_once()

    assert "gamma volume store failed" in caplog.text


@pytest.mark.asyncio
async def test_stop_before_start_is_a_no_op(fake_redis: FakeAsyncRedis) -> None:
    await GammaVolumeRefresh(CountingGamma(), VolumeStore(fake_redis)).stop()


@pytest.mark.asyncio
async def test_a_failed_initial_clob_sync_cancels_the_first_gamma_refresh(
    fake_redis: FakeAsyncRedis,
) -> None:
    gamma = CountingGamma()
    sync = MarketMetadataSync(
        redis=fake_redis,
        clob_client=FakeClobClient([], raise_error=RuntimeError("clob down")),
        gamma_client=gamma,
    )

    with pytest.raises(MetadataSyncError):
        await sync.start()

    await asyncio.sleep(0)
    assert not [task for task in asyncio.all_tasks() if "refresh_once" in repr(task)]
