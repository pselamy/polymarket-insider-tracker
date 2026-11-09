"""Volume record retention after a failed crawl (scenario 4): the prior observation keeps its
acquisition stamp, its TTL is never extended, and it is unavailable once the record expires."""

from __future__ import annotations

from datetime import timedelta

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.detector.volume_state import VolumeState, classify_volume
from polymarket_insider_tracker.ingestor.volume_store import DEFAULT_VOLUME_KEY_PREFIX, VolumeStore
from tests.ingestor.test_volume_store import CID, MARKET_METADATA, STAMP, crawl_of, volume_stats


@pytest.mark.asyncio
async def test_failed_crawl_keeps_prior_observation_available_until_600s_then_stale(
    fake_redis: FakeAsyncRedis,
) -> None:
    store = VolumeStore(fake_redis)
    await store.write_crawl(crawl_of(volume_stats()), STAMP)
    await store.write_crawl(None, STAMP + timedelta(seconds=300))

    merged = await store.merge(MARKET_METADATA)

    assert merged.volume is not None
    assert merged.volume_unavailable_reason is None
    assert merged.volume.acquired_at == "2026-10-19T12:00:00+00:00"
    before = classify_volume(merged.volume, evaluated_at=STAMP + timedelta(seconds=599))
    after = classify_volume(merged.volume, evaluated_at=STAMP + timedelta(seconds=601))
    assert before.state is VolumeState.AVAILABLE
    assert (after.state, after.reason) == (VolumeState.STALE, "older_than_600s")


@pytest.mark.asyncio
async def test_a_failed_crawl_does_not_extend_the_record_ttl(fake_redis: FakeAsyncRedis) -> None:
    store = VolumeStore(fake_redis)
    key = DEFAULT_VOLUME_KEY_PREFIX + CID
    await store.write_crawl(crawl_of(volume_stats()), STAMP)
    # A short, known remaining TTL makes "not reset" observable: a rewrite would restore 1,200 s.
    await fake_redis.expire(key, 100)
    record_before = await fake_redis.get(key)

    await store.write_crawl(None, STAMP + timedelta(seconds=300))

    assert await fake_redis.get(key) == record_before
    remaining = await fake_redis.ttl(key)
    assert 0 < remaining <= 100

    # Redis TTL expiry of the record: EXPIRE 0 removes it now.
    await fake_redis.expire(key, 0)

    merged = await store.merge(MARKET_METADATA)
    assert merged.volume is None
    assert merged.volume_unavailable_reason == "gamma_fetch_failed"
