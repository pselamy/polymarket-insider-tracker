"""Volume record retention after a failed crawl (scenario 4): the prior observation keeps its
acquisition stamp, its TTL is never extended, and it is unavailable once the record expires.

Each fact is one parametrized case over one observed run; the observation helpers assert nothing,
and the expected values come from the specification (600 s recency, 1,200 s TTL).
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.detector.volume_state import VolumeState, classify_volume
from polymarket_insider_tracker.ingestor.gamma_client import GammaCrawl, GammaMarketStats
from polymarket_insider_tracker.ingestor.volume_store import DEFAULT_VOLUME_KEY_PREFIX, VolumeStore
from tests.ingestor.test_volume_store import CID, MARKET_METADATA, STAMP, volume_stats


def crawl_of(*stats: GammaMarketStats, pages_ok: int = 3, truncated: bool = False) -> GammaCrawl:
    return GammaCrawl({s.condition_id: s for s in stats}, pages_ok, 0, truncated, STAMP, STAMP)


async def _observe_staleness(fake_redis: FakeAsyncRedis) -> dict[str, object]:
    store = VolumeStore(fake_redis)
    await store.write_crawl(crawl_of(volume_stats()), STAMP)
    await store.write_crawl(None, STAMP + timedelta(seconds=300))
    merged = await store.merge(MARKET_METADATA)
    volume = merged.volume
    at_599 = classify_volume(volume, evaluated_at=STAMP + timedelta(seconds=599))
    at_601 = classify_volume(volume, evaluated_at=STAMP + timedelta(seconds=601))
    return {
        "volume_present": volume is not None,
        "unavailable_reason": merged.volume_unavailable_reason,
        "acquired_at": volume.acquired_at if volume is not None else None,
        "state_at_599s": at_599.state.value,
        "state_at_601s": at_601.state.value,
        "reason_at_601s": at_601.reason,
    }


async def _observe_ttl(fake_redis: FakeAsyncRedis) -> dict[str, object]:
    store = VolumeStore(fake_redis)
    key = DEFAULT_VOLUME_KEY_PREFIX + CID
    await store.write_crawl(crawl_of(volume_stats()), STAMP)
    # A short, known remaining TTL makes "not reset" observable: a rewrite would restore 1,200 s.
    await fake_redis.expire(key, 100)
    record_before = await fake_redis.get(key)
    await store.write_crawl(None, STAMP + timedelta(seconds=300))
    record_after = await fake_redis.get(key)
    remaining = await fake_redis.ttl(key)
    await fake_redis.expire(key, 0)
    merged = await store.merge(MARKET_METADATA)
    return {
        "record_unchanged_by_failure": record_after == record_before,
        "ttl_remaining_at_most_100s": 0 < remaining <= 100,
        "volume_after_expiry_present": merged.volume is not None,
        "reason_after_expiry": merged.volume_unavailable_reason,
    }


STALENESS_EXPECTED: list[tuple[str, object]] = [
    ("volume_present", True),
    ("unavailable_reason", None),
    ("acquired_at", "2026-10-19T12:00:00+00:00"),
    ("state_at_599s", VolumeState.AVAILABLE.value),
    ("state_at_601s", VolumeState.STALE.value),
    ("reason_at_601s", "older_than_600s"),
]

TTL_EXPECTED: list[tuple[str, object]] = [
    ("record_unchanged_by_failure", True),
    ("ttl_remaining_at_most_100s", True),
    ("volume_after_expiry_present", False),
    ("reason_after_expiry", "gamma_fetch_failed"),
]


@pytest.fixture
async def staleness(fake_redis: FakeAsyncRedis) -> dict[str, object]:
    return await _observe_staleness(fake_redis)


@pytest.fixture
async def ttl(fake_redis: FakeAsyncRedis) -> dict[str, object]:
    return await _observe_ttl(fake_redis)


@pytest.mark.parametrize("fact", [name for name, _ in STALENESS_EXPECTED])
async def test_failed_crawl_keeps_prior_observation_available_until_600s_then_stale(
    staleness: dict[str, object], fact: str
) -> None:
    # Arrange
    expected = dict(STALENESS_EXPECTED)[fact]

    # Act
    actual = staleness[fact]

    # Assert
    assert actual == expected


@pytest.mark.parametrize("fact", [name for name, _ in TTL_EXPECTED])
async def test_a_failed_crawl_does_not_extend_the_record_ttl(
    ttl: dict[str, object], fact: str
) -> None:
    # Arrange
    expected = dict(TTL_EXPECTED)[fact]

    # Act
    actual = ttl[fact]

    # Assert
    assert actual == expected
