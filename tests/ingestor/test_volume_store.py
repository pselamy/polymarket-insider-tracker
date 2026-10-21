"""Volume records live apart from the CLOB records and are never replaced by volume-less ones."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from redis.typing import KeyT

from polymarket_insider_tracker.ingestor.gamma_client import GammaCrawl, GammaMarketStats
from polymarket_insider_tracker.ingestor.metadata_sync import (
    DEFAULT_REDIS_KEY_PREFIX,
    MarketMetadataSync,
)
from polymarket_insider_tracker.ingestor.models import Market, MarketMetadata, Token
from polymarket_insider_tracker.ingestor.volume_observation import JsonValue
from polymarket_insider_tracker.ingestor.volume_store import (
    DEFAULT_VOLUME_KEY_PREFIX,
    VOLUME_RECORD_TTL_SECONDS,
    VolumeRecord,
    VolumeStore,
)
from tests.fakes import FakeGammaClient
from tests.ingestor.test_metadata_sync import FakeClobClient

STAMP = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
CID = "0xcond"
MARKET = Market(CID, "Will a trial succeed?", "", (Token("t1", "Yes", Decimal("0.5")),))
MARKET_METADATA = MarketMetadata.from_market(MARKET)


def _stats(
    raw: JsonValue = "30000", cid: str = CID, acquired_at: datetime = STAMP
) -> GammaMarketStats:
    return GammaMarketStats(
        condition_id=cid,
        daily_volume=Decimal("30000"),
        weekly_volume=Decimal("100000"),
        monthly_volume=None,
        total_volume=None,
        liquidity=Decimal("42"),
        acquired_at=acquired_at,
        volume24hr_present=True,
        volume24hr_raw=raw,
        daily_volume_source="gamma-markets-v1",
    )


def _crawl(*stats: GammaMarketStats, pages_ok: int = 3, truncated: bool = False) -> GammaCrawl:
    return GammaCrawl({s.condition_id: s for s in stats}, pages_ok, 0, truncated, STAMP, STAMP)


def _sync(redis: FakeAsyncRedis, gamma: FakeGammaClient | None = None) -> MarketMetadataSync:
    return MarketMetadataSync(
        redis=redis, clob_client=FakeClobClient([MARKET]), gamma_client=gamma or FakeGammaClient()
    )


@pytest.mark.asyncio
async def test_written_record_keeps_stamp_source_and_raw(fake_redis: FakeAsyncRedis) -> None:
    store = VolumeStore(fake_redis)
    await store.write_crawl(_crawl(_stats(raw=30000)), STAMP)

    record = await store.read(CID)

    assert record is not None
    assert record.observation.raw == 30000
    assert record.observation.acquired_at == "2026-10-19T12:00:00+00:00"
    assert record.observation.source == "gamma-markets-v1"
    assert (record.daily_volume, record.weekly_volume) == (Decimal("30000"), Decimal("100000"))


@pytest.mark.asyncio
async def test_record_outlives_the_recency_rule_with_a_1200_second_ttl(
    fake_redis: FakeAsyncRedis,
) -> None:
    await VolumeStore(fake_redis).write_crawl(_crawl(_stats()), STAMP)

    remaining = await fake_redis.ttl(DEFAULT_VOLUME_KEY_PREFIX + CID)

    assert VOLUME_RECORD_TTL_SECONDS - 5 <= remaining <= VOLUME_RECORD_TTL_SECONDS


@pytest.mark.asyncio
async def test_a_cache_read_never_restamps(fake_redis: FakeAsyncRedis) -> None:
    store = VolumeStore(fake_redis)
    await store.write_crawl(_crawl(_stats()), STAMP)
    first = await store.read(CID)

    assert await store.read(CID) == first


@pytest.mark.asyncio
async def test_failed_crawl_keeps_the_older_observation(fake_redis: FakeAsyncRedis) -> None:
    store = VolumeStore(fake_redis)
    await store.write_crawl(_crawl(_stats()), STAMP)

    written = await store.write_crawl(None, datetime(2026, 10, 19, 12, 3, tzinfo=UTC))

    assert written == 0
    record = await store.read(CID)
    assert record is not None
    assert record.observation.acquired_at == "2026-10-19T12:00:00+00:00"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("crawl", "reason"),
    [
        (None, "gamma_fetch_failed"),
        (_crawl(pages_ok=0), "gamma_fetch_failed"),
        (_crawl(_stats(cid="0xother"), truncated=True), "crawl_truncated"),
        (_crawl(_stats(cid="0xother")), "absent_from_complete_crawl"),
    ],
    ids=["raised", "no-page-read", "truncated", "complete"],
)
async def test_unavailable_reason_follows_the_last_crawl(
    fake_redis: FakeAsyncRedis, crawl: GammaCrawl | None, reason: str
) -> None:
    store = VolumeStore(fake_redis)
    await store.write_crawl(crawl, STAMP)

    merged = await store.merge(MarketMetadata.from_market(MARKET))

    assert merged.volume is None
    assert merged.volume_unavailable_reason == reason


@pytest.mark.asyncio
async def test_no_record_and_no_crawl_status_is_no_volume_record(
    fake_redis: FakeAsyncRedis,
) -> None:
    merged = await VolumeStore(fake_redis).merge(MarketMetadata.from_market(MARKET))

    assert merged.volume_unavailable_reason == "no_volume_record"


@pytest.mark.asyncio
async def test_a_garbled_crawl_status_reads_as_no_volume_record(fake_redis: FakeAsyncRedis) -> None:
    await fake_redis.set(DEFAULT_VOLUME_KEY_PREFIX + "_crawl", b"not json")

    merged = await VolumeStore(fake_redis).merge(MarketMetadata.from_market(MARKET))

    assert merged.volume_unavailable_reason == "no_volume_record"


@pytest.mark.asyncio
async def test_expiry_leaves_stale_data_readable_then_unavailable(
    fake_redis: FakeAsyncRedis,
) -> None:
    store = VolumeStore(fake_redis)
    await store.write_crawl(_crawl(_stats()), STAMP)
    assert (await store.merge(MarketMetadata.from_market(MARKET))).volume is not None

    await fake_redis.delete(DEFAULT_VOLUME_KEY_PREFIX + CID, DEFAULT_VOLUME_KEY_PREFIX + "_crawl")

    merged = await store.merge(MarketMetadata.from_market(MARKET))
    assert (merged.volume, merged.volume_unavailable_reason) == (None, "no_volume_record")


@pytest.mark.asyncio
async def test_corrupt_volume_record_is_not_a_record(fake_redis: FakeAsyncRedis) -> None:
    await fake_redis.set(DEFAULT_VOLUME_KEY_PREFIX + CID, json.dumps({"observation": 1}))
    await fake_redis.set(DEFAULT_VOLUME_KEY_PREFIX + "other", b"[")
    await fake_redis.set(DEFAULT_VOLUME_KEY_PREFIX + "list", b"[]")
    store = VolumeStore(fake_redis)

    assert [await store.read(key) for key in (CID, "other", "list")] == [None, None, None]


def test_record_with_unparseable_decimals_reads_them_as_none() -> None:
    text = _stats().volume_observation().to_record()
    record = VolumeRecord.from_json(json.dumps({"observation": text, "daily_volume": "x"}))

    assert record is not None
    assert (record.daily_volume, record.weekly_volume) == (None, None)


@pytest.mark.asyncio
async def test_sync_merges_the_volume_into_the_cached_market(fake_redis: FakeAsyncRedis) -> None:
    now = datetime.now(UTC)
    sync = _sync(fake_redis, FakeGammaClient({CID: _stats(acquired_at=now)}))
    await sync.start()
    try:
        market = await sync.get_market(CID)
    finally:
        await sync.stop()

    assert market is not None
    assert market.daily_volume == Decimal("30000")
    assert market.volume is not None
    assert market.volume.acquired_at == now.isoformat()


@pytest.mark.asyncio
async def test_clob_cache_record_never_carries_the_volume(fake_redis: FakeAsyncRedis) -> None:
    sync = _sync(fake_redis, FakeGammaClient({CID: _stats()}))
    await sync.start()
    await sync.stop()

    stored = json.loads(await fake_redis.get(f"{DEFAULT_REDIS_KEY_PREFIX}{CID}"))

    assert (stored["volume"], stored["daily_volume"]) == (None, None)


@pytest.mark.asyncio
async def test_clob_resync_never_replaces_a_stamped_volume(fake_redis: FakeAsyncRedis) -> None:
    sync = _sync(fake_redis, FakeGammaClient({CID: _stats()}))
    await sync.start()
    try:
        await sync.force_sync()
        market = await sync.get_cached_market(CID)
    finally:
        await sync.stop()

    assert market is not None
    assert market.volume is not None
    assert market.volume.raw == "30000"


@pytest.mark.asyncio
async def test_miss_path_makes_no_volume_claim_and_names_its_reason(
    fake_redis: FakeAsyncRedis,
) -> None:
    sync = _sync(fake_redis)

    market = await sync.get_market(CID)

    assert market is not None
    assert (market.volume, market.volume_unavailable_reason) == (None, "clob_miss_path")
    assert await fake_redis.exists(DEFAULT_VOLUME_KEY_PREFIX + CID) == 0


@pytest.mark.asyncio
async def test_miss_path_uses_a_stamped_record_when_one_exists(fake_redis: FakeAsyncRedis) -> None:
    await VolumeStore(fake_redis).write_crawl(_crawl(_stats()), STAMP)

    market = await _sync(fake_redis).get_market(CID)

    assert market is not None
    assert market.volume is not None
    assert market.volume.source == "gamma-markets-v1"


@pytest.mark.asyncio
async def test_miss_path_write_after_a_sync_write_leaves_the_record_intact(
    fake_redis: FakeAsyncRedis,
) -> None:
    sync = _sync(fake_redis)
    key = f"{DEFAULT_REDIS_KEY_PREFIX}{CID}"
    winner = MarketMetadata.from_market(MARKET)
    await sync._cache_market_if_absent(winner)
    before = await fake_redis.get(key)

    await sync._cache_market_if_absent(MarketMetadata(CID, "late", "", ()))

    assert await fake_redis.get(key) == before


@pytest.mark.asyncio
async def test_legacy_cached_value_upgrades_to_unqualified_then_to_the_stamped_record(
    fake_redis: FakeAsyncRedis,
) -> None:
    legacy = {**MarketMetadata.from_market(MARKET).to_dict(), "daily_volume": "30000"}
    del legacy["volume"]
    await fake_redis.set(f"{DEFAULT_REDIS_KEY_PREFIX}{CID}", json.dumps(legacy))
    sync = _sync(fake_redis)

    before = await sync.get_cached_market(CID)
    await VolumeStore(fake_redis).write_crawl(_crawl(_stats()), STAMP)
    after = await sync.get_cached_market(CID)

    assert before is not None and before.volume is not None
    assert (before.volume.source, before.volume.acquired_at) == (None, None)
    assert after is not None and after.volume is not None
    assert after.volume.source == "gamma-markets-v1"


@pytest.mark.asyncio
async def test_category_scan_merges_the_volume_too(fake_redis: FakeAsyncRedis) -> None:
    sync = _sync(fake_redis, FakeGammaClient({CID: _stats()}))
    await sync.start()
    try:
        found = await sync.get_markets_by_category(MarketMetadata.from_market(MARKET).category)
    finally:
        await sync.stop()

    assert [m.volume is not None for m in found] == [True]


class UnreadableVolumeRedis(FakeAsyncRedis):
    """Serves the CLOB record but fails every read of a volume key."""

    async def get(self, name: KeyT) -> object:
        if str(name).startswith(DEFAULT_VOLUME_KEY_PREFIX):
            raise ConnectionError("volume read failed")
        return await super().get(name)


@pytest.mark.asyncio
async def test_unreadable_volume_store_leaves_the_clob_metadata_usable() -> None:
    redis = UnreadableVolumeRedis()
    await redis.set(
        f"{DEFAULT_REDIS_KEY_PREFIX}{CID}", json.dumps(MarketMetadata.from_market(MARKET).to_dict())
    )

    market = await _sync(redis).get_cached_market(CID)

    assert market is not None
    assert (market.question, market.volume) == (MARKET.question, None)
    assert market.volume_unavailable_reason == "no_volume_record"


@pytest.mark.asyncio
async def test_non_utf8_volume_bytes_are_not_a_record(fake_redis: FakeAsyncRedis) -> None:
    await fake_redis.set(DEFAULT_VOLUME_KEY_PREFIX + CID, b"\xff\xfe")
    await fake_redis.set(DEFAULT_VOLUME_KEY_PREFIX + "_crawl", b"\xff\xfe")

    merged = await VolumeStore(fake_redis).merge(MarketMetadata.from_market(MARKET))

    assert (merged.volume, merged.volume_unavailable_reason) == (None, "no_volume_record")


@pytest.mark.asyncio
async def test_a_later_gamma_page_without_the_key_replaces_the_value_as_missing(
    fake_redis: FakeAsyncRedis,
) -> None:
    """A Gamma object without `volume24hr` is a fresh observation of absence, not a CLOB write."""
    store = VolumeStore(fake_redis)
    await store.write_crawl(_crawl(_stats()), STAMP)
    absent = GammaMarketStats(
        CID, None, None, None, None, None, STAMP, False, None, "gamma-markets-v1"
    )

    await store.write_crawl(_crawl(absent), STAMP)

    record = await store.read(CID)
    assert record is not None
    assert (record.observation.present, record.observation.raw) == (False, None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("age_seconds", "legacy_kept"),
    [(0, True), (599, True), (600, True), (601, False), (1200, False), (-1, False)],
)
async def test_legacy_volume_fields_are_kept_only_within_600_seconds(
    fake_redis: FakeAsyncRedis, age_seconds: int, legacy_kept: bool
) -> None:
    await VolumeStore(fake_redis).write_crawl(_crawl(_stats()), STAMP)
    read_at = STAMP + timedelta(seconds=age_seconds)

    merged = await VolumeStore(fake_redis, clock=lambda: read_at).merge(MARKET_METADATA)

    legacy = (merged.daily_volume, merged.weekly_volume, merged.liquidity)
    expected = (Decimal("30000"), Decimal("100000"), Decimal("42")) if legacy_kept else (None,) * 3
    assert legacy == expected
    assert merged.volume is not None
    assert merged.volume.acquired_at == STAMP.isoformat()


@pytest.mark.asyncio
async def test_a_fresh_record_gives_the_scorer_the_inputs_main_gave_it(
    fake_redis: FakeAsyncRedis,
) -> None:
    await VolumeStore(fake_redis).write_crawl(_crawl(_stats()), STAMP)

    merged = await VolumeStore(fake_redis, clock=lambda: STAMP).merge(MARKET_METADATA)

    assert (merged.daily_volume, merged.weekly_volume, merged.liquidity) == (
        Decimal("30000"),
        Decimal("100000"),
        Decimal("42"),
    )
