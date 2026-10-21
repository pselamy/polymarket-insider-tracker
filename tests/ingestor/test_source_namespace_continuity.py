"""Continuity of the logical source namespace across the official v1/v2 endpoint pair.

Contract item 4 (``specs/001-supported-trade-ingestion/spec.md``, 2026-10-18 amendment): the exact
official v1/v2 default endpoint pair with the same coverage mode keeps the existing v1 logical
source namespace, so a saved checkpoint and identity window survive the endpoint upgrade. Custom
URLs and coverage modes are never merged, and a corrupt checkpoint fails visibly rather than being
cleared or silently replaced.
"""

from __future__ import annotations

import hashlib

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.ingestor.observation_boundary import (
    BoundarySchemaError,
    ObservationBoundary,
    source_id,
)
from tests.fakes.redis_await import write_hash

OFFICIAL_V1 = "https://data-api.polymarket.com/trades"
OFFICIAL_V2 = "https://data-api.polymarket.com/v2/trades"
CUSTOM = "https://proxy.invalid/feed"
OTHER_CUSTOM = "https://other.invalid/trades"
HORIZON = 600
COVERAGES = ("all", "taker-only")


def _namespace(trades_url: str, coverage: str) -> str:
    return source_id(trades_url, coverage)


def _expected(trades_url: str, coverage: str) -> str:
    return hashlib.sha256(f"{trades_url}|{coverage}".encode()).hexdigest()[:12]


def _boundary(redis: FakeAsyncRedis, trades_url: str, coverage: str = "all") -> ObservationBoundary:
    return ObservationBoundary(
        redis, trades_url=trades_url, coverage=coverage, horizon_seconds=HORIZON
    )


class TestOfficialPairContinuity:
    def test_official_v2_url_reuses_the_v1_namespace_for_each_coverage(self) -> None:
        for coverage in COVERAGES:
            assert _namespace(OFFICIAL_V2, coverage) == _namespace(OFFICIAL_V1, coverage)

    def test_coverage_modes_stay_distinct_across_the_pair(self) -> None:
        for trades_url in (OFFICIAL_V1, OFFICIAL_V2):
            assert _namespace(trades_url, "all") != _namespace(trades_url, "taker-only")

    async def test_v1_and_v2_boundaries_share_every_durable_key(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        v1 = _boundary(fake_redis, OFFICIAL_V1)
        v2 = _boundary(fake_redis, OFFICIAL_V2)

        assert v1.checkpoint_key == v2.checkpoint_key
        assert v1.identities_key == v2.identities_key
        assert v1.loss_events_key == v2.loss_events_key


class TestExistingNamespaceValuesUnchanged:
    def test_official_v1_namespace_is_byte_identical_to_before(self) -> None:
        for coverage in COVERAGES:
            assert _namespace(OFFICIAL_V1, coverage) == _expected(OFFICIAL_V1, coverage)

    def test_custom_namespace_is_byte_identical_to_before(self) -> None:
        for coverage in COVERAGES:
            assert _namespace(CUSTOM, coverage) == _expected(CUSTOM, coverage)


class TestCustomUrlsNeverMerge:
    def test_custom_url_is_not_merged_with_the_official_pair(self) -> None:
        for official in (OFFICIAL_V1, OFFICIAL_V2):
            assert _namespace(CUSTOM, "all") != _namespace(official, "all")

    def test_distinct_custom_urls_are_not_merged(self) -> None:
        assert _namespace(CUSTOM, "all") != _namespace(OTHER_CUSTOM, "all")


class TestCorruptCheckpointFailsVisibly:
    async def test_corrupt_checkpoint_on_the_shared_key_raises_and_is_never_cleared(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        key = f"polymarket:ingest:{_namespace(OFFICIAL_V1, 'all')}:checkpoint"
        await write_hash(fake_redis, key, {"schema_version": "999", "boundary_time": "1"})

        boundary = _boundary(fake_redis, OFFICIAL_V2)

        assert boundary.checkpoint_key == key
        with pytest.raises(BoundarySchemaError):
            await boundary.load()
        assert await fake_redis.exists(key) == 1
