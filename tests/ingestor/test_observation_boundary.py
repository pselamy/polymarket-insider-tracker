"""Durable observation boundary: checkpoint, identity window, loss events, and the proof rule."""

from __future__ import annotations

import hashlib
import json

import pytest
from fakeredis import FakeAsyncRedis
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from polymarket_insider_tracker.ingestor.observation_boundary import (
    LOSS_EVENT_RETENTION,
    SCHEMA_VERSION,
    BoundaryOrigin,
    BoundarySchemaError,
    Checkpoint,
    LossEvent,
    LossReason,
    ObservationBoundary,
    ProofResult,
    source_id,
)
from tests.fakes import FakeClock

URL = "https://trades.invalid/trades"
NOW = 1_788_983_720
HORIZON = 600


def _boundary(
    redis: Redis, *, coverage: str = "all", clock: FakeClock | None = None
) -> ObservationBoundary:
    return ObservationBoundary(
        redis,
        trades_url=URL,
        coverage=coverage,
        horizon_seconds=HORIZON,
        clock=clock or FakeClock(NOW),
    )


async def _zset(redis: Redis, key: str) -> dict[str, int]:
    members = await redis.zrangebyscore(key, "-inf", "+inf", withscores=True)
    return {member.decode(): int(score) for member, score in members}


def _event(from_time: int, reason: LossReason = LossReason.HORIZON_EXPIRED) -> LossEvent:
    return LossEvent(
        from_time=from_time,
        to_time=from_time + 100,
        reason=reason,
        detected_at=NOW,
        recorded_at=NOW + 5,
        pages_examined=2,
    )


class TestNamespace:
    def test_source_id_is_the_sha256_prefix_of_url_and_coverage(self) -> None:
        expected = hashlib.sha256(f"{URL}|all".encode()).hexdigest()[:12]

        assert source_id(URL, "all") == expected
        assert source_id(URL, "taker-only") != expected
        assert source_id("https://other.invalid/trades", "all") != expected

    async def test_keys_live_under_the_documented_prefix(self, fake_redis: FakeAsyncRedis) -> None:
        boundary = _boundary(fake_redis)
        prefix = f"polymarket:ingest:{source_id(URL, 'all')}:"

        assert boundary.checkpoint_key == f"{prefix}checkpoint"
        assert boundary.identities_key == f"{prefix}identities"
        assert boundary.loss_events_key == f"{prefix}loss-events"


class TestLoad:
    async def test_absent_checkpoint_loads_as_none_with_an_empty_window(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        boundary = _boundary(fake_redis)

        assert await boundary.load() is None
        assert boundary.checkpoint is None
        assert dict(boundary.window) == {}
        assert boundary.loss_events == ()

    async def test_first_start_anchor_is_durable_and_reloads_the_window(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        boundary = _boundary(fake_redis, clock=FakeClock(NOW + 7))

        written = await boundary.advance(
            boundary_time=NOW,
            origin=BoundaryOrigin.FIRST_START,
            last_request_end=NOW + 1,
            identities={"a": NOW - 10, "b": NOW},
        )

        assert written == Checkpoint(
            boundary_time=NOW,
            boundary_origin=BoundaryOrigin.FIRST_START,
            written_at=NOW + 7,
            coverage="all",
            last_request_end=NOW + 1,
        )
        assert await fake_redis.hgetall(boundary.checkpoint_key) == {
            b"schema_version": str(SCHEMA_VERSION).encode(),
            b"boundary_time": str(NOW).encode(),
            b"boundary_origin": b"first-start",
            b"written_at": str(NOW + 7).encode(),
            b"coverage": b"all",
            b"last_request_end": str(NOW + 1).encode(),
        }

        reloaded = _boundary(fake_redis)
        assert await reloaded.load() == written
        assert dict(reloaded.window) == {"a": NOW - 10, "b": NOW}
        assert reloaded.contains("a") is True
        assert reloaded.contains("zzz") is False
        assert dict(reloaded.window) == await _zset(fake_redis, boundary.identities_key)

    async def test_coverage_mismatch_is_treated_as_absent(self, fake_redis: FakeAsyncRedis) -> None:
        boundary = _boundary(fake_redis)
        await fake_redis.hset(
            boundary.checkpoint_key,
            mapping={
                "schema_version": SCHEMA_VERSION,
                "boundary_time": NOW,
                "boundary_origin": "proven",
                "written_at": NOW,
                "coverage": "taker-only",
                "last_request_end": NOW,
            },
        )
        await fake_redis.zadd(boundary.identities_key, {"stale": NOW})

        assert await boundary.load() is None
        assert dict(boundary.window) == {}

    @pytest.mark.parametrize(
        "mapping",
        [
            {"schema_version": 2, "boundary_time": NOW, "coverage": "all"},
            {"schema_version": "one", "boundary_time": NOW, "coverage": "all"},
            {"boundary_time": NOW, "coverage": "all"},
            {"schema_version": 1, "coverage": "all"},
            {
                "schema_version": 1,
                "boundary_time": NOW,
                "boundary_origin": "unexpected",
                "written_at": NOW,
                "coverage": "all",
                "last_request_end": NOW,
            },
        ],
    )
    async def test_unknown_or_incomplete_schema_is_terminal(
        self, fake_redis: FakeAsyncRedis, mapping: dict[str, object]
    ) -> None:
        boundary = _boundary(fake_redis)
        await fake_redis.hset(boundary.checkpoint_key, mapping=mapping)

        with pytest.raises(BoundarySchemaError):
            await boundary.load()

    async def test_loss_events_reload_newest_first(self, fake_redis: FakeAsyncRedis) -> None:
        boundary = _boundary(fake_redis)
        await boundary.advance(
            boundary_time=NOW,
            origin=BoundaryOrigin.RE_ANCHORED,
            last_request_end=NOW,
            identities={},
            loss_events=[_event(1), _event(2)],
        )

        reloaded = _boundary(fake_redis)
        await reloaded.load()

        assert reloaded.loss_events == (_event(2), _event(1))
        assert boundary.loss_events == (_event(2), _event(1))
        assert json.loads(_event(1).to_json()) == {
            "from_time": 1,
            "to_time": 101,
            "reason": "horizon-expired",
            "detected_at": NOW,
            "recorded_at": NOW + 5,
            "pages_examined": 2,
        }


class TestProof:
    async def test_reach_and_continuity_prove_a_page(self, fake_redis: FakeAsyncRedis) -> None:
        boundary = _boundary(fake_redis)
        await boundary.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=1_000,
            identities={"old": 700, "mid": 900, "newest": 1_000},
        )

        proven = boundary.prove(page_identities={"mid", "newest", "later"}, oldest=800)
        no_reach = boundary.prove(page_identities={"mid", "newest"}, oldest=1_000)
        no_continuity = boundary.prove(page_identities={"newest", "later"}, oldest=800)
        empty = boundary.prove(page_identities=set(), oldest=None)

        assert proven == ProofResult(reach=True, continuity=True, oldest=800)
        assert proven.proven is True
        assert no_reach == ProofResult(reach=False, continuity=True, oldest=1_000)
        assert no_continuity == ProofResult(
            reach=True, continuity=False, oldest=800, missing_identities=("mid",)
        )
        assert no_continuity.missing == 1
        assert empty == ProofResult(reach=False, continuity=True, oldest=None)
        assert empty.proven is False

    async def test_equal_second_rows_at_the_oldest_timestamp_are_not_expected(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        boundary = _boundary(fake_redis)
        await boundary.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=1_000,
            identities={"same-second-a": 800, "same-second-b": 800, "newer": 900},
        )

        proof = boundary.prove(page_identities={"same-second-b", "newer"}, oldest=800)

        assert proof.proven is True

    async def test_proof_against_an_explicit_provisional_boundary(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        boundary = _boundary(fake_redis)
        await boundary.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=1_000,
            identities={"newest": 1_000},
        )
        await boundary.record_identity("provisional", 1_200)

        against_durable = boundary.prove(page_identities={"provisional"}, oldest=1_100)
        against_provisional = boundary.prove(
            page_identities={"provisional"}, oldest=1_100, boundary_time=1_200
        )

        assert against_durable.reach is False
        assert against_provisional == ProofResult(reach=True, continuity=True, oldest=1_100)

    async def test_ignored_identities_do_not_break_continuity(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        boundary = _boundary(fake_redis)
        await boundary.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=1_000,
            identities={"gone": 900, "present": 950},
        )

        strict = boundary.prove(page_identities={"present"}, oldest=800)
        lenient = boundary.prove(page_identities={"present"}, oldest=800, ignored={"gone"})

        assert strict.missing_identities == ("gone",)
        assert lenient.proven is True

    async def test_prove_requires_a_boundary(self, fake_redis: FakeAsyncRedis) -> None:
        boundary = _boundary(fake_redis)

        with pytest.raises(BoundarySchemaError, match="no boundary"):
            boundary.prove(page_identities=set(), oldest=1)


class TestWrites:
    async def test_record_identity_is_written_immediately(self, fake_redis: FakeAsyncRedis) -> None:
        boundary = _boundary(fake_redis)

        await boundary.record_identity("one", NOW)

        assert dict(boundary.window) == {"one": NOW}
        assert await _zset(fake_redis, boundary.identities_key) == {"one": NOW}

    async def test_advance_trims_relative_to_the_new_boundary(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        boundary = _boundary(fake_redis)
        await boundary.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=1_000,
            identities={"drop": 300, "edge": 400, "keep": 900},
        )

        await boundary.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=1_001,
            identities={"new": 1_000},
        )

        assert dict(boundary.window) == {"edge": 400, "keep": 900, "new": 1_000}
        assert dict(boundary.window) == await _zset(fake_redis, boundary.identities_key)
        assert boundary.checkpoint is not None
        assert boundary.checkpoint.last_request_end == 1_001

    async def test_retain_writes_identities_without_moving_the_boundary(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        boundary = _boundary(fake_redis)
        await boundary.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=1_000,
            identities={"anchor": 1_000},
        )

        await boundary.retain(
            identities={"gap-side": 1_500, "stale": 700},
            trim_floor=800,
            last_request_end=1_500,
        )

        assert boundary.checkpoint is not None
        assert boundary.checkpoint.boundary_time == 1_000
        assert boundary.checkpoint.boundary_origin is BoundaryOrigin.PROVEN
        assert boundary.checkpoint.last_request_end == 1_500
        assert await fake_redis.hget(boundary.checkpoint_key, "boundary_time") == b"1000"
        assert await fake_redis.hget(boundary.checkpoint_key, "last_request_end") == b"1500"
        assert dict(boundary.window) == {"anchor": 1_000, "gap-side": 1_500}
        assert dict(boundary.window) == await _zset(fake_redis, boundary.identities_key)

    async def test_loss_events_are_capped_newest_first(self, fake_redis: FakeAsyncRedis) -> None:
        boundary = _boundary(fake_redis)

        await boundary.advance(
            boundary_time=NOW,
            origin=BoundaryOrigin.RE_ANCHORED,
            last_request_end=NOW,
            identities={},
            loss_events=[_event(n) for n in range(LOSS_EVENT_RETENTION + 10)],
        )

        stored = await fake_redis.lrange(boundary.loss_events_key, 0, -1)
        assert len(stored) == LOSS_EVENT_RETENTION
        assert LossEvent.from_json(stored[0]).from_time == LOSS_EVENT_RETENTION + 9
        assert len(boundary.loss_events) == LOSS_EVENT_RETENTION
        assert boundary.loss_events[0].from_time == LOSS_EVENT_RETENTION + 9
        assert boundary.loss_events[-1].from_time == 10

    async def test_failed_advance_leaves_memory_and_checkpoint_untouched(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        seeded = _boundary(fake_redis)
        await seeded.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.PROVEN,
            last_request_end=1_000,
            identities={"kept": 1_000},
        )
        unreachable = Redis.from_url("redis://127.0.0.1:1", socket_connect_timeout=0.2)
        boundary = _boundary(unreachable)
        boundary_state = ObservationBoundary(
            fake_redis, trades_url=URL, coverage="all", horizon_seconds=HORIZON
        )
        await boundary_state.load()
        boundary._checkpoint = boundary_state.checkpoint
        boundary._window.update(boundary_state.window)

        with pytest.raises(RedisConnectionError):
            await boundary.advance(
                boundary_time=2_000,
                origin=BoundaryOrigin.PROVEN,
                last_request_end=2_000,
                identities={"lost": 2_000},
            )
        with pytest.raises(RedisConnectionError):
            await boundary.record_identity("lost-too", 2_000)
        await unreachable.aclose()

        assert boundary.checkpoint == boundary_state.checkpoint
        assert dict(boundary.window) == {"kept": 1_000}
        assert await fake_redis.hget(seeded.checkpoint_key, "boundary_time") == b"1000"
