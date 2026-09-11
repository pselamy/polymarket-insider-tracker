"""Shared behavioral contract for every Redis operation the product exercises.

The same scenarios run against ``fakeredis`` (always) and against a real loopback Redis when the
services profile selects it with ``RUN_SERVICE_TESTS=1``. Selecting the real service is fail-closed:
an unreachable or non-loopback ``REDIS_URL`` fails the run instead of skipping it. Every scenario
works inside a unique key namespace that is deleted afterwards, so a shared local Redis is never
flushed.

Covered because the product relies on it: string values and their bytes encoding, ``SET NX EX``
deduplication and TTL rules, sorted-set score ranges and ordering, transactional pipelines, stream
append/consumer-group/pending/ack/trim lifecycles including deleted-entry tombstones, error types,
pattern scans, and the ingestion boundary operations: the checkpoint hash, identity sorted-set
range/trim/cardinality, loss-event list push/trim/range, and the transactional advance pipeline.
"""

from __future__ import annotations

import importlib.util
import os
import uuid
from asyncio import CancelledError
from collections.abc import AsyncIterator
from pathlib import Path
from types import ModuleType

import pytest
from fakeredis import FakeAsyncRedis, FakeServer
from redis.asyncio import Redis
from redis.exceptions import ResponseError, WatchError

from polymarket_insider_tracker.ingestor.observation_boundary import (
    BoundaryOrigin,
    BoundarySchemaError,
    ObservationBoundary,
)

RUNTIME_SERVICES_PATH = Path(__file__).parents[2] / "scripts" / "runtime_services.py"
LOCAL_REDIS_URL = "redis://localhost:6379"
REAL_REDIS_SELECTED = os.environ.get("RUN_SERVICE_TESTS") == "1"
IMPLEMENTATIONS = ["fake", "real"] if REAL_REDIS_SELECTED else ["fake"]


def _runtime_services() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "runtime_services_contract", RUNTIME_SERVICES_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _connect_real_redis() -> Redis:
    """Connect to the loopback Redis named by ``REDIS_URL``; never skip, never echo the URL."""
    url = _runtime_services().validate_loopback_redis_url(
        os.environ.get("REDIS_URL", LOCAL_REDIS_URL)
    )
    client = Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
    try:
        assert await client.ping() is True
    except BaseException:
        await client.aclose()
        raise
    return client


class ContractRedis:
    """A Redis client plus the unique namespace this test may write to."""

    def __init__(
        self, client: Redis, implementation: str, fake_server: FakeServer | None = None
    ) -> None:
        self.client = client
        self.implementation = implementation
        self.fake_server = fake_server
        self.prefix = f"pit-contract:{uuid.uuid4().hex}:"

    def key(self, name: str) -> str:
        return f"{self.prefix}{name}"

    async def cleanup(self) -> None:
        async for key in self.client.scan_iter(match=f"{self.prefix}*"):
            await self.client.delete(key)

    async def close(self) -> None:
        try:
            await self.cleanup()
            remaining = [key async for key in self.client.scan_iter(match=f"{self.prefix}*")]
            assert remaining == []
        finally:
            await self.client.aclose()


@pytest.fixture(params=IMPLEMENTATIONS)
async def contract(request: pytest.FixtureRequest) -> AsyncIterator[ContractRedis]:
    implementation = str(request.param)
    fake_server = FakeServer() if implementation == "fake" else None
    client = (
        FakeAsyncRedis(server=fake_server)
        if fake_server is not None
        else await _connect_real_redis()
    )
    scoped = ContractRedis(client, implementation, fake_server)
    try:
        yield scoped
    finally:
        await scoped.close()


class FailedHandshakeRedis(FakeAsyncRedis):
    """Inject a setup failure and expose whether the client was closed afterwards."""

    def __init__(self, failure: BaseException) -> None:
        super().__init__()
        self.failure = failure
        self.closed = False

    async def ping(self):
        raise self.failure

    async def aclose(self, close_connection_pool=None) -> None:
        await super().aclose(close_connection_pool=close_connection_pool)
        self.closed = True


@pytest.mark.parametrize("failure", [ResponseError("rejected"), CancelledError()])
async def test_failed_real_client_setup_always_closes(monkeypatch, failure) -> None:
    client = FailedHandshakeRedis(failure)
    monkeypatch.setenv("REDIS_URL", LOCAL_REDIS_URL)
    monkeypatch.setattr(Redis, "from_url", lambda _url, **_options: client)

    with pytest.raises(type(failure)):
        await _connect_real_redis()

    assert client.closed is True


class FailedCleanupContract(ContractRedis):
    async def cleanup(self) -> None:
        raise ResponseError("cleanup unavailable")


async def test_failed_namespace_cleanup_still_closes_client() -> None:
    client = FailedHandshakeRedis(ResponseError("unused handshake"))
    scoped = FailedCleanupContract(client, "fake")

    with pytest.raises(ResponseError, match="cleanup unavailable"):
        await scoped.close()

    assert client.closed is True


async def test_string_values_round_trip_as_bytes(contract: ContractRedis) -> None:
    redis, key = contract.client, contract.key("string")

    assert await redis.get(key) is None
    assert await redis.exists(key) == 0
    assert await redis.set(key, "text value") is True
    assert await redis.get(key) == b"text value"
    assert await redis.set(key, b"\x00binary\xff") is True
    assert await redis.get(key) == b"\x00binary\xff"
    assert await redis.exists(key) == 1
    assert await redis.delete(key, contract.key("absent")) == 1
    assert await redis.get(key) is None


async def test_set_nx_ex_and_ttl_rules(contract: ContractRedis) -> None:
    redis, key = contract.client, contract.key("dedup")

    assert await redis.ttl(key) == -2
    assert await redis.set(key, "first", nx=True, ex=3600) is True
    assert await redis.set(key, "second", nx=True, ex=3600) is None
    assert await redis.get(key) == b"first"
    assert 0 < await redis.ttl(key) <= 3600

    assert await redis.set(key, "plain overwrite") is True
    assert await redis.ttl(key) == -1
    assert await redis.expire(key, 120) is True
    assert 0 < await redis.ttl(key) <= 120
    assert await redis.expire(contract.key("absent"), 120) is False

    assert await redis.setex(contract.key("setex"), 60, "cached") is True
    assert 0 < await redis.ttl(contract.key("setex")) <= 60
    with pytest.raises(ResponseError, match="invalid expire time"):
        await redis.set(contract.key("invalid"), "v", ex=0)


async def test_key_types_are_exclusive(contract: ContractRedis) -> None:
    redis, key = contract.client, contract.key("typed")
    await redis.set(key, "string")

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await redis.zadd(key, {"member": 1.0})
    assert await redis.get(key) == b"string"


async def test_sorted_set_ranges_and_ordering(contract: ContractRedis) -> None:
    redis, key = contract.client, contract.key("index")

    assert await redis.zadd(key, {"b": 5.0, "a": 5.0, "c": 20.0, "d": 30.0}) == 4
    assert await redis.zrangebyscore(key, "-inf", "+inf") == [b"a", b"b", b"c", b"d"]
    assert await redis.zrangebyscore(key, 5.0, 20.0) == [b"a", b"b", b"c"]
    assert await redis.zrangebyscore(key, 6, 25, withscores=True) == [(b"c", 20.0)]
    assert await redis.zrangebyscore(key, "-inf", "+inf", start=1, num=2) == [b"b", b"c"]
    assert await redis.zcount(key, "-inf", "+inf") == 4
    assert await redis.zcount(key, 6, 30) == 2
    assert await redis.zremrangebyscore(key, "-inf", 5) == 2
    assert await redis.zrangebyscore(key, "-inf", "+inf") == [b"c", b"d"]
    assert await redis.zrangebyscore(contract.key("absent"), "-inf", "+inf") == []


async def test_pipeline_applies_every_command_in_order(contract: ContractRedis) -> None:
    redis = contract.client
    record, index = contract.key("record"), contract.key("time-index")

    async with redis.pipeline() as pipe:
        pipe.set(record, "payload", ex=300)
        pipe.zadd(index, {"alert-1": 10.0})
        pipe.expire(index, 300)
        pipe.get(record)
        results = await pipe.execute()

    assert results == [True, 1, True, b"payload"]
    assert 0 < await redis.ttl(index) <= 300

    batch = redis.pipeline()
    batch.xadd(contract.key("stream"), {"n": "1"}, maxlen=100)
    batch.xadd(contract.key("stream"), {"n": "2"}, maxlen=100)
    entry_ids = await batch.execute()

    assert [isinstance(entry_id, bytes) for entry_id in entry_ids] == [True, True]
    assert entry_ids[0] < entry_ids[1]
    assert await redis.xlen(contract.key("stream")) == 2


async def _read(redis: Redis, group: str, consumer: str, stream: str, start: str) -> list[object]:
    results = await redis.xreadgroup(group, consumer, {stream: start}, count=10)
    return list(results[0][1]) if results else []


async def test_stream_consumer_group_lifecycle(contract: ContractRedis) -> None:
    redis, stream = contract.client, contract.key("trades")

    assert await redis.xgroup_create(stream, "workers", id="0", mkstream=True) is True
    assert await redis.xlen(stream) == 0
    with pytest.raises(ResponseError, match="BUSYGROUP"):
        await redis.xgroup_create(stream, "workers", id="0", mkstream=True)

    first = await redis.xadd(stream, {"trade_id": "t1"}, maxlen=1000)
    second = await redis.xadd(stream, {"trade_id": "t2"}, maxlen=1000)
    assert isinstance(first, bytes) and first < second

    delivered = await _read(redis, "workers", "worker-1", stream, ">")
    assert delivered == [(first, {b"trade_id": b"t1"}), (second, {b"trade_id": b"t2"})]
    assert await _read(redis, "workers", "worker-1", stream, ">") == []
    assert [entry[0] for entry in await _read(redis, "workers", "worker-1", stream, "0")] == [
        first,
        second,
    ]

    assert await redis.xack(stream, "workers", first) == 1
    assert await redis.xack(stream, "workers", first) == 0
    assert [entry[0] for entry in await _read(redis, "workers", "worker-1", stream, "0")] == [
        second
    ]


async def test_deleted_pending_entries_read_back_as_empty_records(contract: ContractRedis) -> None:
    redis, stream = contract.client, contract.key("tombstones")
    await redis.xgroup_create(stream, "workers", id="0", mkstream=True)
    first = await redis.xadd(stream, {"trade_id": "t1"})
    second = await redis.xadd(stream, {"trade_id": "t2"})
    await _read(redis, "workers", "worker-1", stream, ">")

    assert await redis.xdel(stream, first) == 1

    assert await _read(redis, "workers", "worker-1", stream, "0") == [
        (first, {}),
        (second, {b"trade_id": b"t2"}),
    ]
    assert await _read(redis, "workers", "worker-1", stream, ">") == []


async def test_stream_trim_info_and_error_types(contract: ContractRedis) -> None:
    redis, stream = contract.client, contract.key("bounded")
    for number in range(5):
        await redis.xadd(stream, {"n": str(number)})

    assert await redis.xtrim(stream, maxlen=2, approximate=False) == 3
    assert await redis.xlen(stream) == 2
    assert await redis.xtrim(stream, maxlen=1) >= 0
    assert await redis.xlen(stream) >= 1
    assert (await redis.xinfo_stream(stream))["length"] == await redis.xlen(stream)

    absent = contract.key("absent-stream")
    assert await redis.xlen(absent) == 0
    with pytest.raises(ResponseError, match="no such key"):
        await redis.xinfo_stream(absent)
    with pytest.raises(ResponseError, match="NOGROUP"):
        await redis.xreadgroup("missing", "worker-1", {stream: ">"}, count=1)
    assert await redis.xtrim(absent, maxlen=1) == 0


async def test_scan_matches_only_the_requested_pattern(contract: ContractRedis) -> None:
    redis = contract.client
    await redis.set(contract.key("market:one"), "1")
    await redis.set(contract.key("market:two"), "2")
    await redis.set(contract.key("other"), "3")

    matched = [key async for key in redis.scan_iter(match=contract.key("market:*"), count=1)]

    assert sorted(matched) == [
        contract.key("market:one").encode(),
        contract.key("market:two").encode(),
    ]
    others = [key async for key in redis.scan_iter(match=contract.key("other"), count=1)]
    assert others == [contract.key("other").encode()]


async def test_checkpoint_hash_round_trips_as_bytes(contract: ContractRedis) -> None:
    """The ingestion checkpoint is a hash of integer and enum fields read back as bytes."""
    redis, key = contract.client, contract.key("ingest:checkpoint")

    assert await redis.hgetall(key) == {}
    assert (
        await redis.hset(
            key,
            mapping={
                "schema_version": 2,
                "boundary_time": 1788983716,
                "emission_floor": 1788983716,
                "boundary_origin": "first-start",
                "written_at": 1788983720,
                "coverage": "all",
                "last_request_end": 1788983720,
            },
        )
        == 7
    )
    assert await redis.hgetall(key) == {
        b"schema_version": b"2",
        b"boundary_time": b"1788983716",
        b"emission_floor": b"1788983716",
        b"boundary_origin": b"first-start",
        b"written_at": b"1788983720",
        b"coverage": b"all",
        b"last_request_end": b"1788983720",
    }
    assert await redis.hset(key, mapping={"boundary_time": 1788983800}) == 0
    assert await redis.hget(key, "boundary_time") == b"1788983800"
    assert await redis.hget(key, "absent") is None


async def test_identity_window_trim_and_cardinality(contract: ContractRedis) -> None:
    """Identities are scored by provider timestamp, trimmed below a floor, and counted."""
    redis, key = contract.client, contract.key("ingest:identities")
    members = {f"id-{n}": float(1000 + n) for n in range(6)}

    assert await redis.zadd(key, members) == 6
    assert await redis.zadd(key, {"id-0": 1000.0}) == 0
    assert await redis.zcard(key) == 6
    assert await redis.zrangebyscore(key, 1003, "+inf") == [b"id-3", b"id-4", b"id-5"]
    scored = await redis.zrangebyscore(key, "-inf", "+inf", withscores=True)
    assert scored[:2] == [(b"id-0", 1000.0), (b"id-1", 1001.0)]
    assert await redis.zremrangebyscore(key, "-inf", "(1002") == 2
    assert await redis.zcard(key) == 4
    assert await redis.zrangebyscore(key, "-inf", "+inf") == [b"id-2", b"id-3", b"id-4", b"id-5"]
    assert await redis.zcard(contract.key("absent")) == 0


async def test_loss_event_list_is_newest_first_and_bounded(contract: ContractRedis) -> None:
    """Loss events are pushed newest-first and trimmed to a bounded retention."""
    redis, key = contract.client, contract.key("ingest:loss-events")

    assert await redis.lrange(key, 0, -1) == []
    assert await redis.lpush(key, '{"n": 1}') == 1
    assert await redis.lpush(key, '{"n": 2}', '{"n": 3}') == 3
    assert await redis.lrange(key, 0, -1) == [b'{"n": 3}', b'{"n": 2}', b'{"n": 1}']
    assert await redis.ltrim(key, 0, 1) is True
    assert await redis.lrange(key, 0, -1) == [b'{"n": 3}', b'{"n": 2}']
    assert await redis.lrange(key, 0, 4) == [b'{"n": 3}', b'{"n": 2}']
    assert await redis.llen(key) == 2


async def test_boundary_advance_pipeline_is_transactional(contract: ContractRedis) -> None:
    """One MULTI/EXEC pipeline writes the checkpoint, identities, trim, and loss events."""
    redis = contract.client
    checkpoint = contract.key("ingest:checkpoint")
    identities = contract.key("ingest:identities")
    losses = contract.key("ingest:loss-events")
    await redis.zadd(identities, {"old": 100.0, "kept": 500.0})

    async with redis.pipeline(transaction=True) as pipe:
        pipe.hset(checkpoint, mapping={"boundary_time": 900, "boundary_origin": "proven"})
        pipe.zadd(identities, {"new-a": 850.0, "new-b": 900.0})
        pipe.zremrangebyscore(identities, "-inf", "(300")
        pipe.lpush(losses, '{"reason": "horizon-expired"}')
        pipe.ltrim(losses, 0, 49)
        results = await pipe.execute()

    assert results == [2, 2, 1, 1, True]
    assert await redis.hget(checkpoint, "boundary_time") == b"900"
    assert await redis.zrangebyscore(identities, "-inf", "+inf") == [b"kept", b"new-a", b"new-b"]
    assert await redis.lrange(losses, 0, -1) == [b'{"reason": "horizon-expired"}']

    async with redis.pipeline(transaction=True) as pipe:
        pipe.hset(checkpoint, mapping={"boundary_time": 950})
        pipe.zadd(identities, {"new-c": 950.0})
        pipe.zremrangebyscore(identities, "-inf", "(350")
        pipe.lpush(losses, '{"reason": "restart-beyond-horizon"}')
        pipe.ltrim(losses, 0, 0)
        assert await pipe.execute() == [0, 1, 0, 2, True]

    assert await redis.hget(checkpoint, "boundary_time") == b"950"
    assert await redis.zcard(identities) == 4
    assert await redis.lrange(losses, 0, -1) == [b'{"reason": "restart-beyond-horizon"}']


async def test_watched_type_change_aborts_before_transaction_writes(
    contract: ContractRedis,
) -> None:
    redis = contract.client
    watched = contract.key("watched")
    transaction_owned = contract.key("transaction-owned")
    competitor = (
        FakeAsyncRedis(server=contract.fake_server)
        if contract.fake_server is not None
        else await _connect_real_redis()
    )
    try:
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(watched)
            assert await pipe.type(watched) == b"none"
            await competitor.set(watched, "wrong-type")
            pipe.multi()
            pipe.hset(watched, mapping={"field": "value"})
            pipe.set(transaction_owned, "must-not-exist")
            with pytest.raises(WatchError):
                await pipe.execute()
        assert await redis.get(watched) == b"wrong-type"
        assert await redis.exists(transaction_owned) == 0
    finally:
        await competitor.aclose()


async def test_compare_and_delete_removes_only_the_owned_value(contract: ContractRedis) -> None:
    """The delivery-claim release sequence: WATCH, GET, compare, MULTI DELETE.

    ``AlertHistory.release_channel_claim`` relies on exactly this sequence to delete a
    claim only while its ownership token is still the stored value.
    """
    redis, key = contract.client, contract.key("claim")
    await redis.set(key, "owner-token", ex=60)

    async with redis.pipeline(transaction=True) as pipe:
        await pipe.watch(key)
        assert await pipe.get(key) == b"owner-token"
        pipe.multi()
        pipe.delete(key)
        assert await pipe.execute() == [1]

    assert await redis.exists(key) == 0


async def test_compare_and_delete_aborts_when_the_value_changes_under_watch(
    contract: ContractRedis,
) -> None:
    """A claim re-acquired by another owner between WATCH and EXEC must survive."""
    redis, key = contract.client, contract.key("claim")
    await redis.set(key, "stale-token", ex=60)
    competitor = (
        FakeAsyncRedis(server=contract.fake_server)
        if contract.fake_server is not None
        else await _connect_real_redis()
    )
    try:
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            assert await pipe.get(key) == b"stale-token"
            await competitor.set(key, "new-owner-token", ex=60)
            pipe.multi()
            pipe.delete(key)
            with pytest.raises(WatchError):
                await pipe.execute()
        assert await redis.get(key) == b"new-owner-token"
    finally:
        await competitor.aclose()


async def test_compare_and_expire_extends_only_the_owned_value(contract: ContractRedis) -> None:
    """The claim-renewal sequence: WATCH, GET, compare, MULTI EXPIRE.

    ``AlertHistory.extend_channel_claim`` relies on exactly this sequence to renew a
    claim's lease only while its ownership token is still the stored value.
    """
    redis, key = contract.client, contract.key("claim-extend")
    await redis.set(key, "owner-token", ex=2)

    async with redis.pipeline(transaction=True) as pipe:
        await pipe.watch(key)
        assert await pipe.get(key) == b"owner-token"
        pipe.multi()
        pipe.expire(key, 120)
        assert await pipe.execute() == [True]

    assert await redis.ttl(key) > 2
    assert await redis.get(key) == b"owner-token"


async def test_compare_and_expire_aborts_when_the_value_changes_under_watch(
    contract: ContractRedis,
) -> None:
    """A claim re-acquired by another owner between WATCH and EXEC keeps its own lease."""
    redis, key = contract.client, contract.key("claim-extend")
    await redis.set(key, "stale-token", ex=30)
    competitor = (
        FakeAsyncRedis(server=contract.fake_server)
        if contract.fake_server is not None
        else await _connect_real_redis()
    )
    try:
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            assert await pipe.get(key) == b"stale-token"
            await competitor.set(key, "new-owner-token", ex=30)
            pipe.multi()
            pipe.expire(key, 600)
            with pytest.raises(WatchError):
                await pipe.execute()
        assert await redis.get(key) == b"new-owner-token"
        assert await redis.ttl(key) <= 30
    finally:
        await competitor.aclose()


@pytest.mark.parametrize("key_name", ["checkpoint_key", "identities_key", "loss_events_key"])
async def test_observation_boundary_rejects_wrong_key_types_atomically(
    contract: ContractRedis, key_name: str
) -> None:
    boundary = ObservationBoundary(
        contract.client,
        trades_url=f"https://trades.invalid/{contract.prefix}",
        coverage="all",
        horizon_seconds=600,
        clock=lambda: 1_000.0,
    )
    keys = (boundary.checkpoint_key, boundary.identities_key, boundary.loss_events_key)
    try:
        await contract.client.set(getattr(boundary, key_name), "wrong-type")

        with pytest.raises(BoundarySchemaError, match="has type string"):
            await boundary.advance(
                boundary_time=1_000,
                origin=BoundaryOrigin.FIRST_START,
                last_request_end=1_000,
                identities={"new": 1_000},
            )

        assert boundary.checkpoint is None
        assert await contract.client.exists(boundary.checkpoint_key) == (
            1 if key_name == "checkpoint_key" else 0
        )
        await contract.client.delete(*keys)
        checkpoint = await boundary.advance(
            boundary_time=1_000,
            origin=BoundaryOrigin.FIRST_START,
            last_request_end=1_000,
            identities={"new": 1_000},
        )
        assert checkpoint.boundary_time == 1_000
    finally:
        await contract.client.delete(*keys)
