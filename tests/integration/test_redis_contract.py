"""Shared behavioral contract for every Redis operation the product exercises.

The same scenarios run against ``fakeredis`` (always) and against a real loopback Redis when the
services profile selects it with ``RUN_SERVICE_TESTS=1``. Selecting the real service is fail-closed:
an unreachable or non-loopback ``REDIS_URL`` fails the run instead of skipping it. Every scenario
works inside a unique key namespace that is deleted afterwards, so a shared local Redis is never
flushed.

Covered because the product relies on it: string values and their bytes encoding, ``SET NX EX``
deduplication and TTL rules, sorted-set score ranges and ordering, transactional pipelines, stream
append/consumer-group/pending/ack/trim lifecycles including deleted-entry tombstones, error types,
and pattern scans.
"""

from __future__ import annotations

import importlib.util
import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from types import ModuleType

import pytest
from fakeredis import FakeAsyncRedis
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

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
    client = Redis.from_url(url)
    try:
        assert await client.ping() is True
    except RedisConnectionError as exc:
        await client.aclose()
        pytest.fail(f"RUN_SERVICE_TESTS=1 selected a real Redis that is unreachable: {exc!s}")
    return client


class ContractRedis:
    """A Redis client plus the unique namespace this test may write to."""

    def __init__(self, client: Redis, implementation: str) -> None:
        self.client = client
        self.implementation = implementation
        self.prefix = f"pit-contract:{uuid.uuid4().hex}:"

    def key(self, name: str) -> str:
        return f"{self.prefix}{name}"

    async def cleanup(self) -> None:
        async for key in self.client.scan_iter(match=f"{self.prefix}*"):
            await self.client.delete(key)


@pytest.fixture(params=IMPLEMENTATIONS)
async def contract(request: pytest.FixtureRequest) -> AsyncIterator[ContractRedis]:
    implementation = str(request.param)
    client = FakeAsyncRedis() if implementation == "fake" else await _connect_real_redis()
    scoped = ContractRedis(client, implementation)
    try:
        yield scoped
    finally:
        await scoped.cleanup()
        remaining = [key async for key in client.scan_iter(match=f"{scoped.prefix}*")]
        await client.aclose()
        assert remaining == []


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
    cursor, page = await redis.scan(0, match=contract.key("other"), count=10)
    assert isinstance(cursor, int)
    assert page == [contract.key("other").encode()]
