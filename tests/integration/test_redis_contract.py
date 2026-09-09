"""Shared behavioral contract tests for Redis implementations."""

import os
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis
from tests.fakes.redis import FakeRedis


async def _probe_real_redis() -> Redis | None:
    """Return connected real Redis instance if environment supplies reachable service."""
    url = os.environ.get("REDIS_URL")
    if not url:
        return None
    try:
        client = Redis.from_url(url)
        if await client.ping():
            return client
        await client.aclose()
        return None
    except Exception:
        return None


@pytest.fixture
async def fake_redis_client() -> AsyncIterator[FakeRedis]:
    """Provide isolated FakeRedis instance."""
    client = FakeRedis()
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_real_redis_parity_when_available() -> None:
    """Verify real Redis parity when service profile is active."""
    client = await _probe_real_redis()
    if client is None:
        pytest.skip("Real Redis service not available in local environment")
    try:
        assert await client.ping() is True
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_fake_redis_string_lifecycle(fake_redis_client: FakeRedis) -> None:
    """Verify string set, get, exists, and delete contract."""
    redis = fake_redis_client
    assert await redis.get("test:key") is None
    assert await redis.exists("test:key") == 0

    assert await redis.set("test:key", "value123") is True
    assert await redis.exists("test:key") == 1
    assert await redis.get("test:key") == b"value123"

    assert await redis.delete("test:key") == 1
    assert await redis.get("test:key") is None
    assert await redis.exists("test:key") == 0


@pytest.mark.asyncio
async def test_fake_redis_ttl_and_expire(fake_redis_client: FakeRedis) -> None:
    """Verify ttl and expire contract."""
    redis = fake_redis_client
    assert await redis.ttl("test:missing") == -2

    await redis.set("test:key", "abc")
    assert await redis.ttl("test:key") == -1

    assert await redis.expire("test:key", 3600) is True
    assert await redis.ttl("test:key") > 0


@pytest.mark.asyncio
async def test_fake_redis_hash_contract(fake_redis_client: FakeRedis) -> None:
    """Verify hash hset, hget, hgetall, and hdel contract."""
    redis = fake_redis_client
    added = await redis.hset("test:hash", mapping={"f1": "v1", "f2": "v2"})
    assert added == 2

    assert await redis.hget("test:hash", "f1") == b"v1"
    all_fields = await redis.hgetall("test:hash")
    assert all_fields == {b"f1": b"v1", b"f2": b"v2"}

    assert await redis.hdel("test:hash", "f1") == 1
    assert await redis.hget("test:hash", "f1") is None
    assert await redis.hget("test:hash", "f2") == b"v2"


@pytest.mark.asyncio
async def test_fake_redis_set_contract(fake_redis_client: FakeRedis) -> None:
    """Verify set sadd, smembers, sismember, and srem contract."""
    redis = fake_redis_client
    assert await redis.sadd("test:set", "m1", "m2") == 2
    assert await redis.sismember("test:set", "m1") is True
    assert await redis.sismember("test:set", "m3") is False

    members = await redis.smembers("test:set")
    assert members == {b"m1", b"m2"}

    assert await redis.srem("test:set", "m1") == 1
    assert await redis.sismember("test:set", "m1") is False


@pytest.mark.asyncio
async def test_fake_redis_sorted_set_contract(fake_redis_client: FakeRedis) -> None:
    """Verify sorted set zadd, zcount, zrangebyscore, and zremrangebyscore contract."""
    redis = fake_redis_client
    added = await redis.zadd("test:zset", {"a": 10.0, "b": 20.0, "c": 30.0})
    assert added == 3

    assert await redis.zcount("test:zset", 15.0, 35.0) == 2
    in_range = await redis.zrangebyscore("test:zset", 15.0, 35.0)
    assert in_range == [b"b", b"c"]

    with_scores = await redis.zrangebyscore("test:zset", "-inf", "+inf", withscores=True)
    assert with_scores == [(b"a", 10.0), (b"b", 20.0), (b"c", 30.0)]

    removed = await redis.zremrangebyscore("test:zset", 15.0, 25.0)
    assert removed == 1
    assert await redis.zrangebyscore("test:zset", "-inf", "+inf") == [b"a", b"c"]


@pytest.mark.asyncio
async def test_fake_redis_stream_contract(fake_redis_client: FakeRedis) -> None:
    """Verify stream xadd, xlen, xgroup_create, xreadgroup, and xack contract."""
    redis = fake_redis_client
    eid1 = await redis.xadd("test:stream", {"action": "buy", "qty": "10"})
    eid2 = await redis.xadd("test:stream", {"action": "sell", "qty": "5"})
    assert eid1 is not None and eid2 is not None
    assert await redis.xlen("test:stream") == 2

    assert await redis.xgroup_create("test:stream", "g1", id_val="0") is True

    reads = await redis.xreadgroup("g1", "c1", {"test:stream": ">"}, count=1)
    assert len(reads) == 1
    assert reads[0][0] == b"test:stream"
    assert len(reads[0][1]) == 1
    read_eid = reads[0][1][0][0]
    assert read_eid.decode() == eid1

    acked = await redis.xack("test:stream", "g1", eid1)
    assert acked == 1

    pending = await redis.xreadgroup("g1", "c1", {"test:stream": "0"})
    assert not pending

    info = await redis.xinfo_stream("test:stream")
    assert info["length"] == 2

    trimmed = await redis.xtrim("test:stream", maxlen=1)
    assert trimmed == 1
    assert await redis.xlen("test:stream") == 1


@pytest.mark.asyncio
async def test_fake_redis_pipeline_contract(fake_redis_client: FakeRedis) -> None:
    """Verify pipeline command queueing and execution."""
    redis = fake_redis_client
    async with redis.pipeline() as pipe:
        pipe.set("p:1", "first")
        pipe.set("p:2", "second")
        pipe.get("p:1")
        results = await pipe.execute()

    assert results == [True, True, b"first"]
    assert await redis.get("p:2") == b"second"
