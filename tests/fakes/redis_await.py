"""Await the ``Awaitable[T] | T`` results the redis-py stubs declare for async clients."""

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Protocol, TypeVar

from redis.asyncio import Redis

T = TypeVar("T")


async def settled(value: Awaitable[T] | T) -> T:
    """Return the awaited result; redis-py types sync/async calls as one union."""
    if inspect.isawaitable(value):
        awaited: T = await value
        return awaited
    return value


async def as_bytes(value: Awaitable[str | None] | str | None) -> bytes | None:
    """Await a string-typed lookup that these byte-mode test clients return as ``bytes``."""
    result = await settled(value)
    assert result is None or isinstance(result, bytes)
    return result


class KeyLister(Protocol):
    """The slice of ``redis.asyncio.Redis`` that ``key_count`` needs, with a typed result."""

    def keys(self, pattern: str = "*") -> Awaitable[list[bytes]] | list[bytes]: ...


async def keys_matching(client: KeyLister, pattern: str) -> list[bytes]:
    """Keys matching a glob pattern (via ``keys``; the stub's result type is partly unknown)."""
    return await settled(client.keys(pattern))


async def key_count(client: KeyLister) -> int:
    """Number of keys in the client's database (via ``keys``; the ``dbsize`` stub is untyped)."""
    return len(await settled(client.keys("*")))


class ScoredSetReader(Protocol):
    """The slice of ``redis.asyncio.Redis`` that ``scored_members`` needs, with a typed result."""

    @property
    def zrangebyscore(
        self,
    ) -> Callable[
        [str, float | str, float | str, int | None, int | None, bool],
        Awaitable[list[tuple[bytes, float]]] | list[tuple[bytes, float]],
    ]: ...


async def scored_members(
    client: ScoredSetReader, key: str, low: float | str = "-inf", high: float | str = "+inf"
) -> list[tuple[bytes, float]]:
    """Every ``(member, score)`` of a sorted set in a score range (the stub is partly untyped)."""
    return await settled(client.zrangebyscore(key, low, high, None, None, True))


class MemberRangeReader(Protocol):
    """The slice of ``redis.asyncio.Redis`` that ``ranged_members`` needs, with a typed result."""

    @property
    def zrangebyscore(
        self,
    ) -> Callable[
        [str, float | str, float | str, int | None, int | None],
        Awaitable[list[bytes]] | list[bytes],
    ]: ...


async def ranged_members(
    client: MemberRangeReader,
    key: str,
    low: float | str = "-inf",
    high: float | str = "+inf",
    start: int | None = None,
    num: int | None = None,
) -> list[bytes]:
    """Members of a sorted set in a score range, in score order (``start``/``num`` page them)."""
    return await settled(client.zrangebyscore(key, low, high, start, num))


class HashWriter(Protocol):
    """The slice of ``redis.asyncio.Redis`` that ``write_hash`` needs, with a typed result."""

    def hset(self, name: str, *, mapping: dict[str, str | int]) -> Awaitable[int] | int: ...


async def write_hash(client: HashWriter, key: str, mapping: Mapping[str, str | int]) -> int:
    """Set hash fields and return how many were added (the ``hset`` stub is partly untyped)."""
    return await settled(client.hset(key, mapping=dict(mapping)))


class PipelineHashWriter(Protocol):
    """The slice of a ``redis.asyncio`` pipeline that ``queue_hash`` needs."""

    def hset(self, name: str, *, mapping: dict[str, str | int]) -> object: ...


def queue_hash(pipe: PipelineHashWriter, key: str, mapping: Mapping[str, str | int]) -> None:
    """Queue an ``HSET`` on a pipeline (the ``hset`` stub is partly untyped)."""
    pipe.hset(key, mapping=dict(mapping))


class HashReader(Protocol):
    """The slice of ``redis.asyncio.Redis`` that ``read_hash`` needs, with a typed result."""

    def hgetall(self, name: str) -> Awaitable[dict[bytes, bytes]] | dict[bytes, bytes]: ...


async def read_hash(client: HashReader, key: str) -> dict[bytes, bytes]:
    """Every field of a hash (the ``hgetall`` stub is partly untyped)."""
    return await settled(client.hgetall(key))


class ListReader(Protocol):
    """The slice of ``redis.asyncio.Redis`` that ``read_list`` needs, with a typed result."""

    def lrange(self, name: str, start: int, end: int) -> Awaitable[list[bytes]] | list[bytes]: ...


async def read_list(client: ListReader, key: str, start: int = 0, end: int = -1) -> list[bytes]:
    """The inclusive ``start``..``end`` slice of a list, head first (the ``lrange`` stub is untyped)."""
    return await settled(client.lrange(key, start, end))


class KeyScanner(Protocol):
    """The slice of ``redis.asyncio.Redis`` that ``scanned_keys`` needs, with a typed result."""

    def scan_iter(
        self, match: str | None = None, count: int | None = None
    ) -> AsyncIterator[bytes]: ...


async def scanned_keys(client: KeyScanner, pattern: str, count: int | None = None) -> list[bytes]:
    """Every key matching ``pattern`` (the ``scan_iter`` stub yields an unknown element type)."""
    return [key async for key in client.scan_iter(match=pattern, count=count)]


class Pinger(Protocol):
    """The slice of ``redis.asyncio.Redis`` that ``ping_ok`` needs, with a typed result."""

    def ping(self) -> Awaitable[bool] | bool: ...


async def ping_ok(client: Pinger) -> bool:
    """Whether the server answered ``PING`` (the ``ping`` stub carries untyped keyword options)."""
    return await settled(client.ping())


class RedisFactory(Protocol):
    """Redis construction from a URL, typed without the client's untyped keyword arguments."""

    @classmethod
    def from_url(cls, url: str) -> Redis: ...


def redis_factory() -> type[RedisFactory]:
    """The real ``Redis`` class viewed through its typed ``from_url``."""
    return Redis
