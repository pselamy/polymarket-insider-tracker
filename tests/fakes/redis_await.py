"""Await the ``Awaitable[T] | T`` results the redis-py stubs declare for async clients."""

from collections.abc import Awaitable
from typing import TypeVar

from redis.asyncio import Redis

T = TypeVar("T")


async def settled(value: Awaitable[T] | T) -> T:
    """Return the awaited result; redis-py types sync/async calls as one union."""
    if isinstance(value, Awaitable):
        return await value
    return value


async def as_bytes(value: Awaitable[str | None] | str | None) -> bytes | None:
    """Await a string-typed lookup that these byte-mode test clients return as ``bytes``."""
    result = await settled(value)
    assert result is None or isinstance(result, bytes)
    return result


async def key_count(client: Redis) -> int:
    """Number of keys in the client's database (via ``keys``; the ``dbsize`` stub is untyped)."""
    return len(await settled(client.keys("*")))
