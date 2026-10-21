"""Await the ``Awaitable[T] | T`` results the redis-py stubs declare for async clients."""

import inspect
from collections.abc import Awaitable
from typing import Protocol, TypeVar

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


async def key_count(client: KeyLister) -> int:
    """Number of keys in the client's database (via ``keys``; the ``dbsize`` stub is untyped)."""
    return len(await settled(client.keys("*")))
