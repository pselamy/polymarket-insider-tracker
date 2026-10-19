"""Await the ``Awaitable[T] | T`` results the redis-py stubs declare for async clients."""

from collections.abc import Awaitable
from typing import TypeVar

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
