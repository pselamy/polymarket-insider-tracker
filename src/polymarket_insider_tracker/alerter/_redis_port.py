"""The Redis surface :class:`~polymarket_insider_tracker.alerter.history.AlertHistory` depends on.

redis-py types command replies as ``Any``; the protocols here declare them as ``object`` and the
readers below narrow each reply to the type the history code needs, rejecting any other shape with
``TypeError`` rather than trusting it.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Awaitable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol

from polymarket_insider_tracker.shapes import object_mapping, object_sequence


class HistoryPipeline(Protocol):
    """A Redis pipeline: queued writes are synchronous, WATCH/GET/EXEC are awaited."""

    def set(self, name: str, value: str, *, ex: int | None = None) -> object:
        """Queue ``SET``."""
        ...

    def zadd(self, name: str, mapping: Mapping[str, float]) -> object:
        """Queue ``ZADD``."""

    def expire(self, name: str, time: int) -> object:
        """Queue ``EXPIRE``."""

    def delete(self, *names: str) -> object:
        """Queue ``DEL``."""

    def multi(self) -> None:
        """Start buffering commands after a ``WATCH``."""

    def watch(self, *names: str) -> Awaitable[object]:
        """``WATCH`` the keys."""
        ...

    def unwatch(self) -> Awaitable[object]:
        """``UNWATCH`` every watched key."""
        ...

    def get(self, name: str) -> Awaitable[object]:
        """``GET`` a key (immediate while watching)."""
        ...

    def execute(self) -> Awaitable[object]:
        """``EXEC`` the queued commands."""
        ...


class HistoryRedis(Protocol):
    """The async Redis client methods ``AlertHistory`` calls."""

    def pipeline(self, transaction: bool = True) -> AbstractAsyncContextManager[HistoryPipeline]:
        """Open a pipeline."""
        ...

    def exists(self, *names: str) -> Awaitable[object]:
        """``EXISTS``."""
        ...

    def get(self, name: str) -> Awaitable[object]:
        """``GET``."""
        ...

    def set(
        self, name: str, value: str, *, ex: int | None = None, nx: bool = False
    ) -> Awaitable[object]:
        """``SET``."""
        ...

    def ttl(self, name: str) -> Awaitable[object]:
        """``TTL``."""
        ...

    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
    ) -> Awaitable[object]:
        """``ZRANGEBYSCORE``."""
        ...

    def zcount(self, name: str, min: float | str, max: float | str) -> Awaitable[object]:
        """``ZCOUNT``."""
        ...

    def zremrangebyscore(self, name: str, min: float | str, max: float | str) -> Awaitable[object]:
        """``ZREMRANGEBYSCORE``."""
        ...


class BoundaryPipeline(Protocol):
    """The transactional pipeline surface the boundary queues commands on."""

    @abstractmethod
    def watch(self, *names: str) -> Awaitable[object]:
        """Watch ``names`` for modification before the transaction."""

    @abstractmethod
    def type(self, name: str) -> Awaitable[object]:
        """Redis type of ``name``."""

    @abstractmethod
    def multi(self) -> None:
        """Begin queuing commands for the transaction."""

    @abstractmethod
    def hset(self, name: str, *, mapping: dict[str, str | int]) -> object:
        """Queue a hash write."""

    @abstractmethod
    def zadd(self, name: str, mapping: Mapping[str, float]) -> object:
        """Add scored members to a sorted set."""

    @abstractmethod
    def zremrangebyscore(self, name: str, min: str, max: str) -> object:
        """Remove sorted-set members whose score is within ``min``..``max``."""

    @abstractmethod
    def lpush(self, name: str, *values: str) -> object:
        """Push ``values`` onto the head of a list."""

    @abstractmethod
    def ltrim(self, name: str, start: int, end: int) -> object:
        """Trim a list to ``start``..``end``."""

    @abstractmethod
    def execute(self) -> Awaitable[object]:
        """Execute the queued commands atomically."""

    @abstractmethod
    async def __aenter__(self) -> BoundaryPipeline:
        """Enter the pipeline context."""

    @abstractmethod
    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        """Leave the pipeline context."""


class BoundaryRedis(Protocol):
    """The ``redis.asyncio.Redis`` surface the boundary uses; replies are checked, not trusted."""

    @abstractmethod
    def hgetall(self, name: str) -> Awaitable[object] | dict[object, object]:
        """All fields of a hash."""

    @abstractmethod
    def zadd(self, name: str, mapping: Mapping[str, float]) -> Awaitable[object]:
        """Add scored members to a sorted set."""

    @abstractmethod
    def zrangebyscore(
        self,
        name: str,
        min: str,
        max: str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> Awaitable[object]:
        """Sorted-set members within a score range."""

    @abstractmethod
    def lrange(self, name: str, start: int, end: int) -> Awaitable[object] | list[object]:
        """A slice of a list."""

    @abstractmethod
    def pipeline(self, transaction: bool) -> BoundaryPipeline:
        """Open a pipeline."""


class ScanRedis(Protocol):
    """The ``SCAN`` command of an async Redis client."""

    def scan(
        self, cursor: int = 0, match: str | None = None, count: int | None = None
    ) -> Awaitable[object]:
        """``SCAN``."""
        ...


async def scan_page(
    client: ScanRedis, cursor: int, match: str, count: int
) -> tuple[int, list[str | bytes]]:
    """One checked ``SCAN`` page; ``client`` is passed in so redis-py's loose stubs stay unseen."""
    return reply_scan(await client.scan(cursor, match=match, count=count))


async def settle(reply: Awaitable[object] | dict[object, object] | list[object]) -> object:
    """The reply, awaiting it when the client returned an awaitable (redis-py types both)."""
    if isinstance(reply, (dict, list)):
        return reply
    return await reply


def reply_text(reply: object) -> str | bytes:
    """``reply`` when it is ``str`` or ``bytes``; ``TypeError`` otherwise."""
    if isinstance(reply, (str, bytes)):
        return reply
    raise TypeError(f"expected a text Redis reply, got {type(reply).__name__}")


def reply_int(reply: object) -> int:
    """``reply`` when it is an integer count; ``TypeError`` otherwise."""
    if isinstance(reply, int) and not isinstance(reply, bool):
        return reply
    raise TypeError(f"expected an integer Redis reply, got {type(reply).__name__}")


def reply_ids(reply: object) -> list[str | bytes]:
    """The members of a list reply, each ``str`` or ``bytes``; ``TypeError`` otherwise."""
    members = object_sequence(reply)
    if members is None:
        raise TypeError(f"expected a list Redis reply, got {type(reply).__name__}")
    return [reply_text(member) for member in members]


def reply_bytes(reply: object) -> bytes:
    """``reply`` when it is ``bytes``; ``TypeError`` otherwise."""
    if isinstance(reply, bytes):
        return reply
    raise TypeError(f"expected a bytes Redis reply, got {type(reply).__name__}")


def reply_byte_hash(reply: object) -> dict[bytes, bytes]:
    """A hash reply whose fields and values are all ``bytes``; ``TypeError`` otherwise."""
    fields = object_mapping(reply)
    if fields is None:
        raise TypeError(f"expected a hash Redis reply, got {type(reply).__name__}")
    return {reply_bytes(name): reply_bytes(value) for name, value in fields.items()}


def _reply_score(reply: object) -> float:
    if isinstance(reply, (int, float)) and not isinstance(reply, bool):
        return float(reply)
    raise TypeError(f"expected a numeric Redis score, got {type(reply).__name__}")


def reply_scored_members(reply: object) -> list[tuple[bytes, float]]:
    """A ``WITHSCORES`` reply as ``(member, score)`` pairs; ``TypeError`` otherwise."""
    entries = object_sequence(reply)
    if entries is None:
        raise TypeError(f"expected a list Redis reply, got {type(reply).__name__}")
    scored: list[tuple[bytes, float]] = []
    for entry in entries:
        pair = object_sequence(entry)
        if pair is None or len(pair) != 2:
            raise TypeError("expected a (member, score) Redis pair")
        scored.append((reply_bytes(pair[0]), _reply_score(pair[1])))
    return scored


def reply_scan(reply: object) -> tuple[int, list[str | bytes]]:
    """A ``SCAN`` reply as ``(next_cursor, keys)``; ``TypeError`` otherwise."""
    pair = object_sequence(reply)
    if pair is None or len(pair) != 2:
        raise TypeError("expected a (cursor, keys) Redis scan reply")
    return reply_int(pair[0]), reply_ids(pair[1])
