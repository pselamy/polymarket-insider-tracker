"""The Redis surface :class:`~polymarket_insider_tracker.alerter.history.AlertHistory` depends on.

redis-py types command replies as ``Any``; the protocols here declare them as ``object`` and the
readers below narrow each reply to the type the history code needs, rejecting any other shape with
``TypeError`` rather than trusting it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol

from polymarket_insider_tracker.shapes import object_sequence


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
