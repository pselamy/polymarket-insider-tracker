"""The strictly increasing, clock-safe cycle cutoff shared by both acquisition paths.

Every acquisition cycle requests its page(s) with an ``end`` that is strictly greater than the
previous cycle's (a distinct provider cache key) yet never ahead of the local clock. The v1 page
client and the v2 cursor walk both need this exact rule, so it lives in one place rather than being
re-declared per path.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]


class CycleCutoff:
    """The next strictly increasing cycle ``end``, waiting if it would run ahead of the clock."""

    def __init__(self, clock: Clock, sleeper: Sleeper) -> None:
        self._clock = clock
        self._sleeper = sleeper
        self._last: int | None = None

    @property
    def last(self) -> int | None:
        return self._last

    def restore(self, end: int | None) -> None:
        """Continue the strictly increasing sequence from a durable checkpoint."""
        self._last = end

    async def next(self) -> int:
        """Return the next ``end``, sleeping only when it would exceed the current clock."""
        now = self._clock()
        end = int(now) if self._last is None else max(self._last + 1, int(now))
        if end > now:
            await self._sleeper(end - now)
        self._last = end
        return end
