"""Rolling attempt-start budget for v2 acquisition (unwired).

A pure, injectable-clock limiter that permits at most ``max_attempts`` physical
attempt *starts* in any rolling ``window_seconds`` interval for one acquisition
client. Capacity is consumed by starts, never completions, and the wait before
the next permitted start is an ordinary cancellable await.

This module is standalone: nothing in the HTTP client, retry helper, or poller
imports it, so it changes no existing behavior. ``RollingAttemptWindow`` holds
the side-effect-free bookkeeping over start timestamps; ``AttemptBudget`` adds
the injected clock and sleep.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Final

DEFAULT_MAX_ATTEMPTS: Final[int] = 30
DEFAULT_WINDOW_SECONDS: Final[float] = 10.0

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]

__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_WINDOW_SECONDS",
    "AttemptBudget",
    "RollingAttemptWindow",
]


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _require_positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer, got {value!r}")
    return value


def _require_positive_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive number, got {value!r}")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{name} must be a positive finite number, got {value!r}")
    return number


class RollingAttemptWindow:
    """Pure rolling-window bookkeeping over physical attempt start times.

    A start recorded at ``t`` counts against the budget while ``now - t`` is
    strictly less than ``window_seconds``; at exactly ``window_seconds`` it has
    aged out. Expired starts are discarded lazily on the next read.
    """

    def __init__(self, max_attempts: int, window_seconds: float) -> None:
        self._max_attempts = _require_positive_int(max_attempts, "max_attempts")
        self._window_seconds = _require_positive_float(window_seconds, "window_seconds")
        self._starts: deque[float] = deque()

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    @property
    def window_seconds(self) -> float:
        return self._window_seconds

    def recorded_starts(self) -> tuple[float, ...]:
        """Recorded start times, oldest first, before lazy expiry is applied."""
        return tuple(self._starts)

    def seconds_until_available(self, now: float) -> float:
        """Seconds to wait before a new start fits, or ``0.0`` if it fits now."""
        self._discard_expired(now)
        if len(self._starts) < self._max_attempts:
            return 0.0
        return self._starts[0] + self._window_seconds - now

    def try_start(self, now: float) -> bool:
        """Record a start at ``now`` if the window allows it; report whether it did."""
        if self.seconds_until_available(now) > 0.0:
            return False
        self._starts.append(now)
        return True

    def _discard_expired(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._starts and self._starts[0] <= cutoff:
            self._starts.popleft()


class AttemptBudget:
    """Async limiter allowing ``max_attempts`` starts per rolling window.

    The clock and sleep are injected so the limiter is deterministic and pure of
    real time in tests; both default to the standard library.
    """

    def __init__(
        self,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        *,
        clock: Clock = time.monotonic,
        sleep: Sleep = _default_sleep,
    ) -> None:
        self._window = RollingAttemptWindow(max_attempts, window_seconds)
        self._clock = clock
        self._sleep = sleep

    @property
    def max_attempts(self) -> int:
        return self._window.max_attempts

    @property
    def window_seconds(self) -> float:
        return self._window.window_seconds

    def recorded_starts(self) -> tuple[float, ...]:
        """Recorded start times currently counted against the budget."""
        return self._window.recorded_starts()

    def seconds_until_available(self) -> float:
        """Seconds until the next start is permitted, per the injected clock."""
        return self._window.seconds_until_available(self._clock())

    async def acquire(self) -> None:
        """Wait for and record one physical attempt start.

        The wait is cancellable: if the task is cancelled while sleeping,
        ``asyncio.CancelledError`` propagates and no start is recorded.
        """
        while True:
            now = self._clock()
            if self._window.try_start(now):
                return
            await self._sleep(self._window.seconds_until_available(now))
