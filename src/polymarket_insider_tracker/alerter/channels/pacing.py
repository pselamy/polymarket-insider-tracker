"""Delivery pacing shared by the channel implementations.

Both channel modules pace the same way before and between delivery attempts,
so that knowledge lives here exactly once: a sliding one-minute send window
that holds each channel's own request history, and the exponential backoff
slept between retry attempts. The channels keep their genuinely different
protocol handling (success criterion, retry-after shape, rejection labels,
credential components) in their own modules and only the channel name used
in the rate-limit log line is supplied per channel.
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class RateLimitWindow:
    """A sliding one-minute send window for one channel instance.

    Every send appends its loop time to the window; requests older than one
    minute are forgotten; when the window already holds ``limit_per_minute``
    requests, the next send sleeps until the oldest one expires rather than
    being rejected outright. The lock serializes the check-and-append so
    concurrent sends cannot both claim the same free slot.
    """

    def __init__(self, *, limit_per_minute: int, label: str) -> None:
        """Create the window for one channel.

        Args:
            limit_per_minute: Maximum sends per sliding one-minute window.
            label: The channel name used in the rate-limit log line.
        """
        self.limit_per_minute = limit_per_minute
        self._label = label
        self._request_times: list[float] = []
        self._lock = asyncio.Lock()

    async def wait_for_slot(self) -> None:
        """Wait if rate limit is exceeded."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            # Remove requests older than 1 minute
            self._request_times = [t for t in self._request_times if now - t < 60]

            if len(self._request_times) >= self.limit_per_minute:
                # Wait until the oldest request expires
                wait_time = 60 - (now - self._request_times[0])
                if wait_time > 0:
                    logger.debug(f"{self._label} rate limit hit, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)

            self._request_times.append(now)


async def sleep_backoff(attempt: int, max_retries: int, retry_delay: float) -> None:
    """Sleep the exponential delay that spaces out the retry after ``attempt``.

    The delay doubles with each zero-based attempt, and the final permitted
    attempt is not followed by a sleep because no retry would follow it.
    """
    if attempt < max_retries - 1:
        delay = retry_delay * (2**attempt)
        await asyncio.sleep(delay)
