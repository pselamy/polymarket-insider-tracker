"""Shared retry policy for acquisition clients: documented caps plus full jitter.

The v1 ``TradesSourceClient`` and the v2 bounded cursor walk retry transient failures with one
policy: an exponential backoff capped at ``BACKOFF_CAP_SECONDS``, multiplied by an injected random
source (full jitter), plus any ``Retry-After`` delay capped at ``RETRY_AFTER_CAP_SECONDS``. The
constants and the delay formula live here so
neither acquisition path re-declares them. Each client keeps its own transient predicate because the
provider error types differ, but it counts and delays its retries through this one policy.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

MAX_RETRIES: Final[int] = 4
BACKOFF_BASE_SECONDS: Final[float] = 1.0
BACKOFF_CAP_SECONDS: Final[float] = 30.0
RETRY_AFTER_CAP_SECONDS: Final[float] = 60.0

RandomSource = Callable[[], float]

__all__ = [
    "BACKOFF_BASE_SECONDS",
    "BACKOFF_CAP_SECONDS",
    "MAX_RETRIES",
    "RETRY_AFTER_CAP_SECONDS",
    "RandomSource",
    "RetryPolicy",
    "retry_delay_seconds",
]


def retry_delay_seconds(
    attempt: int, *, retry_after_seconds: float, random_source: RandomSource
) -> float:
    """Capped full-jitter backoff for the next retry plus a capped ``Retry-After`` delay.

    ``attempt`` is zero-based, so the first retry doubles the base. The exponential term is
    multiplied by ``random_source()`` (full jitter), so callers sharing an attempt number still draw
    independent delays.
    """
    backoff = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (1 << attempt)) * random_source()
    return backoff + min(retry_after_seconds, RETRY_AFTER_CAP_SECONDS)


@dataclass(frozen=True)
class RetryPolicy:
    """One retry budget and jitter source shared by an acquisition client."""

    max_retries: int = MAX_RETRIES
    random_source: RandomSource = random.random

    def delay_seconds(self, attempt: int, *, retry_after_seconds: float) -> float:
        """The jittered sleep before retrying ``attempt``, honouring ``Retry-After``."""
        return retry_delay_seconds(
            attempt, retry_after_seconds=retry_after_seconds, random_source=self.random_source
        )
