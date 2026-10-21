"""The evaluation clock: one aware-UTC instant per evaluated trade.

Contract: ``specs/004-truthful-detection-contract/contracts/detector-api.md`` section 2.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

Clock = Callable[[], datetime]


def system_clock() -> datetime:
    """Return the current aware UTC instant (the default evaluation clock)."""
    return datetime.now(UTC)


def utc_instant(value: datetime) -> datetime:
    """Return ``value`` normalised to UTC; a naive datetime is a caller error."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("evaluated_at must be timezone-aware")
    return value.astimezone(UTC)


def read_evaluation_instant(clock: Clock) -> datetime:
    """Read ``clock`` once and return the instant normalised to UTC."""
    return utc_instant(clock())


def parse_stored_instant(text: str) -> datetime:
    """Parse a stored ISO-8601 instant that carries an explicit ``+00:00`` offset."""
    instant = datetime.fromisoformat(text)
    if instant.utcoffset() != timedelta(0):
        raise ValueError("stored instants carry an explicit +00:00 offset")
    return instant
