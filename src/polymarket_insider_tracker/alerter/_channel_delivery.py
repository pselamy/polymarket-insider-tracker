"""Channel-scoped delivery deduplication for alert history.

This module holds the side-effectful helpers implementing the authoritative
per-channel delivery deduplication lifecycle used by
:class:`~polymarket_insider_tracker.alerter.history.AlertHistory`: the keyed
suppression checks, the atomic in-flight send claim, its compare-and-delete /
compare-and-expire release, and ambiguity-window recording.

Following the publisher-codec extraction pattern, the helpers use public names
here and are re-exported as the historical ``AlertHistory.<name>`` methods by
:mod:`polymarket_insider_tracker.alerter.history`, so import paths,
introspection and monkeypatch seams keep resolving.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

from redis.exceptions import WatchError

if TYPE_CHECKING:
    from .history import AlertHistory


class _QueuedPipeline(Protocol):
    """Surface of the queued terminal Redis operations used by the CAS home."""

    def delete(self, *names: str) -> object:
        """Queue deletion of the named keys."""

    def expire(self, name: str, time: int) -> object:
        """Queue a TTL update for the named key."""


async def is_channel_suppressed(
    self: AlertHistory, channel: str, wallet: str, market: str
) -> tuple[bool, str | None]:
    """Check if delivery to a channel is suppressed.

    Returns:
        (True, "duplicate") if already delivered.
        (True, "ambiguous_timeout") if in 60s ambiguity window.
        (False, None) if eligible for delivery.
    """
    dedup_key = self.get_channel_dedup_key(channel, wallet, market)
    if await self.redis.exists(dedup_key):
        return True, "duplicate"

    ambiguous_key = self.get_channel_ambiguous_key(channel, wallet, market)
    if await self.redis.exists(ambiguous_key):
        return True, "ambiguous_timeout"

    return False, None


async def is_channel_delivered(self: AlertHistory, channel: str, wallet: str, market: str) -> bool:
    """Return True when a confirmed delivery is recorded inside the dedup window."""
    key = self.get_channel_dedup_key(channel, wallet, market)
    return bool(await self.redis.exists(key))


async def record_channel_delivery(
    self: AlertHistory, channel: str, wallet: str, market: str, ttl: int | None = None
) -> None:
    """Record confirmed delivery for a channel with TTL."""
    key = self.get_channel_dedup_key(channel, wallet, market)
    effective_ttl = ttl or self.dedup_ttl
    await self.redis.set(key, datetime.now(UTC).isoformat(), ex=effective_ttl)


async def claim_channel_send(
    self: AlertHistory, channel: str, wallet: str, market: str, ttl: int = 60
) -> str | None:
    """Atomically claim the single in-flight send for a delivery identity.

    The ambiguity key doubles as the claim: ``SET NX EX`` either acquires it
    (returning this claim's unique ownership token) or observes a concurrent
    attempt or active ambiguity window (returning None). The claim is released
    by compare-and-delete with the token on a confirmed outcome and retained on
    an ambiguous one, so the check-then-send sequence cannot deliver the same
    identity twice concurrently and a stale owner can never delete a newer claim.
    """
    key = self.get_channel_ambiguous_key(channel, wallet, market)
    token = uuid.uuid4().hex
    acquired = await self.redis.set(key, token, nx=True, ex=ttl)
    if acquired is None or acquired is False:
        return None
    return token


async def release_channel_claim(
    self: AlertHistory, channel: str, wallet: str, market: str, token: str
) -> bool:
    """Release the in-flight claim only while ``token`` still owns it.

    The compare-and-delete runs as WATCH/MULTI so an expired-and-reacquired claim
    (a different owner's token) is never deleted. Returns True when this owner's
    claim was released; False when the claim already expired, was re-acquired, or
    changed concurrently.
    """
    key = self.get_channel_ambiguous_key(channel, wallet, market)
    try:
        return await compare_and_delete(self, key, token)
    except WatchError:
        return False


async def compare_owned_then_execute(
    self: AlertHistory,
    key: str,
    token: str,
    queue_terminal: Callable[[_QueuedPipeline], object],
) -> bool:
    """Single home of the delivery-claim compare-owner CAS rule.

    Opens a WATCH/MULTI/EXEC transaction against ``key`` and decides against
    the stored ownership token under watch, so an expired-and-reacquired claim
    (another owner's token) can never be mutated. When this owner still owns
    the claim, ``queue_terminal`` queues the terminal operation inside the
    MULTI block exactly once and ``EXEC`` runs it unretried; otherwise the
    watch is released with ``UNWATCH`` and no transaction is issued at all.
    Callers translate a ``WatchError`` raised by ``EXEC`` into ``False`` —
    there is no retry by design; the transaction aborts atomically so the
    queued terminal operation never lands.
    """
    async with self.redis.pipeline(transaction=True) as pipe:
        await pipe.watch(key)
        current = await pipe.get(key)
        owner = current.decode() if isinstance(current, bytes) else current
        if owner != token:
            await pipe.unwatch()
            return False
        pipe.multi()
        queue_terminal(pipe)
        await pipe.execute()
        return True


async def compare_and_delete(self: AlertHistory, key: str, token: str) -> bool:
    """Return True while releasing the claim at ``key`` owned by ``token``.

    Compare-then-DELETE release of the delivery claim via
    :func:`compare_owned_then_execute`; a foreign or absent owner is never
    deleted. ``WatchError`` must be translated by callers into ``False``.
    """
    return await compare_owned_then_execute(self, key, token, lambda pipe: pipe.delete(key))


async def extend_channel_claim(
    self: AlertHistory, channel: str, wallet: str, market: str, token: str, ttl: int = 60
) -> bool:
    """Extend the in-flight claim's lease only while ``token`` still owns it.

    Used while a deadline-abandoned send is still physically running: renewing
    the lease keeps any concurrent dispatch of the same identity suppressed so
    no second send can start while the first is in flight. The compare-and-expire
    runs as WATCH/MULTI, so an expired-and-reacquired claim (another owner's
    token) is never extended. Returns True when this owner's lease was renewed.
    """
    key = self.get_channel_ambiguous_key(channel, wallet, market)
    try:
        return await compare_and_expire(self, key, token, ttl)
    except WatchError:
        return False


async def compare_and_expire(self: AlertHistory, key: str, token: str, ttl: int) -> bool:
    """Return True while renewing the lease at ``key`` owned by ``token``.

    Compare-then-EXPIRE lease renewal via :func:`compare_owned_then_execute`;
    a foreign or absent owner is never extended. ``WatchError`` must be
    translated by callers into ``False``.
    """
    return await compare_owned_then_execute(self, key, token, lambda pipe: pipe.expire(key, ttl))


async def record_channel_ambiguous(
    self: AlertHistory, channel: str, wallet: str, market: str, ttl: int = 60
) -> None:
    """Record ambiguous delivery attempt with 60s window."""
    key = self.get_channel_ambiguous_key(channel, wallet, market)
    await self.redis.set(key, datetime.now(UTC).isoformat(), ex=ttl)
