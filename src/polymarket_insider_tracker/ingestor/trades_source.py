"""Acquisition of public trade pages from the documented anonymous trades query.

``TradesSourceClient`` sends exactly the documented parameters over an injected
``httpx.AsyncClient``, keeps the requested ``end`` strictly increasing so every cycle has a distinct
cache key, classifies responses as success, transient, or terminal, retries transient failures
with capped full-jitter backoff plus ``Retry-After``, and records every attempt so the request
budget is observable. It never sends a credential and never requests beyond the documented
``limit``/``offset`` maximums. See ``contracts/source-acquisition.md``.

The per-attempt request-build, send, status-classification, attempt-recording, and budget-window
mechanics live in :mod:`polymarket_insider_tracker.ingestor._http_attempt` so this client and the v2
page transport share exactly one implementation; this module keeps only the v1 list-body success
parser and its error taxonomy.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

from polymarket_insider_tracker.ingestor._cycle_cutoff import CycleCutoff
from polymarket_insider_tracker.ingestor._http_attempt import (
    BUDGET_WINDOW_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    TRANSIENT_STATUSES,
    USER_AGENT,
    AttemptOutcome,
    AttemptResponse,
    AttemptTerminalError,
    AttemptTransientError,
    HttpAttempts,
    RequestAttempt,
    TerminalReason,
    TransientKind,
    is_transient_status,
    parse_retry_after,
    redacted_url,
)
from polymarket_insider_tracker.ingestor._retry import BACKOFF_CAP_SECONDS as BACKOFF_CAP_SECONDS
from polymarket_insider_tracker.ingestor._retry import (
    MAX_RETRIES,
    RandomSource,
    RetryPolicy,
)
from polymarket_insider_tracker.ingestor._retry import (
    RETRY_AFTER_CAP_SECONDS as RETRY_AFTER_CAP_SECONDS,
)
from polymarket_insider_tracker.shapes import object_list

__all__ = [
    "BACKOFF_CAP_SECONDS",
    "BUDGET_WINDOW_SECONDS",
    "MAX_RETRIES",
    "PAGE_LIMIT",
    "RECOVERY_OFFSET",
    "REQUEST_TIMEOUT_SECONDS",
    "RETRY_AFTER_CAP_SECONDS",
    "TRANSIENT_STATUSES",
    "USER_AGENT",
    "AttemptOutcome",
    "Clock",
    "RequestAttempt",
    "ResponseHeaders",
    "Sleeper",
    "TerminalReason",
    "TradesPage",
    "TradesRequest",
    "TradesSourceClient",
    "TradesSourceError",
    "TradesTerminalError",
    "TradesTransientError",
    "TransientKind",
    "is_transient_status",
    "parse_retry_after",
    "redacted_url",
    "start_for",
]

PAGE_LIMIT = 10_000
RECOVERY_OFFSET = 10_000

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]


class TradesSourceError(Exception):
    """Base error for the trades source; messages never carry a query string or credential."""

    def __init__(self, message: str, *, status: int | None) -> None:
        super().__init__(message)
        self.status = status


class TradesTransientError(TradesSourceError):
    """A retryable failure: transport error, timeout, throttling, 5xx, or unparseable body."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None,
        kind: TransientKind,
        retry_after_seconds: float = 0.0,
    ) -> None:
        super().__init__(message, status=status)
        self.kind = kind
        self.retry_after_seconds = retry_after_seconds

    @property
    def timeout(self) -> bool:
        return self.kind == "timeout"


class TradesTerminalError(TradesSourceError):
    """A non-retryable failure: a terminal HTTP status or an incompatible response schema."""

    def __init__(self, message: str, *, status: int | None, reason: TerminalReason) -> None:
        super().__init__(message, status=status)
        self.reason = reason


@dataclass(frozen=True)
class TradesRequest:
    """The documented query parameters of one page request."""

    offset: int
    taker_only: bool
    start: int
    end: int

    def params(self) -> dict[str, str]:
        return {
            "limit": str(PAGE_LIMIT),
            "offset": str(self.offset),
            "takerOnly": "true" if self.taker_only else "false",
            "start": str(self.start),
            "end": str(self.end),
        }


@dataclass(frozen=True)
class ResponseHeaders:
    """The cache-related response headers the smoke record reports."""

    cache_control: str | None
    cf_cache_status: str | None
    age: str | None


@dataclass(frozen=True)
class TradesPage:
    """One successful page: raw rows plus provenance for parsing, status, and evidence."""

    request: TradesRequest
    rows: tuple[object, ...]
    received_at: float
    attempts: int
    http_status: int
    body_sha256: str
    headers: ResponseHeaders


def start_for(boundary_time: int | None, horizon_seconds: int) -> int:
    """The documented ``start`` bound: boundary minus horizon, or ``0`` on first start."""
    if boundary_time is None:
        return 0
    return max(0, boundary_time - horizon_seconds)


class TradesSourceClient:
    """Rate-conscious, retrying acquisition of documented trade pages."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        coverage: str,
        clock: Clock = time.time,
        sleeper: Sleeper = asyncio.sleep,
        random_source: RandomSource = random.random,
        max_retries: int = MAX_RETRIES,
        on_attempt: Callable[[RequestAttempt], None] | None = None,
    ) -> None:
        self._url = url
        self._taker_only = coverage == "taker-only"
        self._clock = clock
        self._sleeper = sleeper
        self._retry = RetryPolicy(max_retries=max_retries, random_source=random_source)
        self._cutoff = CycleCutoff(clock, sleeper)
        self._http = HttpAttempts(client, url=url, clock=clock, on_attempt=on_attempt)

    @property
    def last_request_end(self) -> int | None:
        return self._cutoff.last

    def restore_last_end(self, end: int | None) -> None:
        """Continue the strictly increasing ``end`` sequence from a durable checkpoint."""
        self._cutoff.restore(end)

    @property
    def attempts(self) -> tuple[RequestAttempt, ...]:
        return self._http.attempts

    def requests_in_window(self, now: float | None = None) -> int:
        """Attempts made within the last ``BUDGET_WINDOW_SECONDS``."""
        return self._http.requests_in_window(now)

    async def fetch_primary(
        self, *, boundary_time: int | None, horizon_seconds: int, proof: object | None = None
    ) -> TradesPage:
        """Fetch the newest page; ``proof`` keeps one call signature across both paths."""
        _ = proof
        end = await self._cutoff.next()
        request = TradesRequest(
            offset=0,
            taker_only=self._taker_only,
            start=start_for(boundary_time, horizon_seconds),
            end=end,
        )
        return await self._fetch(request)

    async def fetch_recovery(self, primary: TradesRequest) -> TradesPage:
        """Fetch the single documented recovery page for the same window."""
        request = TradesRequest(
            offset=RECOVERY_OFFSET,
            taker_only=primary.taker_only,
            start=primary.start,
            end=primary.end,
        )
        return await self._fetch(request)

    async def _fetch(self, request: TradesRequest) -> TradesPage:
        for attempt in range(self._retry.max_retries):
            outcome = await self._attempt(request, attempt)
            if isinstance(outcome, TradesPage):
                return outcome
            retry_after = outcome.retry_after_seconds
            await self._sleeper(self._retry.delay_seconds(attempt, retry_after_seconds=retry_after))
        final = await self._attempt(request, self._retry.max_retries)
        if isinstance(final, TradesPage):
            return final
        raise self._exhausted(final)

    def _exhausted(self, error: TradesTransientError) -> TradesSourceError:
        if error.kind == "unparseable":
            return TradesTerminalError(
                f"incompatible-schema: unparseable body from {redacted_url(self._url)}",
                status=error.status,
                reason="incompatible-schema",
            )
        return error

    async def _attempt(
        self, request: TradesRequest, attempt: int
    ) -> TradesPage | TradesTransientError:
        try:
            result = await self._http.attempt(self._url, request.params(), index=attempt)
            return self._parse_success(result, request)
        except AttemptTransientError as exc:
            return self._to_transient(exc)
        except AttemptTerminalError as exc:
            raise self._to_terminal(exc) from None

    def _to_transient(self, error: AttemptTransientError) -> TradesTransientError:
        return TradesTransientError(
            str(error),
            status=error.status,
            kind=error.kind,
            retry_after_seconds=error.retry_after_seconds,
        )

    def _to_terminal(self, error: AttemptTerminalError) -> TradesTerminalError:
        return TradesTerminalError(str(error), status=error.status, reason=error.reason)

    def _success_page(
        self, result: AttemptResponse, request: TradesRequest, rows: tuple[object, ...]
    ) -> TradesPage:
        """Assemble the one success page both envelope parsers return."""
        return TradesPage(
            request=request,
            rows=rows,
            received_at=self._clock(),
            attempts=result.index + 1,
            http_status=200,
            body_sha256=hashlib.sha256(result.response.content).hexdigest(),
            headers=ResponseHeaders(
                cache_control=result.response.headers.get("cache-control"),
                cf_cache_status=result.response.headers.get("cf-cache-status"),
                age=result.response.headers.get("age"),
            ),
        )

    def _parse_success(self, result: AttemptResponse, request: TradesRequest) -> TradesPage:
        body = self._http.decode(
            self._url, result.response, started=result.started, index=result.index
        )
        items = object_list(body)
        if items is None:
            self._http.record(result.started, result.index, "terminal", 200)
            raise TradesTerminalError(
                f"incompatible-schema: non-list body from {redacted_url(self._url)}",
                status=200,
                reason="incompatible-schema",
            )
        self._http.record(result.started, result.index, "success", 200)
        return self._success_page(result, request, tuple(items))
