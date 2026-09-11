"""Acquisition of public trade pages from the documented anonymous trades query.

``TradesSourceClient`` sends exactly the documented parameters over an injected
``httpx.AsyncClient``, keeps the requested ``end`` strictly increasing so every cycle has a distinct
cache key, classifies responses as success, transient, or terminal, retries transient failures
with capped full-jitter backoff plus ``Retry-After``, and records every attempt so the request
budget is observable. It never sends a credential and never requests beyond the documented
``limit``/``offset`` maximums. See ``contracts/source-acquisition.md``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Literal, cast
from urllib.parse import urlsplit

import httpx

from polymarket_insider_tracker import __version__
from polymarket_insider_tracker.redaction import redact_url as _redact_url

PAGE_LIMIT = 10_000
RECOVERY_OFFSET = 10_000
REQUEST_TIMEOUT_SECONDS = 15.0
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 1.0
BACKOFF_CAP_SECONDS = 30.0
RETRY_AFTER_CAP_SECONDS = 60.0
BUDGET_WINDOW_SECONDS = 10.0
TRANSIENT_STATUSES = frozenset({408, 425, 429})
USER_AGENT = f"polymarket-insider-tracker/{__version__}"

AttemptOutcome = Literal["success", "transient", "terminal"]
TransientKind = Literal["status", "timeout", "transport", "unparseable"]
TerminalReason = Literal["http-status", "incompatible-schema"]

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]
RandomSource = Callable[[], float]


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


@dataclass(frozen=True)
class RequestAttempt:
    """One HTTP attempt, retries included, for budget accounting and metrics."""

    at: float
    outcome: AttemptOutcome
    status: int | None
    duration_seconds: float
    attempt: int


def redacted_url(url: str) -> str:
    """Return the operator-facing endpoint label for ``url``.

    The label goes through the central redaction policy so a non-root path
    (``POLYMARKET_TRADES_URL`` accepts a proxied path that may carry a
    credential segment) is fail-closed while scheme/host/port stay readable.
    Runtime requests still use the configured URL unchanged.
    """
    return _redact_url(url)


def start_for(boundary_time: int | None, horizon_seconds: int) -> int:
    """The documented ``start`` bound: boundary minus horizon, or ``0`` on first start."""
    if boundary_time is None:
        return 0
    return max(0, boundary_time - horizon_seconds)


def _retry_after_seconds(value: str) -> float | None:
    if value.lstrip("-").isdigit():
        return float(value)
    try:
        return parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError):
        return None


def parse_retry_after(value: str | None, *, now: float) -> float:
    """Parse ``Retry-After`` seconds or an HTTP date into a non-negative delay in seconds."""
    if value is None:
        return 0.0
    parsed = _retry_after_seconds(value.strip())
    if parsed is None:
        return 0.0
    delay = parsed if value.strip().lstrip("-").isdigit() else parsed - now
    return max(0.0, delay)


def _is_transient_status(status: int) -> bool:
    return status in TRANSIENT_STATUSES or status >= 500


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
        parsed_url = urlsplit(url)
        if parsed_url.username is not None or parsed_url.password is not None:
            raise ValueError("trades URL must not contain credentials")
        self._client = client
        self._url = url
        self._taker_only = coverage == "taker-only"
        self._clock = clock
        self._sleeper = sleeper
        self._random = random_source
        self._max_retries = max_retries
        self._on_attempt = on_attempt
        self._last_end: int | None = None
        self._attempts: list[RequestAttempt] = []

    @property
    def last_request_end(self) -> int | None:
        return self._last_end

    def restore_last_end(self, end: int | None) -> None:
        """Continue the strictly increasing ``end`` sequence from a durable checkpoint."""
        self._last_end = end

    @property
    def attempts(self) -> tuple[RequestAttempt, ...]:
        return tuple(self._attempts)

    def requests_in_window(self, now: float | None = None) -> int:
        """Attempts made within the last ``BUDGET_WINDOW_SECONDS``."""
        reference = self._clock() if now is None else now
        floor = reference - BUDGET_WINDOW_SECONDS
        return sum(1 for attempt in self._attempts if attempt.at > floor)

    async def fetch_primary(self, *, boundary_time: int | None, horizon_seconds: int) -> TradesPage:
        """Fetch the newest page with a strictly increasing ``end`` as the cycle cutoff."""
        end = await self._next_end()
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

    async def _next_end(self) -> int:
        now = self._clock()
        end = int(now) if self._last_end is None else max(self._last_end + 1, int(now))
        if end > now:
            await self._sleeper(end - now)
        self._last_end = end
        return end

    async def _fetch(self, request: TradesRequest) -> TradesPage:
        for attempt in range(self._max_retries + 1):
            outcome = await self._attempt(request, attempt)
            if isinstance(outcome, TradesPage):
                return outcome
            if attempt == self._max_retries:
                raise self._exhausted(outcome)
            await self._sleeper(self._delay(attempt, outcome))
        raise AssertionError("retry loop always returns or raises")

    def _delay(self, attempt: int, error: TradesTransientError) -> float:
        backoff = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (1 << attempt)) * self._random()
        return backoff + min(error.retry_after_seconds, RETRY_AFTER_CAP_SECONDS)

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
        started = self._clock()
        outgoing = self._built_request(request, started, attempt)
        if isinstance(outgoing, TradesTransientError):
            return outgoing
        try:
            response = await self._client.send(outgoing, auth=None, follow_redirects=False)
        except httpx.TimeoutException:
            return self._transient(started, attempt, None, "timeout", "timeout")
        except httpx.TransportError:
            return self._transient(started, attempt, None, "transport", "transport error")
        return self._classify(response, request, started, attempt)

    def _built_request(
        self, request: TradesRequest, started: float, attempt: int
    ) -> httpx.Request | TradesTransientError:
        """The documented request, or a redacted transient error when it cannot build.

        ``httpx.InvalidURL`` subclasses ``Exception`` directly, not
        ``ValueError``, so it must be named here explicitly or an invalid
        injected URL would bypass this boundary entirely and rely on the
        poller's error-text mask alone.
        """
        try:
            return self._build_request(request)
        except (TypeError, ValueError, httpx.InvalidURL) as exc:
            return self._redacted_request_error(started, attempt, exc)

    def _build_request(self, request: TradesRequest) -> httpx.Request:
        """Assemble the documented GET request for ``request`` unchanged."""
        return httpx.Request(
            "GET",
            self._url,
            params=request.params(),
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            extensions={"timeout": httpx.Timeout(REQUEST_TIMEOUT_SECONDS).as_dict()},
        )

    def _redacted_request_error(
        self, started: float, attempt: int, exc: Exception
    ) -> TradesTransientError:
        """A request-construction failure as transient without echoing the URL."""
        _ = exc
        self._record(started, attempt, "transient", None)
        return TradesTransientError(
            f"transient failure (invalid request URL) from {redacted_url(self._url)}",
            status=None,
            kind="transport",
            retry_after_seconds=0.0,
        )

    def _classify(
        self, response: httpx.Response, request: TradesRequest, started: float, attempt: int
    ) -> TradesPage | TradesTransientError:
        status = response.status_code
        if status == 200:
            return self._parse_success(response, request, started, attempt)
        if _is_transient_status(status):
            retry_after = parse_retry_after(response.headers.get("Retry-After"), now=self._clock())
            return self._transient(
                started, attempt, status, "status", f"HTTP {status}", retry_after
            )
        self._record(started, attempt, "terminal", status)
        raise TradesTerminalError(
            f"HTTP {status} from {redacted_url(self._url)}", status=status, reason="http-status"
        )

    def _parse_success(
        self, response: httpx.Response, request: TradesRequest, started: float, attempt: int
    ) -> TradesPage | TradesTransientError:
        try:
            body: object = json.loads(response.content)
        except ValueError:
            return self._transient(started, attempt, 200, "unparseable", "unparseable body")
        if not isinstance(body, list):
            self._record(started, attempt, "terminal", 200)
            raise TradesTerminalError(
                f"incompatible-schema: non-list body from {redacted_url(self._url)}",
                status=200,
                reason="incompatible-schema",
            )
        self._record(started, attempt, "success", 200)
        return TradesPage(
            request=request,
            rows=tuple(cast(list[object], body)),
            received_at=self._clock(),
            attempts=attempt + 1,
            http_status=200,
            body_sha256=hashlib.sha256(response.content).hexdigest(),
            headers=ResponseHeaders(
                cache_control=response.headers.get("cache-control"),
                cf_cache_status=response.headers.get("cf-cache-status"),
                age=response.headers.get("age"),
            ),
        )

    def _transient(
        self,
        started: float,
        attempt: int,
        status: int | None,
        kind: TransientKind,
        detail: str,
        retry_after: float = 0.0,
    ) -> TradesTransientError:
        self._record(started, attempt, "transient", status)
        return TradesTransientError(
            f"transient failure ({detail}) from {redacted_url(self._url)}",
            status=status,
            kind=kind,
            retry_after_seconds=retry_after,
        )

    def _record(
        self, started: float, attempt: int, outcome: AttemptOutcome, status: int | None
    ) -> None:
        record = RequestAttempt(
            at=started,
            outcome=outcome,
            status=status,
            duration_seconds=max(0.0, self._clock() - started),
            attempt=attempt,
        )
        self._attempts.append(record)
        if self._on_attempt is not None:
            self._on_attempt(record)
