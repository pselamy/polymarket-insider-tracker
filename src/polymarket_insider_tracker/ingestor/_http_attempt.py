"""Per-attempt HTTP mechanics shared by the v1 page client and the v2 page transport.

Both acquisition paths make the same documented request: a JSON ``Accept`` header and the package
``User-Agent``, a fixed request timeout, ``auth=None`` and ``follow_redirects=False`` so an
inherited client credential is never sent, and the documented transient/terminal status
classification with a capped ``Retry-After``. Both record every attempt so the request budget stays
observable. This module owns that knowledge once. Each caller keeps only what genuinely differs:
the v1 list-body success parser versus the v2 raw-envelope decoder, and its own error taxonomy,
mapped from the neutral :class:`AttemptTransientError`/:class:`AttemptTerminalError` raised here.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Literal
from urllib.parse import urlsplit

import httpx

from polymarket_insider_tracker import __version__
from polymarket_insider_tracker.redaction import redact_url as _redact_url

REQUEST_TIMEOUT_SECONDS = 15.0
BUDGET_WINDOW_SECONDS = 10.0
TRANSIENT_STATUSES = frozenset({408, 425, 429})
USER_AGENT = f"polymarket-insider-tracker/{__version__}"

AttemptOutcome = Literal["success", "transient", "terminal"]
TransientKind = Literal["status", "timeout", "transport", "unparseable"]
TerminalReason = Literal["http-status", "incompatible-schema"]

Clock = Callable[[], float]

__all__ = [
    "BUDGET_WINDOW_SECONDS",
    "REQUEST_TIMEOUT_SECONDS",
    "TRANSIENT_STATUSES",
    "USER_AGENT",
    "AttemptOutcome",
    "AttemptResponse",
    "AttemptTerminalError",
    "AttemptTransientError",
    "Clock",
    "HttpAttempts",
    "RequestAttempt",
    "TerminalReason",
    "TransientKind",
    "is_transient_status",
    "parse_retry_after",
    "redacted_url",
]


class AttemptTransientError(Exception):
    """A retryable attempt failure; the caller maps it onto its own transient error type."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None,
        kind: TransientKind,
        retry_after_seconds: float = 0.0,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.kind: TransientKind = kind
        self.retry_after_seconds = retry_after_seconds


class AttemptTerminalError(Exception):
    """A non-retryable attempt failure; the caller maps it onto its own terminal error type."""

    def __init__(self, message: str, *, status: int | None, reason: TerminalReason) -> None:
        super().__init__(message)
        self.status = status
        self.reason: TerminalReason = reason


@dataclass(frozen=True)
class RequestAttempt:
    """One HTTP attempt, retries included, for budget accounting and metrics."""

    at: float
    outcome: AttemptOutcome
    status: int | None
    duration_seconds: float
    attempt: int


@dataclass(frozen=True)
class AttemptResponse:
    """A successful 200 attempt: its start time, attempt index, and raw response."""

    started: float
    index: int
    response: httpx.Response


@dataclass(frozen=True)
class _Attempt:
    """The endpoint and timing of one physical attempt, threaded through classification."""

    url: str
    started: float
    index: int


def redacted_url(url: str) -> str:
    """Return the operator-facing endpoint label for ``url``.

    The label goes through the central redaction policy so a non-root path
    (``POLYMARKET_TRADES_URL`` accepts a proxied path that may carry a
    credential segment) is fail-closed while scheme/host/port stay readable.
    Runtime requests still use the configured URL unchanged.
    """
    return _redact_url(url)


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


def is_transient_status(status: int) -> bool:
    """Whether a status is retryable for the trades endpoint (shared by both paths)."""
    return status in TRANSIENT_STATUSES or status >= 500


class HttpAttempts:
    """The shared request-build, send, classify, record, and budget mechanics.

    The endpoint is supplied per attempt (not cached) so a caller always requests the URL it
    currently holds, and its operator-facing label is redacted where every failure message is built.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        clock: Clock = time.time,
        on_attempt: Callable[[RequestAttempt], None] | None = None,
    ) -> None:
        parsed_url = urlsplit(url)
        if parsed_url.username is not None or parsed_url.password is not None:
            raise ValueError("trades URL must not contain credentials")
        self._client = client
        self._clock = clock
        self._on_attempt = on_attempt
        self._attempts: list[RequestAttempt] = []

    @property
    def attempts(self) -> tuple[RequestAttempt, ...]:
        return tuple(self._attempts)

    @property
    def count(self) -> int:
        return len(self._attempts)

    def requests_in_window(self, now: float | None = None) -> int:
        """Attempts made within the last ``BUDGET_WINDOW_SECONDS``."""
        reference = self._clock() if now is None else now
        floor = reference - BUDGET_WINDOW_SECONDS
        return sum(1 for attempt in self._attempts if attempt.at > floor)

    async def attempt(self, url: str, params: dict[str, str], *, index: int) -> AttemptResponse:
        """One physical GET; a 200 response, or a mapped transient/terminal failure."""
        attempt = _Attempt(url=url, started=self._clock(), index=index)
        outgoing = self._built(attempt, params)
        try:
            response = await self._client.send(outgoing, auth=None, follow_redirects=False)
        except httpx.TimeoutException:
            raise self._transient(attempt, None, "timeout", "timeout") from None
        except httpx.TransportError:
            raise self._transient(attempt, None, "transport", "transport error") from None
        return self._classify(attempt, response)

    def decode(self, url: str, response: httpx.Response, *, started: float, index: int) -> object:
        """The decoded JSON body, or a retryable failure when it is not JSON."""
        attempt = _Attempt(url=url, started=started, index=index)
        try:
            return json.loads(response.content)
        except ValueError:
            raise self._transient(attempt, 200, "unparseable", "unparseable body") from None

    def record(
        self, started: float, index: int, outcome: AttemptOutcome, status: int | None
    ) -> None:
        record = RequestAttempt(
            at=started,
            outcome=outcome,
            status=status,
            duration_seconds=max(0.0, self._clock() - started),
            attempt=index,
        )
        self._attempts.append(record)
        if self._on_attempt is not None:
            self._on_attempt(record)

    def _built(self, attempt: _Attempt, params: dict[str, str]) -> httpx.Request:
        """The documented request, or a redacted transient failure when it cannot build.

        ``httpx.InvalidURL`` subclasses ``Exception`` directly, not ``ValueError``, so it must be
        named here explicitly or an invalid injected URL would bypass this boundary entirely.
        """
        try:
            return httpx.Request(
                "GET",
                attempt.url,
                params=params,
                headers={"Accept": "application/json", "User-Agent": USER_AGENT},
                extensions={"timeout": httpx.Timeout(REQUEST_TIMEOUT_SECONDS).as_dict()},
            )
        except (TypeError, ValueError, httpx.InvalidURL):
            raise self._transient(attempt, None, "transport", "invalid request URL") from None

    def _classify(self, attempt: _Attempt, response: httpx.Response) -> AttemptResponse:
        status = response.status_code
        if status == 200:
            return AttemptResponse(started=attempt.started, index=attempt.index, response=response)
        if is_transient_status(status):
            retry_after = parse_retry_after(response.headers.get("Retry-After"), now=self._clock())
            raise self._transient(attempt, status, "status", f"HTTP {status}", retry_after)
        self.record(attempt.started, attempt.index, "terminal", status)
        raise AttemptTerminalError(
            f"HTTP {status} from {redacted_url(attempt.url)}", status=status, reason="http-status"
        )

    def _transient(
        self,
        attempt: _Attempt,
        status: int | None,
        kind: TransientKind,
        detail: str,
        retry_after: float = 0.0,
    ) -> AttemptTransientError:
        self.record(attempt.started, attempt.index, "transient", status)
        return AttemptTransientError(
            f"transient failure ({detail}) from {redacted_url(attempt.url)}",
            status=status,
            kind=kind,
            retry_after_seconds=retry_after,
        )
