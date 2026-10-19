"""Select and drive the documented trades acquisition path from the configured URL.

When the configured URL is explicitly the documented v2 route (``/v2/trades``, the official v2
endpoint, or a proxy mounted there), acquisition runs as one bounded cursor walk per cycle over the
``{data, pagination}`` envelope. Every other URL — including the default official v1 ``/trades``
endpoint — keeps the existing v1 page client unchanged.

The v2 path reuses the merged pieces rather than re-implementing them: :class:`CursorWalk` for the
bounded walk, the injected :class:`AttemptBudget` for the rolling request ceiling, and the poller's
own durable continuity proof as the walk's proof callback. Every walk outcome maps onto the existing
boundary semantics through the poller:

- proof reached            -> the poller proves the combined page and advances the boundary;
- exhausted / page-limit   -> the poller's existing gap path freezes the boundary (possible data loss);
- empty terminal cycle     -> a quiet success, boundary unchanged;
- envelope/pagination error-> a terminal ``TradesSourceError``, so the poller's ``failed`` health
  path runs and the boundary never advances;
- transport/status error   -> a transient ``TradesSourceError``, so the poller's ``degraded`` health
  path runs.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Final
from urllib.parse import urlsplit

import httpx

from polymarket_insider_tracker.ingestor._attempt_budget import AttemptBudget
from polymarket_insider_tracker.ingestor._cycle_cutoff import CycleCutoff
from polymarket_insider_tracker.ingestor._http_attempt import (
    AttemptTerminalError,
    AttemptTransientError,
    HttpAttempts,
    RequestAttempt,
)
from polymarket_insider_tracker.ingestor._retry import MAX_RETRIES, RandomSource
from polymarket_insider_tracker.ingestor.trade_row_v2 import translate_v2_row
from polymarket_insider_tracker.ingestor.trade_rows import TradeObservation
from polymarket_insider_tracker.ingestor.trades_source import (
    ResponseHeaders,
    TradesPage,
    TradesRequest,
    TradesSourceClient,
    TradesTerminalError,
    TradesTransientError,
    start_for,
)
from polymarket_insider_tracker.ingestor.v2_cursor_walk import MAX_WALK_PAGES, CursorWalk
from polymarket_insider_tracker.ingestor.v2_page import (
    V2Page,
    V2PageRequest,
    V2SourceError,
    V2TerminalError,
    V2TransientError,
    V2TransportError,
)

__all__ = [
    "OFFICIAL_V2_TRADES_URL",
    "V2_TRADES_PATH",
    "V2PageHttpTransport",
    "V2TradesSource",
    "V2UnparseableBodyError",
    "make_trades_source",
    "uses_v2_envelope",
]

OFFICIAL_V2_TRADES_URL: Final[str] = "https://data-api.polymarket.com/v2/trades"
V2_TRADES_PATH: Final[str] = "/v2/trades"

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]
ProofCheck = Callable[[Sequence[TradeObservation]], bool]
ProofFactory = Callable[[int], ProofCheck]
AttemptsCallback = Callable[[RequestAttempt], None]


def uses_v2_envelope(url: str) -> bool:
    """Whether ``url`` is explicitly the documented v2 route (path ``/v2/trades``)."""
    return urlsplit(url).path.rstrip("/") == V2_TRADES_PATH


class V2UnparseableBodyError(V2TransientError):
    """A 200 body that is not JSON.

    It is retried by the bounded walk exactly like v1 retries an unparseable body; when the walk
    exhausts its retries the source maps it to a terminal ``incompatible-schema`` failure, so the
    poller reports the same ``failed`` health as the v1 path.
    """


@dataclass(frozen=True)
class _ReceivedPage:
    """One successful page's raw response bytes and provider-cache headers."""

    content: bytes
    headers: ResponseHeaders


class V2PageHttpTransport:
    """One physical v2 page request over an injected ``httpx.AsyncClient``.

    It makes exactly one attempt (the bounded walk owns retry), returns the decoded JSON body, and
    raises :class:`V2TransientError`/:class:`V2TerminalError` so the walk's shared retry policy
    classifies it. Every attempt is recorded for the poller's metrics and request budget through the
    same :class:`~polymarket_insider_tracker.ingestor._http_attempt.HttpAttempts` executor the v1
    page client uses.

    It also keeps the raw response bytes and provider-cache headers of every successful page, in
    order, so the caller can carry real response evidence onto the combined page (the last page's
    cache headers and a digest over the concatenated raw bodies).
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        clock: Clock = time.time,
        on_attempt: AttemptsCallback | None = None,
    ) -> None:
        self._http = HttpAttempts(client, url=url, clock=clock, on_attempt=on_attempt)
        self._url = url
        self._key: tuple[str | None, int, int] | None = None
        self._index = 0
        self._received: list[_ReceivedPage] = []

    @property
    def attempt_count(self) -> int:
        return self._http.count

    @property
    def page_count(self) -> int:
        return len(self._received)

    def received_since(self, index: int) -> tuple[_ReceivedPage, ...]:
        """The successful pages received since ``page_count`` was ``index``, in order."""
        return tuple(self._received[index:])

    def requests_in_window(self, now: float | None = None) -> int:
        return self._http.requests_in_window(now)

    async def fetch(self, request: V2PageRequest) -> object:
        index = self._next_index(request)
        try:
            result = await self._http.attempt(self._url, request.params(), index=index)
            body = self._http.decode(
                self._url, result.response, started=result.started, index=result.index
            )
            self._http.record(result.started, result.index, "success", 200)
        except AttemptTransientError as exc:
            raise self._transient_error(exc) from None
        except AttemptTerminalError as exc:
            raise V2TerminalError(str(exc)) from None
        self._received.append(
            _ReceivedPage(content=result.response.content, headers=_cache_headers(result.response))
        )
        return body

    def _transient_error(self, exc: AttemptTransientError) -> V2TransientError:
        if exc.kind == "unparseable":
            return V2UnparseableBodyError(str(exc), retry_after_seconds=exc.retry_after_seconds)
        return V2TransientError(str(exc), retry_after_seconds=exc.retry_after_seconds)

    def _next_index(self, request: V2PageRequest) -> int:
        key = (request.cursor, request.start, request.end)
        if key == self._key:
            self._index += 1
        else:
            self._key, self._index = key, 0
        return self._index


def _cache_headers(response: httpx.Response) -> ResponseHeaders:
    """The provider-cache headers the smoke record reports, read off one response."""
    return ResponseHeaders(
        cache_control=response.headers.get("cache-control"),
        cf_cache_status=response.headers.get("cf-cache-status"),
        age=response.headers.get("age"),
    )


class V2TradesSource:
    """Acquire one bounded cursor walk per cycle, shaped like the v1 page client."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        coverage: str,
        clock: Clock = time.time,
        sleeper: Sleeper = asyncio.sleep,
        random_source: RandomSource = random.random,
        on_attempt: AttemptsCallback | None = None,
        budget: AttemptBudget | None = None,
        max_pages: int = MAX_WALK_PAGES,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self._taker_only = coverage == "taker-only"
        self._clock = clock
        self._sleeper = sleeper
        self._random_source = random_source
        self._max_pages = max_pages
        self._max_retries = max_retries
        self._cutoff = CycleCutoff(clock, sleeper)
        self._transport = V2PageHttpTransport(client, url=url, clock=clock, on_attempt=on_attempt)
        self._budget = budget if budget is not None else AttemptBudget(clock=clock, sleep=sleeper)

    @property
    def last_request_end(self) -> int | None:
        return self._cutoff.last

    def restore_last_end(self, end: int | None) -> None:
        self._cutoff.restore(end)

    def requests_in_window(self, now: float | None = None) -> int:
        return self._transport.requests_in_window(now)

    async def fetch_primary(
        self,
        *,
        boundary_time: int | None,
        horizon_seconds: int,
        proof: ProofFactory | None = None,
    ) -> TradesPage:
        """Walk bounded cursor pages, combining them into one page for the poller to process."""
        start = start_for(boundary_time, horizon_seconds)
        end = await self._cutoff.next()
        before = self._transport.attempt_count
        before_pages = self._transport.page_count
        walked = await self._walk(taker_only=self._taker_only, start=start, end=end, proof=proof)
        return self._combined_page(
            walked,
            start=start,
            end=end,
            attempts=before,
            received=self._transport.received_since(before_pages),
        )

    async def fetch_recovery(self, primary: TradesRequest) -> TradesPage:
        """A no-op recovery: the walk was already bounded, so the poller's gap path runs instead."""
        return self._page(
            request=self._request(primary.start, primary.end),
            rows=(),
            attempts=0,
            digest=hashlib.sha256(b"").hexdigest(),
            headers=ResponseHeaders(cache_control=None, cf_cache_status=None, age=None),
        )

    async def _walk(
        self, *, taker_only: bool, start: int, end: int, proof: ProofFactory | None
    ) -> tuple[V2Page, ...]:
        walk = CursorWalk(
            self._transport,
            budget=self._budget,
            clock=self._clock,
            sleeper=self._sleeper,
            random_source=self._random_source,
            max_pages=self._max_pages,
            max_retries=self._max_retries,
        )
        try:
            result = await walk.run(
                taker_only=taker_only,
                start=start,
                end=end,
                proof=None if proof is None else proof(end),
            )
            return result.pages
        except V2SourceError as exc:
            raise TradesTerminalError(
                f"incompatible-schema: {exc}", status=None, reason="incompatible-schema"
            ) from exc
        except V2UnparseableBodyError as exc:
            raise TradesTerminalError(
                f"incompatible-schema: {exc}", status=None, reason="incompatible-schema"
            ) from exc
        except V2TransientError as exc:
            raise TradesTransientError(
                str(exc), status=None, kind="transport", retry_after_seconds=exc.retry_after_seconds
            ) from exc
        except V2TerminalError as exc:
            raise TradesTerminalError(str(exc), status=None, reason="http-status") from exc
        except V2TransportError as exc:
            raise TradesTransientError(str(exc), status=None, kind="transport") from exc

    def _combined_page(
        self,
        pages: tuple[V2Page, ...],
        *,
        start: int,
        end: int,
        attempts: int,
        received: tuple[_ReceivedPage, ...],
    ) -> TradesPage:
        """Combine walked pages, carrying real response evidence onto the single v1-shaped page.

        The digest is taken over the raw response bytes of every page in walk order (not a
        re-serialization of the parsed rows), and the cache headers are those of the last page, so
        the combined page preserves the provider-cache evidence the v1 page records.
        """
        rows = tuple(translate_v2_row(row) for page in pages for row in page.rows)
        received_at = pages[-1].received_at if pages else self._clock()
        return self._page(
            request=self._request(start, end),
            rows=rows,
            attempts=self._transport.attempt_count - attempts,
            digest=hashlib.sha256(b"".join(entry.content for entry in received)).hexdigest(),
            headers=(
                received[-1].headers
                if received
                else ResponseHeaders(cache_control=None, cf_cache_status=None, age=None)
            ),
            received_at=received_at,
        )

    def _page(
        self,
        *,
        request: TradesRequest,
        rows: tuple[object, ...],
        attempts: int,
        digest: str,
        headers: ResponseHeaders,
        received_at: float | None = None,
    ) -> TradesPage:
        return TradesPage(
            request=request,
            rows=rows,
            received_at=self._clock() if received_at is None else received_at,
            attempts=attempts,
            http_status=200,
            body_sha256=digest,
            headers=headers,
        )

    def _request(self, start: int, end: int) -> TradesRequest:
        return TradesRequest(offset=0, taker_only=self._taker_only, start=start, end=end)


def make_trades_source(
    client: httpx.AsyncClient,
    *,
    url: str,
    coverage: str,
    clock: Clock = time.time,
    sleeper: Sleeper = asyncio.sleep,
    random_source: RandomSource = random.random,
    on_attempt: AttemptsCallback | None = None,
) -> TradesSourceClient | V2TradesSource:
    """Select the acquisition client from the configured URL (v2 only for the documented route)."""
    if uses_v2_envelope(url):
        return V2TradesSource(
            client,
            url=url,
            coverage=coverage,
            clock=clock,
            sleeper=sleeper,
            random_source=random_source,
            on_attempt=on_attempt,
        )
    return TradesSourceClient(
        client,
        url=url,
        coverage=coverage,
        clock=clock,
        sleeper=sleeper,
        random_source=random_source,
        on_attempt=on_attempt,
    )
