"""The documented v2 page fetch backing the live-safe smoke check.

The smoke contract is one bounded request per coverage mode, classified into the same seven named
cases the v1 smoke uses. This module subclasses the v1 page client so the retry classification,
the strictly increasing clock-safe ``end`` cutoff, and the request bookkeeping stay one
implementation; only the documented differences are overridden — the v2 query parameters
(``limit``/``taker_only``/``start``/``end``, sent by the shared :class:`V2PageRequest`) and the
``{data, pagination}`` envelope, read by the shared :func:`parse_v2_page`. Envelope and pagination
failures are terminal: they never prove continuity. Rows stay raw; the caller decodes them through
``trade_row_v2.parse_v2_trade_row`` so no parsing is duplicated here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import time
from collections.abc import Callable

import httpx

from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor._retry import MAX_RETRIES, RandomSource
from polymarket_insider_tracker.ingestor.trade_row_v2 import parse_v2_trade_row
from polymarket_insider_tracker.ingestor.trade_rows import parse_trade_row
from polymarket_insider_tracker.ingestor.trades_source import (
    PAGE_LIMIT,
    REQUEST_TIMEOUT_SECONDS,
    USER_AGENT,
    Clock,
    ResponseHeaders,
    Sleeper,
    TradesPage,
    TradesRequest,
    TradesSourceClient,
    TradesTerminalError,
    TradesTransientError,
    redacted_url,
)
from polymarket_insider_tracker.ingestor.v2_page import (
    V2_PAGE_LIMIT,
    V2PageRequest,
    V2SourceError,
    parse_v2_page,
)

__all__ = ["V2SmokeSource", "make_smoke_source"]

RowParser = Callable[..., object]


class V2SmokeSource(TradesSourceClient):
    """The v1 page client speaking the documented v2 envelope (one page per call)."""

    def _build_request(self, request: TradesRequest) -> httpx.Request:
        v2_request = V2PageRequest(
            taker_only=request.taker_only, start=request.start, end=request.end
        )
        return httpx.Request(
            "GET",
            self._url,
            params=v2_request.params(),
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            extensions={"timeout": httpx.Timeout(REQUEST_TIMEOUT_SECONDS).as_dict()},
        )

    def _parse_success(
        self, response: httpx.Response, request: TradesRequest, started: float, attempt: int
    ) -> TradesPage | TradesTransientError:
        try:
            body: object = json.loads(response.content)
        except ValueError:
            return self._transient(started, attempt, 200, "unparseable", "unparseable body")
        v2_request = V2PageRequest(
            taker_only=request.taker_only, start=request.start, end=request.end
        )
        try:
            page = parse_v2_page(v2_request, body, received_at=self._clock())
        except V2SourceError as exc:
            self._record(started, attempt, "terminal", 200)
            raise TradesTerminalError(
                f"incompatible-schema: {exc} from {redacted_url(self._url)}",
                status=200,
                reason="incompatible-schema",
            ) from None
        self._record(started, attempt, "success", 200)
        return TradesPage(
            request=request,
            rows=page.rows,
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


def make_smoke_source(
    client: httpx.AsyncClient,
    *,
    url: str,
    coverage: str,
    clock: Clock = time.time,
    sleeper: Sleeper = asyncio.sleep,
    random_source: RandomSource = random.random,
    max_retries: int = MAX_RETRIES,
) -> tuple[TradesSourceClient, RowParser, int]:
    """The smoke's acquisition client, row decoder, and page limit for ``url``.

    The official v2 endpoint speaks the documented v2 envelope; every other URL keeps the v1
    list-body client unchanged. Returns ``(source, row_parser, page_limit)`` so the smoke's
    classification machinery works for either path without branching again.
    """
    if url == OFFICIAL_V2_TRADES_URL:
        source: TradesSourceClient = V2SmokeSource(
            client,
            url=url,
            coverage=coverage,
            clock=clock,
            sleeper=sleeper,
            random_source=random_source,
            max_retries=max_retries,
        )
        return source, parse_v2_trade_row, V2_PAGE_LIMIT
    source = TradesSourceClient(
        client,
        url=url,
        coverage=coverage,
        clock=clock,
        sleeper=sleeper,
        random_source=random_source,
        max_retries=max_retries,
    )
    return source, parse_trade_row, PAGE_LIMIT
