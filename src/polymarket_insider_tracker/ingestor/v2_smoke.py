"""The documented v2 page fetch backing the live-safe smoke check.

The smoke contract is one bounded request per coverage mode, classified into the same seven named
cases the v1 smoke uses. :class:`V2SmokeSource` subclasses the merged page client so the shared
:class:`~polymarket_insider_tracker.ingestor._http_attempt.HttpAttempts` executor, the retry
classification, and the strictly increasing clock-safe ``end`` cutoff stay one implementation; the
smoke owns no HTTP or parsing code of its own. Only the documented differences are overridden —
the v2 first-page parameters, built by the shared :class:`V2PageRequest`, and the
``{data, pagination}`` envelope, read by the shared :func:`parse_v2_page` with rows translated to
the v1 shape by ``trade_row_v2.translate_v2_row``. Envelope and pagination failures are terminal:
they never prove continuity. Selection mirrors the production seam exactly
(:func:`acquisition.uses_v2_envelope`). See ``contracts/live-smoke.md``.
"""

from __future__ import annotations

import asyncio
import random
import time

import httpx

from polymarket_insider_tracker.ingestor._http_attempt import AttemptResponse
from polymarket_insider_tracker.ingestor._retry import MAX_RETRIES, RandomSource
from polymarket_insider_tracker.ingestor.acquisition import uses_v2_envelope
from polymarket_insider_tracker.ingestor.trade_row_v2 import translate_v2_row
from polymarket_insider_tracker.ingestor.trades_source import (
    PAGE_LIMIT,
    Clock,
    Sleeper,
    TradesPage,
    TradesRequest,
    TradesSourceClient,
    TradesTerminalError,
    redacted_url,
    start_for,
)
from polymarket_insider_tracker.ingestor.v2_page import (
    V2_PAGE_LIMIT,
    V2PageRequest,
    V2SourceError,
    parse_v2_page,
)

__all__ = ["V2SmokeSource", "make_smoke_source"]


class _V2WindowRequest(TradesRequest):
    """The cycle window whose ``params`` are the documented v2 first-page parameters."""

    def params(self) -> dict[str, str]:
        return V2PageRequest(taker_only=self.taker_only, start=self.start, end=self.end).params()


class V2SmokeSource(TradesSourceClient):
    """The merged page client speaking the documented v2 envelope (one page per call)."""

    async def fetch_primary(
        self, *, boundary_time: int | None, horizon_seconds: int, proof: object | None = None
    ) -> TradesPage:
        """One documented v2 first page; ``proof`` keeps one call signature across both paths."""
        _ = proof
        end = await self._cutoff.next()
        return await self._fetch(
            _V2WindowRequest(
                offset=0,
                taker_only=self._taker_only,
                start=start_for(boundary_time, horizon_seconds),
                end=end,
            )
        )

    def _parse_success(self, result: AttemptResponse, request: TradesRequest) -> TradesPage:
        body = self._http.decode(
            self._url, result.response, started=result.started, index=result.index
        )
        v2_request = V2PageRequest(
            taker_only=request.taker_only, start=request.start, end=request.end
        )
        try:
            page = parse_v2_page(v2_request, body, received_at=self._clock())
        except V2SourceError as exc:
            self._http.record(result.started, result.index, "terminal", 200)
            raise TradesTerminalError(
                f"incompatible-schema: {exc} from {redacted_url(self._url)}",
                status=200,
                reason="incompatible-schema",
            ) from None
        self._http.record(result.started, result.index, "success", 200)
        rows = tuple(translate_v2_row(row) for row in page.rows)
        return self._success_page(result, request, rows)


def make_smoke_source(
    client: httpx.AsyncClient,
    *,
    url: str,
    coverage: str,
    clock: Clock = time.time,
    sleeper: Sleeper = asyncio.sleep,
    random_source: RandomSource = random.random,
    max_retries: int = MAX_RETRIES,
) -> tuple[TradesSourceClient, int]:
    """The smoke's acquisition client and page limit for ``url``.

    Selection mirrors the production seam exactly (:func:`acquisition.uses_v2_envelope`): the
    documented v2 route speaks the v2 envelope through :class:`V2SmokeSource`; every other URL
    keeps the v1 list-body client unchanged. Returns ``(source, page_limit)`` so the smoke's
    saturation case uses the documented limit of whichever envelope the URL speaks.
    """
    if uses_v2_envelope(url):
        return (
            V2SmokeSource(
                client,
                url=url,
                coverage=coverage,
                clock=clock,
                sleeper=sleeper,
                random_source=random_source,
                max_retries=max_retries,
            ),
            V2_PAGE_LIMIT,
        )
    return (
        TradesSourceClient(
            client,
            url=url,
            coverage=coverage,
            clock=clock,
            sleeper=sleeper,
            random_source=random_source,
            max_retries=max_retries,
        ),
        PAGE_LIMIT,
    )
