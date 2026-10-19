"""Smoke record model, classification, and per-case record construction."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Awaitable, Callable
from typing import Literal

from polymarket_insider_tracker.ingestor.trade_rows import InvalidRow, parse_trade_row
from polymarket_insider_tracker.ingestor.trades_source import (
    PAGE_LIMIT,
    RETRY_AFTER_CAP_SECONDS,
    TradesPage,
    TradesTerminalError,
    TradesTransientError,
)

MAX_INVALID_SHARE = 0.05
SATURATION_WARNING = (
    "page saturated: a full page is not by itself evidence of loss; "
    "the observation boundary contract owns loss detection"
)

SmokeCase = Literal[
    "valid-wallet-bearing",
    "valid-empty",
    "throttled",
    "timeout",
    "malformed-row",
    "incompatible-schema",
    "possible-page-saturation",
]
Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]
ClientFactory = Callable[[], object]


@dataclasses.dataclass(frozen=True)
class SmokeRecord:
    """One aggregate-only record per request; see the data model's smoke evidence record."""

    endpoint: str
    captured_at: str
    coverage: str
    case: SmokeCase
    http_status: int | None
    row_count: int
    valid_rows: int
    invalid_rows: int
    missing_required_fields: dict[str, int]
    newest_timestamp: int | None
    oldest_timestamp: int | None
    provider_lag_seconds: float | None
    page_saturated: bool
    cache_control: str | None
    cf_cache_status: str | None
    age: str | None
    response_sha256: str | None
    passed: bool
    reachable: bool
    transient: bool
    timeout: bool
    retry_after_honoured: bool
    in_window_rows: int
    window_seconds: int
    warning: str | None
    error: str | None
    retained_wallet_identifiers: bool = False

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), sort_keys=True)


@dataclasses.dataclass
class _Aggregate:
    """Counts derived from a page while the rows themselves are discarded."""

    valid_rows: int = 0
    missing: dict[str, int] = dataclasses.field(default_factory=dict[str, int])
    newest: int | None = None
    oldest: int | None = None
    in_window: int = 0

    def add(self, parsed: object, *, window_start: int) -> None:
        if isinstance(parsed, InvalidRow):
            self.missing[parsed.field] = self.missing.get(parsed.field, 0) + 1
            return
        timestamp = getattr(parsed, "provider_timestamp", 0)
        self.valid_rows += 1
        self.newest = timestamp if self.newest is None else max(self.newest, timestamp)
        self.oldest = timestamp if self.oldest is None else min(self.oldest, timestamp)
        if timestamp >= window_start:
            self.in_window += 1

    @property
    def invalid_rows(self) -> int:
        return sum(self.missing.values())


def _aggregate(page: TradesPage, *, window_start: int) -> _Aggregate:
    aggregate = _Aggregate()
    now = int(page.received_at)
    for raw in page.rows:
        aggregate.add(parse_trade_row(raw, now=now), window_start=window_start)
    return aggregate


def _classify_page(aggregate: _Aggregate, row_count: int, window_start: int) -> SmokeCase:
    if row_count == 0:
        return "valid-empty"
    saturated_beyond_window = aggregate.oldest is not None and aggregate.oldest > window_start
    if row_count >= PAGE_LIMIT and saturated_beyond_window:
        return "possible-page-saturation"
    if aggregate.invalid_rows > 0:
        return "malformed-row"
    return "valid-wallet-bearing"


def _page_verdict(case: SmokeCase, aggregate: _Aggregate, row_count: int) -> bool:
    """Empty pages pass; every non-empty page must meet the same row-quality floor."""
    if case == "valid-empty":
        return True
    share = aggregate.invalid_rows / row_count
    return aggregate.valid_rows > 0 and share <= MAX_INVALID_SHARE


@dataclasses.dataclass(frozen=True)
class Probe:
    """Everything one request needs, so record builders stay small."""

    endpoint: str
    coverage: str
    window_seconds: int
    window_start: int
    captured_at: str


def page_record(probe: Probe, page: TradesPage) -> SmokeRecord:
    aggregate = _aggregate(page, window_start=probe.window_start)
    row_count = len(page.rows)
    case = _classify_page(aggregate, row_count, probe.window_start)
    lag = None if aggregate.newest is None else page.received_at - aggregate.newest
    return SmokeRecord(
        endpoint=probe.endpoint,
        captured_at=probe.captured_at,
        coverage=probe.coverage,
        case=case,
        http_status=page.http_status,
        row_count=row_count,
        valid_rows=aggregate.valid_rows,
        invalid_rows=aggregate.invalid_rows,
        missing_required_fields=dict(aggregate.missing),
        newest_timestamp=aggregate.newest,
        oldest_timestamp=aggregate.oldest,
        provider_lag_seconds=lag,
        page_saturated=row_count >= PAGE_LIMIT,
        cache_control=page.headers.cache_control,
        cf_cache_status=page.headers.cf_cache_status,
        age=page.headers.age,
        response_sha256=page.body_sha256,
        passed=_page_verdict(case, aggregate, row_count),
        reachable=True,
        transient=False,
        timeout=False,
        retry_after_honoured=True,
        in_window_rows=aggregate.in_window,
        window_seconds=probe.window_seconds,
        warning=SATURATION_WARNING if case == "possible-page-saturation" else None,
        error=None,
    )


def failure_record(
    probe: Probe,
    *,
    case: SmokeCase,
    status: int | None,
    passed: bool,
    transient: bool,
    timeout: bool,
    honoured: bool,
    error: str,
) -> SmokeRecord:
    return SmokeRecord(
        endpoint=probe.endpoint,
        captured_at=probe.captured_at,
        coverage=probe.coverage,
        case=case,
        http_status=status,
        row_count=0,
        valid_rows=0,
        invalid_rows=0,
        missing_required_fields={},
        newest_timestamp=None,
        oldest_timestamp=None,
        provider_lag_seconds=None,
        page_saturated=False,
        cache_control=None,
        cf_cache_status=None,
        age=None,
        response_sha256=None,
        passed=passed,
        reachable=False,
        transient=transient,
        timeout=timeout,
        retry_after_honoured=honoured,
        in_window_rows=0,
        window_seconds=probe.window_seconds,
        warning=None,
        error=error,
    )


def transient_record(probe: Probe, exc: TradesTransientError, delays: list[float]) -> SmokeRecord:
    required = min(exc.retry_after_seconds, RETRY_AFTER_CAP_SECONDS)
    honoured = all(delay >= required for delay in delays)
    case: SmokeCase = "timeout" if exc.timeout else "throttled"
    return failure_record(
        probe,
        case=case,
        status=exc.status,
        passed=honoured if case == "throttled" else exc.status is None,
        transient=True,
        timeout=exc.timeout,
        honoured=honoured,
        error=str(exc),
    )


def terminal_record(probe: Probe, exc: TradesTerminalError) -> SmokeRecord:
    return failure_record(
        probe,
        case="incompatible-schema",
        status=exc.status,
        passed=False,
        transient=False,
        timeout=False,
        honoured=True,
        error=str(exc),
    )
