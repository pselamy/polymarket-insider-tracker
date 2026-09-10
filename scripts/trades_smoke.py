#!/usr/bin/env python3
"""Live-safe smoke check for the documented public trades query (FR-013, SC-005).

The deterministic core, ``run_smoke``, takes an injected ``httpx.AsyncClient`` and classifies each
response into exactly one of seven named cases. ``--live`` performs one bounded anonymous request per
selected coverage mode against the real endpoint. Records are aggregate-only: no wallet, name,
pseudonym, profile value, or raw row is ever retained; the body is hashed and discarded. The command
never opens Redis or PostgreSQL, never constructs an alert channel, never sends a credential, and is
not part of any verifier profile or CI job. See ``contracts/live-smoke.md``.

Exit status: ``0`` when every record passed and was reachable (an HTTP 200 list); ``1`` when a record
failed (incompatible schema, a terminal status, a share of invalid rows above 5%) or every attempt was
transient (throttled or timed out); ``2`` for an invocation error.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import os
import random
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Literal, NoReturn

import httpx

from polymarket_insider_tracker.ingestor.trade_rows import InvalidRow, parse_trade_row
from polymarket_insider_tracker.ingestor.trades_source import (
    PAGE_LIMIT,
    RETRY_AFTER_CAP_SECONDS,
    TradesPage,
    TradesSourceClient,
    TradesTerminalError,
    TradesTransientError,
    redacted_url,
)

DEFAULT_TRADES_URL = "https://data-api.polymarket.com/trades"
COVERAGE_MODES = ("all", "taker-only")
SMOKE_MAX_RETRIES = 2
MAX_INVALID_SHARE = 0.05
SATURATION_WARNING = (
    "page saturated: a full page is not by itself evidence of loss; "
    "the observation boundary contract owns loss detection"
)
EXIT_PASSED = 0
EXIT_FAILED = 1
EXIT_USAGE = 2

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
ClientFactory = Callable[[], httpx.AsyncClient]


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
    """Empty and saturated pages pass by definition; otherwise valid rows must dominate."""
    if case in ("valid-empty", "possible-page-saturation"):
        return True
    share = aggregate.invalid_rows / row_count
    return aggregate.valid_rows > 0 and share <= MAX_INVALID_SHARE


@dataclasses.dataclass(frozen=True)
class _Probe:
    """Everything one request needs, so record builders stay small."""

    endpoint: str
    coverage: str
    window_seconds: int
    window_start: int
    captured_at: str


def _page_record(probe: _Probe, page: TradesPage) -> SmokeRecord:
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


def _failure_record(
    probe: _Probe,
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


def _transient_record(
    probe: _Probe, exc: TradesTransientError, delays: Sequence[float]
) -> SmokeRecord:
    required = min(exc.retry_after_seconds, RETRY_AFTER_CAP_SECONDS)
    honoured = all(delay >= required for delay in delays)
    case: SmokeCase = "timeout" if exc.timeout else "throttled"
    return _failure_record(
        probe,
        case=case,
        status=exc.status,
        passed=honoured if case == "throttled" else exc.status is None,
        transient=True,
        timeout=exc.timeout,
        honoured=honoured,
        error=str(exc),
    )


def _terminal_record(probe: _Probe, exc: TradesTerminalError) -> SmokeRecord:
    return _failure_record(
        probe,
        case="incompatible-schema",
        status=exc.status,
        passed=False,
        transient=False,
        timeout=False,
        honoured=True,
        error=str(exc),
    )


async def _probe(
    client: httpx.AsyncClient,
    probe: _Probe,
    *,
    url: str,
    clock: Clock,
    sleeper: Sleeper,
    random_source: Callable[[], float],
) -> SmokeRecord:
    delays: list[float] = []

    async def recording_sleeper(seconds: float) -> None:
        delays.append(seconds)
        await sleeper(seconds)

    source = TradesSourceClient(
        client,
        url=url,
        coverage=probe.coverage,
        clock=clock,
        sleeper=recording_sleeper,
        random_source=random_source,
        max_retries=SMOKE_MAX_RETRIES,
    )
    try:
        page = await source.fetch_primary(
            boundary_time=int(clock()), horizon_seconds=probe.window_seconds
        )
    except TradesTransientError as exc:
        return _transient_record(probe, exc, delays)
    except TradesTerminalError as exc:
        return _terminal_record(probe, exc)
    return _page_record(probe, page)


async def run_smoke(
    client: httpx.AsyncClient,
    *,
    url: str,
    coverage_modes: Sequence[str],
    window_seconds: int,
    clock: Clock = time.time,
    sleeper: Sleeper = asyncio.sleep,
    random_source: Callable[[], float] = random.random,
) -> list[SmokeRecord]:
    """One bounded request per coverage mode, classified into exactly one case each."""
    records: list[SmokeRecord] = []
    async with client:
        for coverage in coverage_modes:
            now = clock()
            probe = _Probe(
                endpoint=redacted_url(url),
                coverage=coverage,
                window_seconds=window_seconds,
                window_start=int(now) - window_seconds,
                captured_at=datetime.fromtimestamp(now, tz=UTC).isoformat(),
            )
            records.append(
                await _probe(
                    client,
                    probe,
                    url=url,
                    clock=clock,
                    sleeper=sleeper,
                    random_source=random_source,
                )
            )
    return records


def exit_code_for(records: Sequence[SmokeRecord]) -> int:
    """``0`` only when every record passed its verdict and the endpoint was reachable."""
    if all(record.passed and record.reachable for record in records):
        return EXIT_PASSED
    return EXIT_FAILED


def resolve_trades_url() -> str:
    """The configured trades URL, or the documented default; never the legacy setting."""
    return os.environ.get("POLYMARKET_TRADES_URL", DEFAULT_TRADES_URL)


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise argparse.ArgumentError(None, message)


def _window(value: str) -> int:
    seconds = int(value)
    if not 1 <= seconds <= 60:
        raise argparse.ArgumentTypeError("--window-seconds must be between 1 and 60")
    return seconds


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="trades_smoke.py", description=__doc__, allow_abbrev=False, exit_on_error=False
    )
    parser.add_argument("--live", action="store_true", help="perform the bounded live request")
    parser.add_argument("--window-seconds", type=_window, default=5)
    parser.add_argument("--coverage", choices=(*COVERAGE_MODES, "both"), default="both")
    parser.add_argument("--json", action="store_true", help="one JSON object per record")
    return parser


def _render_line(record: SmokeRecord) -> str:
    verdict = "passed" if record.passed else "failed"
    reach = "reachable" if record.reachable else "unreachable"
    return (
        f"{record.coverage}: {record.case} {verdict} ({reach}); rows={record.row_count} "
        f"valid={record.valid_rows} invalid={record.invalid_rows} "
        f"lag={record.provider_lag_seconds} saturated={record.page_saturated}"
    )


def _render(records: Sequence[SmokeRecord], as_json: bool) -> None:
    for record in records:
        print(record.to_json() if as_json else _render_line(record))


def _parse(argv: Sequence[str] | None) -> argparse.Namespace | None:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except (argparse.ArgumentError, argparse.ArgumentTypeError) as exc:
        print(f"{parser.prog}: error: {exc}", file=sys.stderr)
        return None
    if not args.live:
        print(
            f"{parser.prog}: live runs are explicit; pass --live to perform one bounded "
            "anonymous request per coverage mode",
            file=sys.stderr,
        )
        return None
    return args


def main(argv: Sequence[str] | None = None, *, client_factory: ClientFactory | None = None) -> int:
    """Parse options, run the bounded live smoke, print records, and return the exit status."""
    args = _parse(argv)
    if args is None:
        return EXIT_USAGE
    url = resolve_trades_url()
    if not url.startswith(("http://", "https://")):
        print(
            "trades_smoke.py: error: POLYMARKET_TRADES_URL must be an HTTP(S) URL", file=sys.stderr
        )
        return EXIT_USAGE
    modes = list(COVERAGE_MODES) if args.coverage == "both" else [args.coverage]
    build = client_factory or httpx.AsyncClient
    records = asyncio.run(
        run_smoke(build(), url=url, coverage_modes=modes, window_seconds=args.window_seconds)
    )
    _render(records, args.json)
    return exit_code_for(records)


if __name__ == "__main__":
    sys.exit(main())
