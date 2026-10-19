"""Bounded per-coverage-mode probe execution for the live-safe smoke check."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

import httpx

from polymarket_insider_tracker.ingestor.trades_source import (
    TradesSourceClient,
    TradesTerminalError,
    TradesTransientError,
    redacted_url,
)
from scripts._smoke_records import (
    Clock,
    Probe,
    Sleeper,
    SmokeRecord,
    page_record,
    terminal_record,
    transient_record,
)

SMOKE_MAX_RETRIES = 2
COVERAGE_MODES = ("all", "taker-only")


async def _probe(
    client: httpx.AsyncClient,
    probe: Probe,
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
        return transient_record(probe, exc, delays)
    except TradesTerminalError as exc:
        return terminal_record(probe, exc)
    return page_record(probe, page)


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
            probe = Probe(
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
