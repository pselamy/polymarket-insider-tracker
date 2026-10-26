"""T027 Part B ground-truth capture (research.md R09a, protocol v2).

Runs the supported ingestion (the real ``TradePoller``: official v2 trades query, all-participant
coverage, 5 s cadence, 600 s horizon) for a fixed duration against an isolated Redis database and
records, append-only, what it delivered and what it could not prove:

- ``trades.jsonl``: one line per delivered observation (observation identity, lowercased proxy
  wallet, notional, provider timestamp, capture time);
- ``status.jsonl``: a status snapshot every ``STATUS_EVERY_SECONDS`` (state, boundary, failures,
  every loss event the observation boundary holds).

This is the Part A/B shared ground truth: trades counted per address by observation identity, and
proven coverage = the capture window minus every ``possible-data-loss`` interval. Read-only and
anonymous; it sends no credential and writes only to the Redis database it is given, which must be
empty at start (the run refuses otherwise, so application state is never touched).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import sys
import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import IO, Protocol

from redis.asyncio import Redis

from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import TradeCallback, TradePoller
from polymarket_insider_tracker.ingestor.trade_rows import canonical_decimal
from polymarket_insider_tracker.pipeline_components import RedisFactory

DESCRIPTION = "T027 Part B ground-truth capture (research.md R09a, protocol v2)."

STATUS_EVERY_SECONDS = 60.0
CAPTURE_SETTINGS = {
    "POLYMARKET_TRADES_URL": OFFICIAL_V2_TRADES_URL,
    "POLYMARKET_TRADES_COVERAGE": "all",
    "POLYMARKET_TRADES_POLL_INTERVAL_SECONDS": 5,
    "POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS": 600,
}


def event_identity(event: TradeEvent) -> str:
    """The slice-001 observation identity of a delivered event (``trade_rows.observation_identity``).

    Every identity field is carried unchanged on the event, so the hash is recomputed from it; a
    contract test pins this against ``parse_trade_row`` for the same raw row.
    """
    parts = (
        event.trade_id,
        event.wallet_address.lower(),
        event.market_id,
        event.asset_id,
        event.side,
        canonical_decimal(event.price),
        canonical_decimal(event.size),
        str(int(event.timestamp.timestamp())),
    )
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def trade_record(event: TradeEvent, captured_at: float) -> dict[str, object]:
    return {
        "identity": event_identity(event),
        "wallet": event.wallet_address.lower(),
        "notional": canonical_decimal(event.notional_value),
        "ts": int(event.timestamp.timestamp()),
        "captured_at": round(captured_at, 3),
    }


def status_record(poller: TradePoller, at: float) -> dict[str, object]:
    status = poller.status
    return {
        "at": round(at, 3),
        "state": status.state.value,
        "boundary_time": status.boundary_time,
        "consecutive_failures": status.consecutive_failures,
        "counts": status.counts,
        "loss_events": [asdict(event) for event in poller.boundary.loss_events],
    }


def append_line(handle: IO[str], record: dict[str, object]) -> None:
    handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
    handle.flush()


def make_recorder(handle: IO[str], clock: Callable[[], float]) -> TradeCallback:
    async def on_trade(event: TradeEvent) -> None:
        append_line(handle, trade_record(event, clock()))

    return on_trade


class SizedRedis(Protocol):
    """The one call the emptiness check needs; redis-py types its reply loosely."""

    def dbsize(self) -> object: ...


def redis_factory() -> type[RedisFactory]:
    """The real ``Redis`` class viewed through its typed ``from_url`` (as the pipeline does)."""
    return Redis


def redis_client(url: str) -> Redis:
    return redis_factory().from_url(url)


async def require_empty(redis: SizedRedis) -> None:
    reply = redis.dbsize()
    size = await reply if inspect.isawaitable(reply) else reply
    if size != 0:
        raise SystemExit("refusing to capture: the Redis database is not empty")


class Supervisor:
    """Keep one poller running; a poller that stops early (FAILED) is replaced by a fresh one.

    The replacement reloads the durable boundary from Redis, so the ingestion's own restart rules
    decide whether the gap is recovered or recorded as a loss event; the capture adds no judgement.
    """

    def __init__(self, make_poller: Callable[[], TradePoller], status: IO[str]) -> None:
        self._make_poller = make_poller
        self._status = status
        self.poller = make_poller()
        self._task = asyncio.create_task(self.poller.start())

    def restart_if_stopped(self, at: float) -> None:
        if not self._task.done():
            return
        append_line(self._status, status_record(self.poller, at) | {"event": "restart"})
        self.poller = self._make_poller()
        self._task = asyncio.create_task(self.poller.start())

    async def stop(self) -> None:
        await self.poller.stop()
        await self._task


async def record_status_until(
    supervisor: Supervisor, handle: IO[str], deadline: float, clock: Callable[[], float]
) -> None:
    while clock() < deadline:
        await asyncio.sleep(min(STATUS_EVERY_SECONDS, max(0.0, deadline - clock())))
        append_line(handle, status_record(supervisor.poller, clock()))
        supervisor.restart_if_stopped(clock())


async def capture(out_dir: Path, duration_seconds: float, redis_url: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=False)
    redis = redis_client(redis_url)
    await require_empty(redis)
    settings = PolymarketSettings.model_validate(CAPTURE_SETTINGS)
    clock = time.time
    with (out_dir / "trades.jsonl").open("a") as trades, (out_dir / "status.jsonl").open("a") as st:
        recorder = make_recorder(trades, clock)
        append_line(st, {"at": round(clock(), 3), "event": "start", "settings": CAPTURE_SETTINGS})
        supervisor = Supervisor(lambda: TradePoller(recorder, redis=redis, settings=settings), st)
        await record_status_until(supervisor, st, clock() + duration_seconds, clock)
        await supervisor.stop()
        append_line(st, status_record(supervisor.poller, clock()) | {"event": "stop"})
    await redis.aclose()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--hours", type=float, required=True)
    parser.add_argument("--redis", required=True)
    args = parser.parse_args(argv)
    asyncio.run(capture(args.out, args.hours * 3600.0, args.redis))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
