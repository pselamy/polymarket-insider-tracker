"""Fakes for the ingestion state a pipeline reports: a stand-in poller and polling helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime

from redis.asyncio import Redis

from polymarket_insider_tracker.ingestor.health import ComponentStatus
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import (
    IngestionState,
    IngestionStatus,
    TradePoller,
)
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.storage.database import DatabaseManager
from tests.fakes.pipeline import Substitutes, make_test_settings, wire_pipeline
from tests.fakes.trades import FakeClock, FakeTradesServer
from tests.fakes.web3 import FakeEth


async def _ignore_trade(trade: TradeEvent) -> None:
    _ = trade


class StubPoller(TradePoller):
    """A real poller over an idle fake trades server that reports a chosen ingestion state.

    The readiness mapping tests need states the real loop reaches only after many failed cycles.
    The overrides change only what that mapping reads: the state, the running flag, the age of
    the last success and the status snapshot that carries them.
    """

    def __init__(
        self,
        redis: Redis,
        state: IngestionState,
        last_error: str | None = None,
        seconds_since_last_success: float | None = 1.0,
    ) -> None:
        clock = FakeClock(0.0)
        super().__init__(
            _ignore_trade,
            redis=redis,
            settings=make_test_settings().polymarket,
            http_client=FakeTradesServer(clock=clock).client(),
            clock=clock,
            sleeper=clock.sleep,
        )
        self._reported_state = state
        self._reported_error = last_error
        self._reported_age = seconds_since_last_success

    @property
    def state(self) -> IngestionState:
        return self._reported_state

    @property
    def is_running(self) -> bool:
        return True

    @property
    def seconds_since_last_success(self) -> float | None:
        return self._reported_age

    @property
    def status(self) -> IngestionStatus:
        last_success = datetime.now(UTC) if self._reported_age is not None else None
        return replace(
            super().status,
            state=self._reported_state,
            last_error=self._reported_error,
            last_success_at=last_success,
        )


async def ingestion_component(pipeline: Pipeline) -> ComponentStatus:
    """The ingestion component as the pipeline's health monitor evaluates it."""
    return (await pipeline.health_monitor.evaluate_components())["ingestion"]


async def run_until(predicate: Callable[[], bool], *, cycles: int = 25000) -> None:
    """Yield to the event loop until ``predicate`` holds; fail if it never does."""
    for _ in range(cycles):
        if predicate():
            return
        await asyncio.sleep(0.0005)
    raise AssertionError("condition was not reached")


async def stubbed_pipeline(
    redis: Redis,
    state: IngestionState,
    last_error: str | None = None,
    age: float | None = 1.0,
    db_manager: DatabaseManager | None = None,
) -> Pipeline:
    """A wired pipeline whose trade source reports ``state``, an error and a last-success age."""
    return await wire_pipeline(
        make_test_settings(),
        redis=redis,
        eth=FakeEth(),
        db_manager=db_manager,
        substitutes=Substitutes(poller=StubPoller(redis, state, last_error, age)),
    )


async def publish_after_first_poll(server: FakeTradesServer, row: dict[str, object]) -> None:
    """Publish ``row`` once the poller has made its first request, so it is a new observation."""
    await run_until(lambda: len(server.requests) >= 1)
    server.publish(row)
