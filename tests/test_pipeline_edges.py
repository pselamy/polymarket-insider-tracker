"""Pipeline lifecycle and degraded-dependency edges, driven through the public seams.

Every pipeline here is a real ``Pipeline`` on real components (``wire_pipeline`` installs them
through ``install_components``); only external boundaries are doubles, and failures are injected
at those boundaries or by ``TradePoller`` subclasses that override its public lifecycle.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import socket
import threading

import pytest
from fakeredis import FakeAsyncRedis, FakeServer
from sqlalchemy.ext.asyncio import AsyncEngine

from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import TradePoller
from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from polymarket_insider_tracker.storage.database import DatabaseManager
from tests.fakes import (
    FakeAlertChannel,
    FakeBaseClobClient,
    FakeClock,
    FakeEth,
    WiredPipeline,
    make_test_settings,
    publish_after_first_poll,
    run_until,
    wire_pipeline,
)
from tests.fakes.database import UNUSED_DATABASE_URL, FailingSessionDatabase
from tests.integration._journey_harness import (
    make_sample_trade_event,
    persisted_assessments,
    pipeline_row,
    seeded_server,
    stamped_volume_market,
)

POLL_START = 1_788_983_720.0
PIPELINE_LOGGER = "polymarket_insider_tracker.pipeline"


@pytest.fixture
def trade() -> TradeEvent:
    return make_sample_trade_event()


@pytest.fixture
def niche_market(trade: TradeEvent) -> MarketMetadata:
    """A market whose stamped volume makes the sample trade score 0.45 (above 0.4)."""
    return stamped_volume_market(trade)


class CrashingPoller(TradePoller):
    """A poller whose loop dies with an unexpected error as soon as it starts."""

    async def start(self) -> None:
        raise RuntimeError("poll loop crashed")


class StopDeafPoller(TradePoller):
    """A poller whose loop ignores ``stop()``, so only cancelling its task ends it."""

    async def start(self) -> None:
        await asyncio.Event().wait()


class StopDuringSendChannel(FakeAlertChannel):
    """A delivery during which the operator stops the pipeline that owns the connections."""

    pipeline: Pipeline | None = None

    async def send(self, alert: FormattedAlert) -> bool:
        if self.pipeline is not None:
            await self.pipeline.stop()
        return await super().send(alert)


class DatabaseOutageChannel(FakeAlertChannel):
    """A delivery during which the assessment database becomes unreachable."""

    def __init__(self, name: str, database: FailingSessionDatabase) -> None:
        super().__init__(name)
        self.database = database

    async def send(self, alert: FormattedAlert) -> bool:
        self.database.failure = ConnectionError("database went away")
        return await super().send(alert)


async def _polling_pipeline(
    redis: FakeAsyncRedis, port: int, poller_type: type[TradePoller] = TradePoller
) -> WiredPipeline:
    clock = FakeClock(POLL_START)
    return await wire_pipeline(
        make_test_settings(health_port=port),
        redis=redis,
        eth=FakeEth(transaction_count=100),
        trades=seeded_server(clock),
        poll_clock=clock,
        poller_type=poller_type,
    )


async def test_unbindable_health_port_fails_start_and_closes_built_connections(
    unused_tcp_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    pipeline = Pipeline(make_test_settings(health_port=unused_tcp_port))
    with socket.socket() as taken:
        taken.bind(("0.0.0.0", unused_tcp_port))
        taken.listen()
        with caplog.at_level(logging.ERROR, logger=PIPELINE_LOGGER), pytest.raises(OSError):
            await pipeline.start()

    assert pipeline.state is PipelineState.ERROR
    assert pipeline.db_manager is None
    assert f"Failed to bind health server on port {unused_tcp_port}" in caplog.text


async def test_unreachable_redis_is_reported_down() -> None:
    server = FakeServer()
    pipeline = await wire_pipeline(
        make_test_settings(), redis=FakeAsyncRedis(server=server), eth=FakeEth()
    )
    server.connected = False

    ready, _reason, summary = await pipeline.check_readiness()

    assert ready is False
    assert summary["redis"] == "down"


async def test_polled_trade_arrival_time_reaches_the_health_monitor(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int, trade: TradeEvent
) -> None:
    clock = FakeClock(POLL_START)
    server = seeded_server(clock)
    pipeline = await wire_pipeline(
        make_test_settings(health_port=unused_tcp_port),
        redis=fake_redis,
        eth=FakeEth(transaction_count=100),
        trades=server,
        poll_clock=clock,
    )
    poller = pipeline.poller
    assert poller is not None
    await pipeline.start()
    try:
        await publish_after_first_poll(server, pipeline_row(trade, clock))
        await run_until(lambda: poller.status.last_trade_at is not None)
        await pipeline.check_readiness()
    finally:
        await pipeline.stop()

    last_trade_at = poller.status.last_trade_at
    assert last_trade_at is not None
    assert pipeline.health_monitor.last_trade_time == last_trade_at.timestamp()


async def test_trade_before_components_are_installed_is_counted_without_detection(
    trade: TradeEvent,
) -> None:
    pipeline = Pipeline(make_test_settings())

    await pipeline.process_trade(trade)

    assert pipeline.stats.trades_processed == 1
    assert (pipeline.stats.signals_generated, pipeline.stats.errors) == (0, 0)


async def test_crashing_poller_puts_the_pipeline_into_error(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    pipeline = await _polling_pipeline(fake_redis, unused_tcp_port, CrashingPoller)
    await pipeline.start()
    try:
        await run_until(lambda: pipeline.state is PipelineState.ERROR)
    finally:
        await pipeline.stop()

    assert pipeline.state is PipelineState.ERROR
    assert "poll loop crashed" in (pipeline.stats.last_error or "")
    assert pipeline.stop_event is not None and pipeline.stop_event.is_set()


async def test_poller_that_ignores_stop_is_cancelled_quietly(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    pipeline = await _polling_pipeline(fake_redis, unused_tcp_port, StopDeafPoller)
    await pipeline.start()
    await asyncio.sleep(0)

    await asyncio.wait_for(pipeline.stop(), timeout=2.0)

    assert pipeline.state is PipelineState.STOPPED
    assert pipeline.stats.errors == 0


async def test_poller_detached_before_its_task_runs_is_never_started(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    pipeline = await _polling_pipeline(fake_redis, unused_tcp_port)
    assert pipeline.components is not None and pipeline.poller is not None
    await pipeline.start()
    pipeline.install_components(dataclasses.replace(pipeline.components, trade_poller=None))
    try:
        for _ in range(20):
            await asyncio.sleep(0)
    finally:
        await pipeline.stop()

    assert pipeline.poller.is_running is False
    assert pipeline.poller.status.last_success_at is None
    assert pipeline.stats.errors == 0


async def test_failed_metadata_crawl_is_counted_and_ingestion_keeps_running(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    clob = FakeBaseClobClient()
    clob.markets_error = ConnectionError("clob unreachable")
    pipeline = await wire_pipeline(
        make_test_settings(health_port=unused_tcp_port),
        redis=fake_redis,
        eth=FakeEth(),
        clob=clob,
        clob_max_retries=0,
    )
    await pipeline.start()
    try:
        await run_until(lambda: pipeline.stats.errors >= 1)
        assert pipeline.state is PipelineState.RUNNING
    finally:
        await pipeline.stop()

    assert "initial sync failed" in (pipeline.stats.last_error or "")


async def test_stop_during_a_blocked_metadata_crawl_cancels_it_quietly(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    gate = threading.Event()
    log: list[str] = []
    pipeline = await wire_pipeline(
        make_test_settings(health_port=unused_tcp_port),
        redis=fake_redis,
        eth=FakeEth(),
        crawl_gate=gate,
        state_log=log,
    )
    await pipeline.start()
    try:
        await run_until(lambda: "metadata:syncing" in log)
        await asyncio.wait_for(pipeline.stop(), timeout=2.0)
    finally:
        gate.set()

    assert pipeline.state is PipelineState.STOPPED
    assert pipeline.stats.errors == 0
    assert "metadata:idle" not in log


async def _running_run(redis: FakeAsyncRedis, port: int) -> tuple[Pipeline, asyncio.Task[None]]:
    """A pipeline whose ``run()`` task has started it and is waiting for the stop signal."""
    pipeline = await wire_pipeline(make_test_settings(health_port=port), redis=redis, eth=FakeEth())
    runner = asyncio.create_task(pipeline.run())
    await run_until(lambda: pipeline.is_running)
    return pipeline, runner


async def test_run_returns_once_the_pipeline_is_stopped(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    pipeline, runner = await _running_run(fake_redis, unused_tcp_port)

    await pipeline.stop()
    await asyncio.wait_for(runner, timeout=2.0)

    assert pipeline.state is PipelineState.STOPPED


async def test_cancelled_run_stops_the_pipeline_and_returns(
    fake_redis: FakeAsyncRedis, unused_tcp_port: int
) -> None:
    pipeline, runner = await _running_run(fake_redis, unused_tcp_port)

    runner.cancel()
    await asyncio.wait_for(runner, timeout=2.0)

    assert runner.cancelled() is False
    assert pipeline.state is PipelineState.STOPPED


async def test_refused_delivery_is_logged_with_its_disposition(
    fake_redis: FakeAsyncRedis,
    trade: TradeEvent,
    niche_market: MarketMetadata,
    caplog: pytest.LogCaptureFixture,
) -> None:
    channel = FakeAlertChannel("discord", accepting=False)
    pipeline = await wire_pipeline(
        make_test_settings(persist_assessments=False, alert_threshold=0.4),
        redis=fake_redis,
        eth=FakeEth(transaction_count=0),
        market=niche_market,
        channels=[channel],
    )

    with caplog.at_level(logging.WARNING, logger=PIPELINE_LOGGER):
        await pipeline.process_trade(trade)

    assert len(channel.deliveries) == 1
    assert pipeline.stats.alerts_sent == 0
    assert "Alert dispatch disposition=failed (0/1 succeeded)" in caplog.text


async def test_below_threshold_assessment_is_not_persisted_when_disabled(
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    trade: TradeEvent,
) -> None:
    pipeline = await wire_pipeline(
        make_test_settings(persist_assessments=False),
        redis=fake_redis,
        eth=FakeEth(),
        db_manager=db_manager,
    )

    await pipeline.process_trade(trade)

    assert pipeline.stats.signals_generated == 1
    assert await persisted_assessments(async_engine) == []


async def test_database_lost_during_delivery_keeps_the_pending_row_and_counts_both_failures(
    fake_redis: FakeAsyncRedis,
    async_engine: AsyncEngine,
    trade: TradeEvent,
    niche_market: MarketMetadata,
) -> None:
    database = FailingSessionDatabase(UNUSED_DATABASE_URL)
    database.attach_async_engine(async_engine)
    channel = DatabaseOutageChannel("discord", database)
    pipeline = await wire_pipeline(
        make_test_settings(alert_threshold=0.4),
        redis=fake_redis,
        eth=FakeEth(transaction_count=0),
        market=niche_market,
        db_manager=database,
        channels=[channel],
    )

    await pipeline.process_trade(trade)

    assert len(channel.deliveries) == 1
    rows = await persisted_assessments(async_engine)
    assert [row.delivery_disposition for row in rows] == ["unrecorded"]
    assert pipeline.stats.errors == 2
    assert pipeline.stats.last_error is not None
    assert "Failed to persist risk assessment" in pipeline.stats.last_error


async def test_stop_during_delivery_keeps_the_pending_row_without_an_error(
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    unused_tcp_port: int,
    trade: TradeEvent,
    niche_market: MarketMetadata,
) -> None:
    channel = StopDuringSendChannel("discord")
    pipeline = await wire_pipeline(
        make_test_settings(alert_threshold=0.4, health_port=unused_tcp_port),
        redis=fake_redis,
        eth=FakeEth(transaction_count=0),
        market=niche_market,
        db_manager=db_manager,
        channels=[channel],
        owns_connections=True,
    )
    channel.pipeline = pipeline
    await pipeline.start()

    await pipeline.process_trade(trade)

    assert pipeline.state is PipelineState.STOPPED
    assert pipeline.db_manager is None
    assert len(channel.deliveries) == 1
    rows = await persisted_assessments(async_engine)
    assert [row.delivery_disposition for row in rows] == ["unrecorded"]
    assert pipeline.stats.errors == 0
