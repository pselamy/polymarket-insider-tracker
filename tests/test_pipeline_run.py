"""``Pipeline.run``: start, wait for the stop signal, stop, whichever way the wait ends."""

from __future__ import annotations

import asyncio

from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from tests.fakes import FakeEth, make_test_settings, run_until, wire_pipeline


class NothingStartedPipeline(Pipeline):
    """A subclass whose ``start`` brings nothing up, so no stop signal is ever armed."""

    async def start(self) -> None:
        return None


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


async def test_run_without_an_armed_stop_signal_stops_at_once() -> None:
    pipeline = NothingStartedPipeline(make_test_settings())

    await asyncio.wait_for(pipeline.run(), timeout=2.0)

    assert pipeline.stop_event is None
    assert pipeline.state is PipelineState.STOPPED
