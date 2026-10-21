"""``PipelineAccess``: swapping collaborators and running the services of a wired pipeline."""

from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from tests.fakes import (
    FailingDetector,
    FakeClock,
    FakeEth,
    FakeTradesServer,
    detector_trade,
    make_test_settings,
    wire_pipeline,
)
from tests.fakes.pipeline_access import PipelineAccess


def _state(pipeline: Pipeline) -> PipelineState:
    return pipeline.state


async def _wired(*, with_trades: bool = False) -> Pipeline:
    clock = FakeClock(1_788_983_720.0)
    return await wire_pipeline(
        make_test_settings(),
        redis=FakeAsyncRedis(),
        eth=FakeEth(),
        trades=FakeTradesServer(clock=clock) if with_trades else None,
        poll_clock=clock,
    )


async def test_replaced_detector_runs_and_omitted_collaborators_are_kept() -> None:
    pipeline = await _wired()

    PipelineAccess.replace_components(
        pipeline, fresh_wallet_detector=FailingDetector(RuntimeError("wallet down"))
    )
    await pipeline.process_trade(detector_trade())

    assert pipeline.stats.errors == 1
    assert pipeline.stats.last_error is not None
    assert "wallet down" in pipeline.stats.last_error


async def test_a_wired_pipeline_exposes_its_trade_poller_only_when_trades_are_wired() -> None:
    assert PipelineAccess.trade_poller(await _wired(with_trades=True)).status.coverage
    with pytest.raises(AssertionError, match="without a trades server"):
        PipelineAccess.trade_poller(await _wired())


async def test_background_services_run_the_pipeline_and_stop_it_afterwards() -> None:
    pipeline = await _wired(with_trades=True)
    assert _state(pipeline) is PipelineState.STOPPED

    async with PipelineAccess.background_services(pipeline, timeout=2.0):
        assert _state(pipeline) is PipelineState.RUNNING

    assert _state(pipeline) is PipelineState.STOPPED
    assert not PipelineAccess.trade_poller(pipeline).is_running


async def test_background_services_keep_a_terminal_error_state() -> None:
    pipeline = await _wired(with_trades=True)

    async with PipelineAccess.background_services(pipeline, timeout=2.0):
        pipeline.enter_terminal_state()

    assert _state(pipeline) is PipelineState.ERROR
