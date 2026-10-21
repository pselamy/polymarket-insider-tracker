"""Subclass-level access to the protected collaborators of a wired ``Pipeline``.

``tests.fakes.pipeline.wire_pipeline`` composes a real ``Pipeline`` out of fakes, so tests need
three things the product API does not expose: the wired trade poller, a way to swap detection
collaborators, and a way to run the background services without ``start()`` building real
components. Single-underscore members are protected: a subclass may use them, so the access lives
here, in one typed class, instead of being repeated across tests. The class is never instantiated;
its static methods take the pipeline that ``wire_pipeline`` returned.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.detector.evaluation import SizeEvaluation, WalletEvaluation
from polymarket_insider_tracker.ingestor.trade_poller import TradePoller
from polymarket_insider_tracker.pipeline import Evaluator, Pipeline, PipelineState


class PipelineAccess(Pipeline):
    """Protected-member access for tests that drive a pipeline composed by ``wire_pipeline``."""

    @staticmethod
    def trade_poller(pipeline: Pipeline) -> TradePoller:
        """The wired poller, whose ``status`` reports the ingestion boundary."""
        poller = pipeline._trade_poller
        assert poller is not None, "the pipeline was wired without a trades server"
        return poller

    @staticmethod
    def replace_components(
        pipeline: Pipeline,
        *,
        fresh_wallet_detector: Evaluator[WalletEvaluation] | None = None,
        size_anomaly_detector: Evaluator[SizeEvaluation] | None = None,
        alert_formatter: AlertFormatter | None = None,
    ) -> None:
        """Swap detection or formatting collaborators; omitted ones are kept."""
        pipeline._fresh_wallet_detector = fresh_wallet_detector or pipeline._fresh_wallet_detector
        pipeline._size_anomaly_detector = size_anomaly_detector or pipeline._size_anomaly_detector
        pipeline._alert_formatter = alert_formatter or pipeline._alert_formatter

    @staticmethod
    @asynccontextmanager
    async def background_services(
        pipeline: Pipeline, *, timeout: float | None = None
    ) -> AsyncGenerator[None]:
        """Run the poller, metadata crawl and health server of a wired pipeline.

        ``start()`` without component initialization and ``stop()`` without resource cleanup: the
        caller keeps Redis and the database. The pipeline is RUNNING inside the block (unless a
        worker failed terminally first) and STOPPED after it unless it ended in ERROR.
        """
        pipeline._state = PipelineState.STARTING
        try:
            await asyncio.wait_for(pipeline._start_background_services(), timeout)
            if pipeline.state is PipelineState.STARTING:
                pipeline._state = PipelineState.RUNNING
            yield
        finally:
            await asyncio.wait_for(pipeline._stop_background_services(), timeout)
            if pipeline.state is not PipelineState.ERROR:
                pipeline._state = PipelineState.STOPPED
