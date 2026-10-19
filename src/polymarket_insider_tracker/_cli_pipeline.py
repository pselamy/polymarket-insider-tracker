"""Pipeline runtime for the CLI: graceful stop, exit codes, and the run loop."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from polymarket_insider_tracker._cli_redaction import RedactingLogFilter
from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from polymarket_insider_tracker.redaction import redact_exception_message
from polymarket_insider_tracker.shutdown import GracefulShutdown, wait_bounded

EXIT_SUCCESS = 0
EXIT_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_INTERRUPTED = 130


def build_wait_tasks(shutdown: GracefulShutdown, pipeline: Pipeline) -> list[asyncio.Task[object]]:
    tasks: list[asyncio.Task[object]] = [asyncio.create_task(shutdown.wait())]
    if pipeline.stop_event is not None:
        tasks.append(asyncio.create_task(pipeline.stop_event.wait()))
    return tasks


async def wait_for_stop_or_shutdown(shutdown: GracefulShutdown, pipeline: Pipeline) -> None:
    tasks = build_wait_tasks(shutdown, pipeline)
    _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


def exit_code_for_pipeline(pipeline: Pipeline) -> int:
    # Only a terminal pipeline failure is exit 1; recoverable per-trade or metadata
    # errors counted in stats must not turn a graceful shutdown into a failure.
    if pipeline.state == PipelineState.ERROR:
        return EXIT_ERROR
    return EXIT_SUCCESS


class SingleStopGuard:
    """Exactly one pipeline.stop() attempt across the explicit path and shutdown cleanup.

    ``run_pipeline`` stops the pipeline under the shutdown timeout and the registered
    cleanup callback would otherwise stop it again under a second, fresh timeout —
    doubling the worst-case shutdown time. Whichever path runs first consumes the one
    attempt; the other returns immediately (also after a timed-out, abandoned attempt).
    """

    def __init__(self, pipeline: Pipeline) -> None:
        self._pipeline = pipeline
        self._attempted = False

    async def stop(self) -> None:
        if self._attempted:
            return
        self._attempted = True
        await self._pipeline.stop()


async def stop_pipeline_within(stopper: SingleStopGuard, timeout: float) -> bool:
    """Stop the pipeline, bounded by the shutdown timeout; True means it finished.

    The deadline holds even when the stop coroutine suppresses cancellation: the
    attempt is abandoned at the deadline and reported as a timeout regardless of
    whether it later completes on its own.
    """
    if await wait_bounded(stopper.stop(), timeout):
        return True
    logging.getLogger(__name__).error(
        "Graceful pipeline stop exceeded the %.1fs shutdown timeout", timeout
    )
    return False


async def run_pipeline(
    settings: Settings,
    dry_run: bool,
    shutdown_timeout: float = 30.0,
    *,
    pipeline_factory: Callable[..., Pipeline] = Pipeline,
) -> int:
    """Run the main pipeline with graceful shutdown handling.

    Args:
        settings: Application settings.
        dry_run: Whether to skip sending alerts.
        shutdown_timeout: Maximum time to wait for graceful shutdown; a stop or cleanup
            that exceeds it is abandoned and the process exits with code 1.
        pipeline_factory: Pipeline constructor, replaceable for lifecycle tests.

    Returns:
        Exit code.
    """
    logger = logging.getLogger(__name__)
    logger.addFilter(RedactingLogFilter())
    shutdown = GracefulShutdown(timeout=shutdown_timeout)

    try:
        async with shutdown:
            pipeline = pipeline_factory(settings, dry_run=dry_run)

            # Register pipeline cleanup; the guard makes the explicit stop below and
            # this callback one single attempt under one shutdown deadline.
            stopper = SingleStopGuard(pipeline)
            shutdown.register_cleanup(stopper.stop)

            logger.info("Starting pipeline...")
            await pipeline.start()

            logger.info("Pipeline running. Press Ctrl+C to stop.")

            await wait_for_stop_or_shutdown(shutdown, pipeline)

            logger.info("Shutdown signal received, stopping pipeline...")
            stopped_in_time = await stop_pipeline_within(stopper, shutdown_timeout)

        if not stopped_in_time:
            return EXIT_ERROR
        return exit_code_for_pipeline(pipeline)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return EXIT_INTERRUPTED
    except Exception as e:
        logger.exception("Pipeline failed: %s", redact_exception_message(e))
        return EXIT_ERROR
