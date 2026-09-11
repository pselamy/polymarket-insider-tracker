"""CLI entry point for Polymarket Insider Tracker.

This module provides the main entry point for running the tracker
from the command line.

Usage:
    python -m polymarket_insider_tracker [options]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.config
import sys
from collections.abc import Callable
from typing import NoReturn

from pydantic import ValidationError

from polymarket_insider_tracker import __version__
from polymarket_insider_tracker.config import (
    Settings,
    clear_settings_cache,
    get_settings,
    websocket_deprecation_message,
)
from polymarket_insider_tracker.pipeline import Pipeline, PipelineState
from polymarket_insider_tracker.shutdown import GracefulShutdown

# Application info
APP_NAME = "Polymarket Insider Tracker"
APP_VERSION = __version__

# Exit codes
EXIT_SUCCESS = 0
EXIT_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_INTERRUPTED = 130


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the CLI.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="polymarket-insider-tracker",
        description="Detect insider trading activity on Polymarket prediction markets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m polymarket_insider_tracker           Run full pipeline
  python -m polymarket_insider_tracker --config-check  Validate config and exit
  python -m polymarket_insider_tracker --dry-run       Run without sending alerts
  python -m polymarket_insider_tracker --log-level DEBUG  Enable debug logging
        """,
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {APP_VERSION}",
    )

    parser.add_argument(
        "--config-check",
        action="store_true",
        help="Validate configuration and exit without running pipeline",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=None,
        help="Override logging level (default: from settings)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline but don't send alerts",
    )

    parser.add_argument(
        "--health-port",
        type=int,
        default=None,
        help="Override health check port (default: from settings)",
    )

    return parser


def configure_logging(level: str) -> None:
    """Configure structured logging for the application.

    Args:
        level: Logging level string (DEBUG, INFO, etc.)
    """
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": (
                    "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level,
                "formatter": "detailed" if level == "DEBUG" else "standard",
                "stream": "ext://sys.stdout",
            },
        },
        "root": {
            "level": level,
            "handlers": ["console"],
        },
        # Quieter logging for noisy libraries
        "loggers": {
            "httpx": {"level": "WARNING"},
            "httpcore": {"level": "WARNING"},
            "websockets": {"level": "WARNING"},
            "web3": {"level": "WARNING"},
            "urllib3": {"level": "WARNING"},
        },
    }
    logging.config.dictConfig(config)


def print_banner() -> None:
    """Print the application startup banner."""
    banner = f"""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   {APP_NAME:^56}   ║
║   {"v" + APP_VERSION:^56}   ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"""
    print(banner)


def print_config_summary(settings: Settings, dry_run: bool) -> None:
    """Print a summary of the configuration.

    Args:
        settings: Application settings.
        dry_run: Whether dry-run mode is enabled.
    """
    summary = settings.redacted_summary()
    print("Configuration:")
    print(f"  Database: {summary['database_url']}")
    print(f"  Redis: {summary['redis_url']}")
    print_trades_source_summary(settings)
    print(f"  Log Level: {summary['log_level']}")
    print(f"  Health Port: {summary['health_port']}")
    print(f"  Dry Run: {dry_run}")
    print(f"  Discord: {'enabled' if summary['discord_enabled'] == 'True' else 'disabled'}")
    print(f"  Telegram: {'enabled' if summary['telegram_enabled'] == 'True' else 'disabled'}")
    print()


def print_trades_source_summary(settings: Settings) -> None:
    """Print the supported trade source settings and the legacy WebSocket disposition."""
    polymarket = settings.polymarket
    print(f"  Trades URL: {polymarket.trades_url}")
    print(f"  Trades Coverage: {polymarket.trades_coverage.value}")
    print(f"  Trades Poll Interval: {polymarket.trades_poll_interval_seconds}s")
    print(f"  Trades Recovery Horizon: {polymarket.trades_recovery_horizon_seconds}s")
    print(f"  WebSocket URL: {polymarket.ws_url_disposition}")
    if polymarket.ws_url is not None:
        print(f"  WARNING: {websocket_deprecation_message()}")


def _format_error_field(loc: tuple[int | str, ...]) -> str:
    return ".".join(str(part) for part in loc)


def _print_validation_errors(exc: ValidationError) -> None:
    print("Configuration validation failed:", file=sys.stderr)
    for error in exc.errors():
        field = _format_error_field(error["loc"])
        msg = error["msg"]
        print(f"  {field}: {msg}", file=sys.stderr)


def _validate_health_port(health_port: int | None) -> bool:
    if health_port is None:
        return True
    return 1 <= health_port <= 65535


def validate_config(health_port: int | None = None) -> Settings | None:
    """Validate and load configuration.

    Args:
        health_port: Optional CLI override for health port.

    Returns:
        Settings instance if valid, None if invalid.
    """
    if not _validate_health_port(health_port):
        print("Configuration validation failed:", file=sys.stderr)
        print(
            f"  health_port: Port must be between 1 and 65535, got {health_port}",
            file=sys.stderr,
        )
        return None
    try:
        # Clear cache to force reload
        clear_settings_cache()
        settings = get_settings()
        if health_port is not None:
            settings.health_port = health_port
        return settings
    except ValidationError as e:
        _print_validation_errors(e)
        return None


def run_config_check(settings: Settings) -> int:
    """Run configuration check and exit.

    Args:
        settings: Validated settings.

    Returns:
        Exit code (0 for success).
    """
    print("Configuration is valid! Offline syntax and structure checks passed.")
    print(
        "Note: --config-check validates syntax and configuration only; runtime readiness"
        " requires reachable PostgreSQL, Redis, and trade acquisition sources."
    )
    print()
    print_config_summary(settings, dry_run=False)

    # Test component availability
    print("Checking component availability...")

    # Check Discord
    if settings.discord.enabled:
        print("  Discord: configured")
    else:
        print("  Discord: not configured")

    # Check Telegram
    if settings.telegram.enabled:
        print("  Telegram: configured")
    else:
        print("  Telegram: not configured")

    return EXIT_SUCCESS


def _build_wait_tasks(shutdown: GracefulShutdown, pipeline: Pipeline) -> list[asyncio.Task[object]]:
    tasks: list[asyncio.Task[object]] = [asyncio.create_task(shutdown.wait())]
    if pipeline.stop_event is not None:
        tasks.append(asyncio.create_task(pipeline.stop_event.wait()))
    return tasks


async def _wait_for_stop_or_shutdown(shutdown: GracefulShutdown, pipeline: Pipeline) -> None:
    tasks = _build_wait_tasks(shutdown, pipeline)
    _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


def _exit_code_for_pipeline(pipeline: Pipeline) -> int:
    # Only a terminal pipeline failure is exit 1; recoverable per-trade or metadata
    # errors counted in stats must not turn a graceful shutdown into a failure.
    if pipeline.state == PipelineState.ERROR:
        return EXIT_ERROR
    return EXIT_SUCCESS


class _SingleStopGuard:
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


async def _stop_pipeline_within(stopper: _SingleStopGuard, timeout: float) -> bool:
    """Stop the pipeline, bounded by the shutdown timeout; True means it finished."""
    try:
        await asyncio.wait_for(stopper.stop(), timeout=timeout)
        return True
    except TimeoutError:
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
    shutdown = GracefulShutdown(timeout=shutdown_timeout)

    try:
        async with shutdown:
            pipeline = pipeline_factory(settings, dry_run=dry_run)

            # Register pipeline cleanup; the guard makes the explicit stop below and
            # this callback one single attempt under one shutdown deadline.
            stopper = _SingleStopGuard(pipeline)
            shutdown.register_cleanup(stopper.stop)

            logger.info("Starting pipeline...")
            await pipeline.start()

            logger.info("Pipeline running. Press Ctrl+C to stop.")

            await _wait_for_stop_or_shutdown(shutdown, pipeline)

            logger.info("Shutdown signal received, stopping pipeline...")
            stopped_in_time = await _stop_pipeline_within(stopper, shutdown_timeout)

        if not stopped_in_time:
            return EXIT_ERROR
        return _exit_code_for_pipeline(pipeline)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return EXIT_INTERRUPTED
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return EXIT_ERROR


def main(argv: list[str] | None = None) -> NoReturn:
    """Main entry point for the CLI.

    Args:
        argv: Command line arguments (defaults to sys.argv[1:]).
    """
    parser = create_parser()
    args = parser.parse_args(argv)

    # Validate configuration first with CLI health port override
    settings = validate_config(health_port=args.health_port)
    if settings is None:
        sys.exit(EXIT_CONFIG_ERROR)

    # Determine effective log level
    log_level = args.log_level or settings.log_level
    configure_logging(log_level)

    # Print banner
    print_banner()

    # Config check mode
    if args.config_check:
        sys.exit(run_config_check(settings))

    # Determine dry-run mode
    dry_run = args.dry_run or settings.dry_run

    # Print config summary
    print_config_summary(settings, dry_run)

    # Run pipeline
    exit_code = asyncio.run(run_pipeline(settings, dry_run))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
