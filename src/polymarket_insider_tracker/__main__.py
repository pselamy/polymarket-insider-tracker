"""CLI entry point for Polymarket Insider Tracker.

This module provides the main entry point for running the tracker
from the command line.

Usage:
    python -m polymarket_insider_tracker [options]
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
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
from polymarket_insider_tracker.redaction import redact_text, redact_url
from polymarket_insider_tracker.shutdown import GracefulShutdown, wait_bounded

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


def print_config_summary(settings: Settings) -> None:
    """Print a summary of the effective configuration, including CLI overrides.

    Args:
        settings: Application settings with every CLI override already applied.
    """
    summary = settings.redacted_summary()
    print("Configuration:")
    print(f"  Database: {summary['database_url']}")
    print(f"  Redis: {summary['redis_url']}")
    print_trades_source_summary(settings)
    print(f"  Log Level: {summary['log_level']}")
    print(f"  Health Port: {summary['health_port']}")
    print(f"  Dry Run: {settings.dry_run}")
    print(f"  Discord: {'enabled' if summary['discord_enabled'] == 'True' else 'disabled'}")
    print(f"  Telegram: {'enabled' if summary['telegram_enabled'] == 'True' else 'disabled'}")
    print()


def print_trades_source_summary(settings: Settings) -> None:
    """Print the supported trade source settings and the legacy WebSocket disposition."""
    polymarket = settings.polymarket
    print(f"  Trades URL: {redact_url(polymarket.trades_url)}")
    print(f"  Trades Coverage: {polymarket.trades_coverage.value}")
    print(f"  Trades Poll Interval: {polymarket.trades_poll_interval_seconds}s")
    print(f"  Trades Recovery Horizon: {polymarket.trades_recovery_horizon_seconds}s")
    print(f"  WebSocket URL: {polymarket.ws_url_disposition}")
    if polymarket.ws_url is not None:
        print(f"  WARNING: {websocket_deprecation_message()}")


def _format_error_field(loc: tuple[int | str, ...]) -> str:
    return ".".join(str(part) for part in loc)


def _sanitized_validation_message(msg: str) -> str:
    """Render a config-validation diagnostic without echoing raw input.

    Pydantic and ``urlsplit`` error text interpolates the supplied value
    (netloc/userinfo/path), which may carry a credential. The diagnostic keeps
    the safe failure class but never the raw input or the raw exception message.
    """
    lowered = msg.lower()
    if _is_host_component_failure(lowered):
        return "invalid host component: contains characters rejected by URL normalization"
    return _classified_validation_message(lowered)


def _classified_validation_message(lowered: str) -> str:
    """Map the failure class to its safe message without echoing the value."""
    scheme_message = _scheme_failure_message(lowered)
    if scheme_message is not None:
        return scheme_message
    return _non_scheme_validation_message(lowered)


_NON_SCHEME_VALIDATION_MESSAGES: tuple[tuple[str, str], ...] = (
    ("hostname", "must include a valid hostname"),
    ("port", "has an invalid port"),
)


def _non_scheme_validation_message(lowered: str) -> str:
    """Safe messages for hostname, port, database, and generic URL failures."""
    direct = _direct_marker_message(lowered)
    if direct is not None:
        return direct
    return _url_shape_validation_message(lowered)


def _direct_marker_message(lowered: str) -> str | None:
    """Hostname/port markers map one-to-one to their safe message."""
    for marker, message in _NON_SCHEME_VALIDATION_MESSAGES:
        if marker in lowered:
            return message
    return None


def _url_shape_validation_message(lowered: str) -> str:
    """Database, generic URL, and fallback messages without echoing the value."""
    if _is_database_url_failure(lowered):
        return "is not a valid PostgreSQL URL"
    if "url" in lowered and ("valid" in lowered or "scheme" in lowered):
        return "is not a valid URL"
    return "has an invalid value"


def _is_host_component_failure(lowered: str) -> bool:
    """URL normalization rejected the host component carrying the raw value."""
    return "nfkc" in lowered or "invalid characters" in lowered or "netloc" in lowered


_SCHEME_FAILURE_MESSAGES: tuple[tuple[str, str], ...] = (
    ("nested", "malformed nested scheme in its host component"),
    ("redis", "must start with redis://"),
    ("websocket", "must start with ws:// or wss://"),
    ("ws://", "must start with ws:// or wss://"),
    ("wss://", "must start with ws:// or wss://"),
)


def _scheme_failure_message(lowered: str) -> str | None:
    """The safe scheme expectation, or None when the message is not scheme-shaped."""
    for marker, message in _SCHEME_FAILURE_MESSAGES:
        if marker in lowered:
            return message
    if "http" in lowered and "endpoint" in lowered:
        return "must be an HTTP(S) endpoint"
    return None


def _is_database_url_failure(lowered: str) -> bool:
    """The message is about the PostgreSQL URL shape, not its secret value."""
    return "database" in lowered and ("valid" in lowered or "postgresql" in lowered)


def _print_validation_errors(exc: ValidationError) -> None:
    print("Configuration validation failed:", file=sys.stderr)
    for error in exc.errors():
        field = _format_error_field(error["loc"])
        msg = _sanitized_validation_message(str(error["msg"]))
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
    print_config_summary(settings)

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


class _RedactingLogFilter(logging.Filter):
    """Scrub rendered log records so tracebacks cannot re-emit URL secrets.

    ``logger.exception`` renders the exception chain and traceback from the raw
    exception object even when the message argument is pre-sanitized. The
    filter rewrites the record's message arguments and, when an exception is
    attached, replaces the exception with a sanitized clone carrying the same
    type and a redacted message, so formatters render only safe text.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        _redact_record_message(record)
        _redact_record_args(record)
        _redact_record_exc_info(record)
        return True


def _redact_record_message(record: logging.LogRecord) -> None:
    """Scrub the record's format string so static text cannot carry a URL."""
    record.msg = redact_text(str(record.msg))


def _redact_record_args(record: logging.LogRecord) -> None:
    """Scrub %-style arguments; non-string arguments pass through untouched."""
    if isinstance(record.args, tuple):
        record.args = tuple(map(_redacted_arg, record.args))
    elif isinstance(record.args, dict):
        record.args = {key: _redacted_arg(value) for key, value in record.args.items()}


def _redacted_arg(arg: object) -> object:
    if isinstance(arg, str):
        return redact_text(arg)
    return arg


def _redact_record_exc_info(record: logging.LogRecord) -> None:
    """Replace the attached exception with its sanitized clone and no frames."""
    if record.exc_info and record.exc_info[0] is not None:
        record.__dict__["exc_info"] = _sanitized_exc_info(record.exc_info)


def _sanitized_exc_info(
    exc_info: tuple[type[BaseException] | None, BaseException | None, object | None],
) -> tuple[type[BaseException] | None, BaseException | None, object | None]:
    """Clone the exception chain with redacted messages, preserving types."""
    exc_type, exc_value, _traceback = exc_info
    if exc_value is None:
        return (exc_type, exc_value, None)
    return (exc_type, _sanitized_exception(exc_value), None)


def _sanitized_exception(exc: BaseException) -> BaseException:
    """A same-type clone whose message and cause/context chain are redacted."""
    clone = _clone_with_redacted_message(exc)
    clone.__cause__ = _sanitized_cause(exc.__cause__)
    clone.__context__ = _sanitized_cause(exc.__context__)
    clone.__suppress_context__ = exc.__suppress_context__
    return clone


def _sanitized_cause(cause: BaseException | None) -> BaseException | None:
    if cause is None:
        return None
    return _sanitized_exception(cause)


def _clone_with_redacted_message(exc: BaseException) -> BaseException:
    """Clone ``exc`` with every positional message argument redacted as text."""
    redacted_args = tuple(_redacted_arg(arg) for arg in exc.args)
    try:
        clone = type(exc)(*redacted_args)
    except Exception:
        return _fallback_sanitized_error(exc)
    _carry_status(clone, exc)
    return clone


def _fallback_sanitized_error(exc: BaseException) -> BaseException:
    """A plain error carrying only redacted text when the type resists cloning."""
    clone = RuntimeError(redact_text(str(exc)))
    clone.__cause__ = exc
    return clone


def _carry_status(clone: BaseException, exc: BaseException) -> None:
    """Preserve the trades-source HTTP status on the sanitized clone."""
    if exc.__dict__.get("status") is not None:
        _set_cloned_status(clone, exc)


def _set_cloned_status(clone: BaseException, exc: BaseException) -> None:
    """Copy the status attribute without disturbing the clone's type."""
    with contextlib.suppress(AttributeError, TypeError):
        object.__setattr__(clone, "status", exc.__dict__.get("status"))


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
    logger.addFilter(_RedactingLogFilter())
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
        logger.exception("Pipeline failed: %s", redact_text(str(e)))
        return EXIT_ERROR


def apply_cli_overrides(settings: Settings, args: argparse.Namespace) -> None:
    """Fold parsed CLI overrides into the one effective configuration (FR-004).

    Runtime behavior and every summary path read the same settings afterwards, so
    an override can never apply to one and not the other. The health port is
    already applied during validation so its range check runs first.
    """
    if args.dry_run:
        settings.dry_run = True
    if args.log_level:
        settings.log_level = args.log_level


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

    apply_cli_overrides(settings, args)
    configure_logging(settings.log_level)

    # Print banner
    print_banner()

    # Config check mode
    if args.config_check:
        sys.exit(run_config_check(settings))

    # Print config summary
    print_config_summary(settings)

    # Run pipeline
    exit_code = asyncio.run(run_pipeline(settings, settings.dry_run))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
