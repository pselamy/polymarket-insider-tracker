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
from typing import NoReturn

from pydantic import ValidationError

from polymarket_insider_tracker import __version__
from polymarket_insider_tracker._cli_config_messages import (
    print_validation_errors as _print_validation_errors,
)
from polymarket_insider_tracker._cli_config_messages import (
    sanitized_validation_message as sanitized_validation_message,
)
from polymarket_insider_tracker._cli_pipeline import (
    EXIT_CONFIG_ERROR as EXIT_CONFIG_ERROR,
)
from polymarket_insider_tracker._cli_pipeline import (
    EXIT_ERROR as EXIT_ERROR,
)
from polymarket_insider_tracker._cli_pipeline import (
    EXIT_INTERRUPTED as EXIT_INTERRUPTED,
)
from polymarket_insider_tracker._cli_pipeline import (
    EXIT_SUCCESS as EXIT_SUCCESS,
)
from polymarket_insider_tracker._cli_pipeline import (
    exit_code_for_pipeline as exit_code_for_pipeline,
)
from polymarket_insider_tracker._cli_pipeline import (
    run_pipeline as run_pipeline,
)
from polymarket_insider_tracker._cli_redaction import (
    RedactingLogFilter as RedactingLogFilter,
)
from polymarket_insider_tracker._cli_redaction import (
    sanitized_exception as sanitized_exception,
)
from polymarket_insider_tracker.config import (
    Settings,
    clear_settings_cache,
    get_settings,
    websocket_deprecation_message,
)
from polymarket_insider_tracker.redaction import redact_url

__all__ = [
    "EXIT_CONFIG_ERROR",
    "EXIT_ERROR",
    "EXIT_INTERRUPTED",
    "EXIT_SUCCESS",
    "RedactingLogFilter",
    "run_pipeline",
    "sanitized_exception",
]

# Application info
APP_NAME = "Polymarket Insider Tracker"
APP_VERSION = __version__

# Historical private test seams; the helpers now live in the CLI helper modules.
_RedactingLogFilter = RedactingLogFilter
_sanitized_exception = sanitized_exception
_sanitized_validation_message = sanitized_validation_message
_exit_code_for_pipeline = exit_code_for_pipeline


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
        # The filter guards the handler, not a logger: logger-level filters do
        # not run for records propagated from other loggers, so only the
        # handler position covers pipeline, ingestor, alerter, and third-party
        # (httpx/websockets/web3) records with one shared boundary.
        "filters": {
            "redact": {"()": _RedactingLogFilter},
        },
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
                "filters": ["redact"],
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
