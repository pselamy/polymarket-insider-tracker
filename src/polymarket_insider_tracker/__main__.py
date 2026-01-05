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
from polymarket_insider_tracker.config import Settings, clear_settings_cache, get_settings
from polymarket_insider_tracker.pipeline import Pipeline
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
    print(f"  Log Level: {summary['log_level']}")
    print(f"  Health Port: {summary['health_port']}")
    print(f"  Dry Run: {dry_run}")
    print(f"  Discord: {'enabled' if summary['discord_enabled'] == 'True' else 'disabled'}")
    print(f"  Telegram: {'enabled' if summary['telegram_enabled'] == 'True' else 'disabled'}")
    print()


def validate_config() -> Settings | None:
    """Validate and load configuration.

    Returns:
        Settings instance if valid, None if invalid.
    """
    try:
        # Clear cache to force reload
        clear_settings_cache()
        return get_settings()
    except ValidationError as e:
        print("Configuration validation failed:", file=sys.stderr)
        for error in e.errors():
            field = ".".join(str(loc) for loc in error["loc"])
            msg = error["msg"]
            print(f"  {field}: {msg}", file=sys.stderr)
        return None


def run_config_check(settings: Settings) -> int:
    """Run configuration check and exit.

    Args:
        settings: Validated settings.

    Returns:
        Exit code (0 for success).
    """
    print("Configuration is valid!")
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

    print()
    print("All checks passed. Ready to run.")
    return EXIT_SUCCESS


async def run_pipeline(
    settings: Settings,
    dry_run: bool,
    shutdown_timeout: float = 30.0,
) -> int:
    """Run the main pipeline with graceful shutdown handling.

    Args:
        settings: Application settings.
        dry_run: Whether to skip sending alerts.
        shutdown_timeout: Maximum time to wait for graceful shutdown.

    Returns:
        Exit code.
    """
    logger = logging.getLogger(__name__)
    shutdown = GracefulShutdown(timeout=shutdown_timeout)

    try:
        async with shutdown:
            pipeline = Pipeline(settings, dry_run=dry_run)

            # Register pipeline cleanup
            shutdown.register_cleanup(pipeline.stop)

            logger.info("Starting pipeline...")
            await pipeline.start()

            logger.info("Pipeline running. Press Ctrl+C to stop.")

            # Wait for shutdown signal
            await shutdown.wait()

            logger.info("Shutdown signal received, stopping pipeline...")
            await pipeline.stop()

        return EXIT_SUCCESS
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

    # Validate configuration first
    settings = validate_config()
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
