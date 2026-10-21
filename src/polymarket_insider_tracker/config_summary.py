"""Console summary of the effective configuration printed at startup and by --config-check."""

from __future__ import annotations

from polymarket_insider_tracker.config import Settings, websocket_deprecation_message
from polymarket_insider_tracker.detector.alert_path import alert_path_report
from polymarket_insider_tracker.redaction import redact_url


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
    print_alerting_summary(settings, summary)
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


def print_alerting_summary(settings: Settings, summary: dict[str, str | dict[str, str]]) -> None:
    """Print how alerts would be delivered: dry run, channels, and the default alert path (T029)."""
    print(f"  Dry Run: {settings.dry_run}")
    print(f"  Discord: {'enabled' if summary['discord_enabled'] == 'True' else 'disabled'}")
    print(f"  Telegram: {'enabled' if summary['telegram_enabled'] == 'True' else 'disabled'}")
    print(f"  {alert_path_report(settings.detector.alert_threshold).console_line()}")
