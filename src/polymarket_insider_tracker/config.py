"""Configuration management service with Pydantic Settings.

This module provides centralized configuration management for the
Polymarket Insider Tracker application, loading and validating
environment variables at startup.
"""

from __future__ import annotations

import logging
import warnings
from collections.abc import Callable
from enum import StrEnum
from functools import lru_cache
from typing import Annotated, Literal, cast

from pydantic import AfterValidator, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from polymarket_insider_tracker.storage.database_url import normalize_database_url


def _validate_redis_url(value: str) -> str:
    """Validate Redis URL format."""
    if not value.startswith("redis://"):
        raise ValueError("REDIS_URL must start with redis://")
    return value


def _validate_http_url(value: str) -> str:
    """Validate RPC URL format."""
    if not value.startswith(("http://", "https://")):
        raise ValueError("RPC URL must be an HTTP(S) endpoint")
    return value


def _validate_websocket_url(value: str) -> str:
    """Validate WebSocket URL format."""
    if not value.startswith(("ws://", "wss://")):
        raise ValueError("WebSocket URL must start with ws:// or wss://")
    return value


# Field-level validation is attached through annotated types so each rule is an ordinary function
# whose use is visible to readers and static tooling alike.
CanonicalDatabaseUrl = Annotated[str, AfterValidator(normalize_database_url)]
RedisUrl = Annotated[str, AfterValidator(_validate_redis_url)]
HttpEndpointUrl = Annotated[str, AfterValidator(_validate_http_url)]
WebSocketEndpointUrl = Annotated[str, AfterValidator(_validate_websocket_url)]


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    url: CanonicalDatabaseUrl = Field(
        alias="DATABASE_URL",
        description="PostgreSQL connection string",
    )


class RedisSettings(BaseSettings):
    """Redis connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    url: RedisUrl = Field(
        default="redis://localhost:6379",
        alias="REDIS_URL",
        description="Redis connection string",
    )


class PolygonSettings(BaseSettings):
    """Polygon blockchain RPC settings."""

    model_config = SettingsConfigDict(
        env_prefix="POLYGON_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    rpc_url: HttpEndpointUrl = Field(
        default="https://polygon-rpc.com",
        alias="POLYGON_RPC_URL",
        description="Primary Polygon RPC endpoint",
    )
    fallback_rpc_url: HttpEndpointUrl | None = Field(
        default=None,
        alias="POLYGON_FALLBACK_RPC_URL",
        description="Fallback Polygon RPC endpoint",
    )


class TradesCoverage(StrEnum):
    """Which public participant observations the trades query returns."""

    ALL = "all"
    TAKER_ONLY = "taker-only"


class WebSocketSettingDeprecationWarning(UserWarning):
    """``POLYMARKET_WS_URL`` is deprecated and never used for acquisition."""


TRADES_REPLACEMENT_VARIABLES = (
    "POLYMARKET_TRADES_URL",
    "POLYMARKET_TRADES_COVERAGE",
    "POLYMARKET_TRADES_POLL_INTERVAL_SECONDS",
    "POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS",
)


def websocket_deprecation_message() -> str:
    """The actionable, value-free text emitted when the legacy WebSocket setting is set."""
    return (
        "POLYMARKET_WS_URL is deprecated and not used for trade acquisition; the tracker polls "
        "the documented public trades query. Remove it and configure "
        + ", ".join(TRADES_REPLACEMENT_VARIABLES)
        + " instead."
    )


def _warn_about_deprecated_websocket_url(value: str) -> str:
    """Accept a legacy WebSocket URL with the deprecation warning; the value is never printed."""
    warnings.warn(websocket_deprecation_message(), WebSocketSettingDeprecationWarning, stacklevel=2)
    return value


DeprecatedWebSocketUrl = Annotated[
    WebSocketEndpointUrl, AfterValidator(_warn_about_deprecated_websocket_url)
]


class PolymarketSettings(BaseSettings):
    """Polymarket public data source settings."""

    model_config = SettingsConfigDict(
        env_prefix="POLYMARKET_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    trades_url: HttpEndpointUrl = Field(
        default="https://data-api.polymarket.com/trades",
        alias="POLYMARKET_TRADES_URL",
        description="Documented anonymous public trades query",
    )
    trades_coverage: TradesCoverage = Field(
        default=TradesCoverage.ALL,
        alias="POLYMARKET_TRADES_COVERAGE",
        description="all participant observations (default) or the provider's taker-only subset",
    )
    trades_poll_interval_seconds: int = Field(
        default=5,
        alias="POLYMARKET_TRADES_POLL_INTERVAL_SECONDS",
        description="Seconds between acquisition cycles; 1 keeps the budget at 5% of the limit",
        ge=1,
        le=60,
    )
    trades_recovery_horizon_seconds: int = Field(
        default=600,
        alias="POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS",
        description="Identity retention and loss-detection horizon; not a completeness guarantee",
        ge=60,
        le=3600,
    )
    ws_url: DeprecatedWebSocketUrl | None = Field(
        default=None,
        alias="POLYMARKET_WS_URL",
        description="Deprecated legacy WebSocket URL; accepted with a warning and never used",
    )
    api_key: SecretStr | None = Field(
        default=None,
        alias="POLYMARKET_API_KEY",
        description="Optional Polymarket API key",
    )

    @property
    def ws_url_disposition(self) -> str:
        """Render the legacy setting's state without its value."""
        return "(deprecated, set)" if self.ws_url is not None else "(not set)"


class DiscordSettings(BaseSettings):
    """Discord notification settings."""

    model_config = SettingsConfigDict(
        env_prefix="DISCORD_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    webhook_url: SecretStr | None = Field(
        default=None,
        alias="DISCORD_WEBHOOK_URL",
        description="Discord webhook URL for alerts",
    )

    @property
    def enabled(self) -> bool:
        """Check if Discord notifications are enabled."""
        return self.webhook_url is not None and bool(self.webhook_url.get_secret_value().strip())


class TelegramSettings(BaseSettings):
    """Telegram notification settings."""

    model_config = SettingsConfigDict(
        env_prefix="TELEGRAM_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    bot_token: SecretStr | None = Field(
        default=None,
        alias="TELEGRAM_BOT_TOKEN",
        description="Telegram bot token",
    )
    chat_id: str | None = Field(
        default=None,
        alias="TELEGRAM_CHAT_ID",
        description="Telegram chat ID for alerts",
    )

    @property
    def enabled(self) -> bool:
        """Check if Telegram notifications are enabled."""
        return (
            self.bot_token is not None
            and bool(self.bot_token.get_secret_value().strip())
            and self.chat_id is not None
            and bool(self.chat_id.strip())
        )


class DetectorSettings(BaseSettings):
    """Risk-scorer / detector tuning."""

    model_config = SettingsConfigDict(
        env_prefix="DETECTOR_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    alert_threshold: float = Field(
        default=0.80,
        alias="DETECTOR_ALERT_THRESHOLD",
        description="Minimum weighted score required to trigger an alert",
        ge=0.0,
        le=1.0,
    )
    dedup_window_seconds: int = Field(
        default=3600,
        alias="DETECTOR_DEDUP_WINDOW_SECONDS",
        description="Per-(wallet, market) dedup window in seconds",
        ge=0,
    )
    persist_assessments: bool = Field(
        default=True,
        alias="DETECTOR_PERSIST_ASSESSMENTS",
        description="Write every signal-bearing risk assessment to the database",
    )


class Settings(BaseSettings):
    """Main application settings.

    Loads configuration from environment variables with support for
    .env files via python-dotenv.

    Example:
        ```python
        from polymarket_insider_tracker.config import get_settings

        settings = get_settings()
        print(settings.database.url)
        print(settings.log_level)
        ```
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Nested configuration groups
    # Pydantic's static constructor signature cannot express that BaseSettings supplies the required
    # database URL from DATABASE_URL when the zero-argument default factory runs.
    database: DatabaseSettings = Field(
        default_factory=cast(Callable[[], DatabaseSettings], DatabaseSettings)
    )
    redis: RedisSettings = Field(default_factory=RedisSettings)
    polygon: PolygonSettings = Field(default_factory=PolygonSettings)
    polymarket: PolymarketSettings = Field(default_factory=PolymarketSettings)
    discord: DiscordSettings = Field(default_factory=DiscordSettings)
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    detector: DetectorSettings = Field(default_factory=DetectorSettings)

    # Application settings
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        alias="LOG_LEVEL",
        description="Logging level",
    )
    health_port: int = Field(
        default=8080,
        alias="HEALTH_PORT",
        description="HTTP port for health check endpoints",
        ge=1,
        le=65535,
    )
    dry_run: bool = Field(
        default=False,
        alias="DRY_RUN",
        description="Run without sending actual alerts",
    )

    def get_logging_level(self) -> int:
        """Get the numeric logging level."""
        level: int = getattr(logging, self.log_level)
        return level

    def redacted_summary(self) -> dict[str, str | dict[str, str]]:
        """Get a summary of settings with secrets redacted.

        Returns:
            Dictionary of settings with sensitive values masked.
        """
        return {
            "database_url": self._redact_url(self.database.url),
            "redis_url": self._redact_url(self.redis.url),
            "polygon": {
                "rpc_url": self.polygon.rpc_url,
                "fallback_rpc_url": self.polygon.fallback_rpc_url or "(not set)",
            },
            "polymarket": {
                "trades_url": self.polymarket.trades_url,
                "coverage": self.polymarket.trades_coverage.value,
                "poll_interval_seconds": str(self.polymarket.trades_poll_interval_seconds),
                "recovery_horizon_seconds": str(self.polymarket.trades_recovery_horizon_seconds),
                "ws_url": self.polymarket.ws_url_disposition,
                "api_key": "(set)" if self.polymarket.api_key else "(not set)",
            },
            "discord_enabled": str(self.discord.enabled),
            "telegram_enabled": str(self.telegram.enabled),
            "log_level": self.log_level,
            "health_port": str(self.health_port),
            "dry_run": str(self.dry_run),
        }

    @staticmethod
    def _redact_url(url: str) -> str:
        """Redact password from URL if present."""
        if "@" in url and "://" in url:
            # URL has credentials - redact the password
            protocol_end = url.index("://") + 3
            at_pos = url.index("@")
            creds_part = url[protocol_end:at_pos]
            if ":" in creds_part:
                username = creds_part.split(":")[0]
                return f"{url[:protocol_end]}{username}:***@{url[at_pos + 1 :]}"
        return url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get the application settings singleton.

    Uses LRU cache to ensure settings are loaded only once and
    reused across the application.

    Returns:
        The Settings instance.

    Raises:
        ValidationError: If required environment variables are missing
            or have invalid values.
    """
    return Settings()


def clear_settings_cache() -> None:
    """Clear the settings cache.

    Useful for testing when you need to reload settings with
    different environment variables.
    """
    get_settings.cache_clear()
