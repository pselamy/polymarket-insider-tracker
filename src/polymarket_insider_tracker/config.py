"""Configuration management service with Pydantic Settings.

This module provides centralized configuration management for the
Polymarket Insider Tracker application, loading and validating
environment variables at startup.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(env_prefix="")

    url: str = Field(
        alias="DATABASE_URL",
        description="PostgreSQL connection string",
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate database URL format."""
        if not v.startswith(("postgresql://", "postgresql+asyncpg://")):
            raise ValueError("DATABASE_URL must be a PostgreSQL connection string")
        return v


class RedisSettings(BaseSettings):
    """Redis connection settings."""

    model_config = SettingsConfigDict(env_prefix="")

    url: str = Field(
        default="redis://localhost:6379",
        alias="REDIS_URL",
        description="Redis connection string",
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate Redis URL format."""
        if not v.startswith("redis://"):
            raise ValueError("REDIS_URL must start with redis://")
        return v


class PolygonSettings(BaseSettings):
    """Polygon blockchain RPC settings."""

    model_config = SettingsConfigDict(env_prefix="POLYGON_")

    rpc_url: str = Field(
        default="https://polygon-rpc.com",
        alias="POLYGON_RPC_URL",
        description="Primary Polygon RPC endpoint",
    )
    fallback_rpc_url: str | None = Field(
        default=None,
        alias="POLYGON_FALLBACK_RPC_URL",
        description="Fallback Polygon RPC endpoint",
    )

    @field_validator("rpc_url", "fallback_rpc_url")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        """Validate RPC URL format."""
        if v is None:
            return v
        if not v.startswith(("http://", "https://")):
            raise ValueError("RPC URL must be an HTTP(S) endpoint")
        return v


class PolymarketSettings(BaseSettings):
    """Polymarket API settings."""

    model_config = SettingsConfigDict(env_prefix="POLYMARKET_")

    ws_url: str = Field(
        default="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        alias="POLYMARKET_WS_URL",
        description="Polymarket WebSocket URL for live data",
    )
    api_key: SecretStr | None = Field(
        default=None,
        alias="POLYMARKET_API_KEY",
        description="Optional Polymarket API key",
    )

    @field_validator("ws_url")
    @classmethod
    def validate_ws_url(cls, v: str) -> str:
        """Validate WebSocket URL format."""
        if not v.startswith(("ws://", "wss://")):
            raise ValueError("WebSocket URL must start with ws:// or wss://")
        return v


class DiscordSettings(BaseSettings):
    """Discord notification settings."""

    model_config = SettingsConfigDict(env_prefix="DISCORD_")

    webhook_url: SecretStr | None = Field(
        default=None,
        alias="DISCORD_WEBHOOK_URL",
        description="Discord webhook URL for alerts",
    )

    @property
    def enabled(self) -> bool:
        """Check if Discord notifications are enabled."""
        return self.webhook_url is not None


class TelegramSettings(BaseSettings):
    """Telegram notification settings."""

    model_config = SettingsConfigDict(env_prefix="TELEGRAM_")

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
        return self.bot_token is not None and self.chat_id is not None


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
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    polygon: PolygonSettings = Field(default_factory=PolygonSettings)
    polymarket: PolymarketSettings = Field(default_factory=PolymarketSettings)
    discord: DiscordSettings = Field(default_factory=DiscordSettings)
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)

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
                "ws_url": self.polymarket.ws_url,
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
