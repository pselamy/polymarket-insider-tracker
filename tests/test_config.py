"""Tests for configuration management service."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from polymarket_insider_tracker.config import (
    DatabaseSettings,
    DiscordSettings,
    PolygonSettings,
    PolymarketSettings,
    RedisSettings,
    Settings,
    TelegramSettings,
    clear_settings_cache,
    get_settings,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def clear_cache() -> Iterator[None]:
    """Clear settings cache before and after each test."""
    clear_settings_cache()
    yield
    clear_settings_cache()


class TestDatabaseSettings:
    """Tests for DatabaseSettings."""

    def test_valid_postgresql_url(self) -> None:
        """Test valid PostgreSQL URL."""
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://user:pass@localhost/db"}):
            settings = DatabaseSettings()
            assert settings.url == "postgresql://user:pass@localhost/db"

    def test_valid_asyncpg_url(self) -> None:
        """Test valid asyncpg URL."""
        with patch.dict(
            os.environ, {"DATABASE_URL": "postgresql+asyncpg://user:pass@localhost/db"}
        ):
            settings = DatabaseSettings()
            assert settings.url == "postgresql+asyncpg://user:pass@localhost/db"

    def test_invalid_url_raises(self) -> None:
        """Test that invalid database URL raises validation error."""
        with (
            patch.dict(os.environ, {"DATABASE_URL": "mysql://user:pass@localhost/db"}),
            pytest.raises(ValidationError, match="PostgreSQL connection string"),
        ):
            DatabaseSettings()


class TestRedisSettings:
    """Tests for RedisSettings."""

    def test_default_url(self) -> None:
        """Test default Redis URL."""
        with patch.dict(os.environ, {}, clear=True):
            settings = RedisSettings()
            assert settings.url == "redis://localhost:6379"

    def test_custom_url(self) -> None:
        """Test custom Redis URL."""
        with patch.dict(os.environ, {"REDIS_URL": "redis://redis:6380"}):
            settings = RedisSettings()
            assert settings.url == "redis://redis:6380"

    def test_invalid_url_raises(self) -> None:
        """Test that invalid Redis URL raises validation error."""
        with (
            patch.dict(os.environ, {"REDIS_URL": "http://localhost:6379"}),
            pytest.raises(ValidationError, match="redis://"),
        ):
            RedisSettings()


class TestPolygonSettings:
    """Tests for PolygonSettings."""

    def test_default_rpc_url(self) -> None:
        """Test default Polygon RPC URL."""
        with patch.dict(os.environ, {}, clear=True):
            settings = PolygonSettings()
            assert settings.rpc_url == "https://polygon-rpc.com"
            assert settings.fallback_rpc_url is None

    def test_custom_urls(self) -> None:
        """Test custom Polygon RPC URLs."""
        with patch.dict(
            os.environ,
            {
                "POLYGON_RPC_URL": "https://alchemy.io/polygon",
                "POLYGON_FALLBACK_RPC_URL": "https://backup.polygon.io",
            },
        ):
            settings = PolygonSettings()
            assert settings.rpc_url == "https://alchemy.io/polygon"
            assert settings.fallback_rpc_url == "https://backup.polygon.io"

    def test_invalid_url_raises(self) -> None:
        """Test that invalid RPC URL raises validation error."""
        with (
            patch.dict(os.environ, {"POLYGON_RPC_URL": "ws://polygon.io"}),
            pytest.raises(ValidationError, match="HTTP"),
        ):
            PolygonSettings()


class TestPolymarketSettings:
    """Tests for PolymarketSettings."""

    def test_default_ws_url(self) -> None:
        """Test default Polymarket WebSocket URL."""
        with patch.dict(os.environ, {}, clear=True):
            settings = PolymarketSettings()
            assert "polymarket.com" in settings.ws_url
            assert settings.api_key is None

    def test_custom_api_key(self) -> None:
        """Test custom API key (secret)."""
        with patch.dict(os.environ, {"POLYMARKET_API_KEY": "secret-key-123"}):
            settings = PolymarketSettings()
            assert settings.api_key is not None
            assert settings.api_key.get_secret_value() == "secret-key-123"

    def test_invalid_ws_url_raises(self) -> None:
        """Test that invalid WebSocket URL raises validation error."""
        with (
            patch.dict(os.environ, {"POLYMARKET_WS_URL": "http://polymarket.com"}),
            pytest.raises(ValidationError, match="ws://"),
        ):
            PolymarketSettings()


class TestDiscordSettings:
    """Tests for DiscordSettings."""

    def test_disabled_by_default(self) -> None:
        """Test Discord is disabled when no webhook URL."""
        with patch.dict(os.environ, {}, clear=True):
            settings = DiscordSettings()
            assert not settings.enabled
            assert settings.webhook_url is None

    def test_enabled_with_webhook(self) -> None:
        """Test Discord is enabled with webhook URL."""
        with patch.dict(os.environ, {"DISCORD_WEBHOOK_URL": "https://discord.com/webhook/123"}):
            settings = DiscordSettings()
            assert settings.enabled
            assert settings.webhook_url is not None


class TestTelegramSettings:
    """Tests for TelegramSettings."""

    def test_disabled_by_default(self) -> None:
        """Test Telegram is disabled when no credentials."""
        with patch.dict(os.environ, {}, clear=True):
            settings = TelegramSettings()
            assert not settings.enabled

    def test_disabled_with_partial_config(self) -> None:
        """Test Telegram is disabled with only token or chat_id."""
        with patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "token123"}):
            settings = TelegramSettings()
            assert not settings.enabled

        with patch.dict(os.environ, {"TELEGRAM_CHAT_ID": "12345"}):
            settings = TelegramSettings()
            assert not settings.enabled

    def test_enabled_with_full_config(self) -> None:
        """Test Telegram is enabled with both token and chat_id."""
        with patch.dict(
            os.environ,
            {
                "TELEGRAM_BOT_TOKEN": "token123",
                "TELEGRAM_CHAT_ID": "12345",
            },
        ):
            settings = TelegramSettings()
            assert settings.enabled


class TestSettings:
    """Tests for main Settings class."""

    def test_loads_with_required_vars(self) -> None:
        """Test settings load with required environment variables."""
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "REDIS_URL": "redis://localhost:6379",
            },
        ):
            settings = Settings()
            assert settings.database.url == "postgresql://user:pass@localhost/db"
            assert settings.redis.url == "redis://localhost:6379"

    def test_default_log_level(self) -> None:
        """Test default log level is INFO."""
        with patch.dict(
            os.environ,
            {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
        ):
            settings = Settings()
            assert settings.log_level == "INFO"

    def test_custom_log_level(self) -> None:
        """Test custom log level."""
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "LOG_LEVEL": "DEBUG",
            },
        ):
            settings = Settings()
            assert settings.log_level == "DEBUG"

    def test_invalid_log_level_raises(self) -> None:
        """Test invalid log level raises validation error."""
        with (
            patch.dict(
                os.environ,
                {
                    "DATABASE_URL": "postgresql://user:pass@localhost/db",
                    "LOG_LEVEL": "TRACE",
                },
            ),
            pytest.raises(ValidationError),
        ):
            Settings()

    def test_health_port_validation(self) -> None:
        """Test health port must be valid port number."""
        with (
            patch.dict(
                os.environ,
                {
                    "DATABASE_URL": "postgresql://user:pass@localhost/db",
                    "HEALTH_PORT": "99999",
                },
            ),
            pytest.raises(ValidationError, match="65535"),
        ):
            Settings()

    def test_get_logging_level(self) -> None:
        """Test get_logging_level returns numeric level."""
        import logging

        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "LOG_LEVEL": "WARNING",
            },
        ):
            settings = Settings()
            assert settings.get_logging_level() == logging.WARNING

    def test_redacted_summary(self) -> None:
        """Test redacted_summary masks sensitive data."""
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:secretpass@localhost/db",
                "REDIS_URL": "redis://localhost:6379",
            },
        ):
            settings = Settings()
            summary = settings.redacted_summary()

            # Database password should be redacted
            db_url = summary["database_url"]
            assert isinstance(db_url, str)
            assert "secretpass" not in db_url
            assert "***" in db_url
            assert "user" in db_url


class TestGetSettings:
    """Tests for get_settings singleton."""

    def test_returns_same_instance(self) -> None:
        """Test get_settings returns cached instance."""
        with patch.dict(
            os.environ,
            {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
        ):
            settings1 = get_settings()
            settings2 = get_settings()
            assert settings1 is settings2

    def test_clear_cache_allows_reload(self) -> None:
        """Test clear_settings_cache allows reloading settings."""
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "LOG_LEVEL": "INFO",
            },
        ):
            settings1 = get_settings()
            assert settings1.log_level == "INFO"

        clear_settings_cache()

        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "LOG_LEVEL": "DEBUG",
            },
        ):
            settings2 = get_settings()
            assert settings2.log_level == "DEBUG"
            assert settings1 is not settings2
