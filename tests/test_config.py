"""Tests for configuration management service."""

from __future__ import annotations

import contextlib
import os
import warnings
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from polymarket_insider_tracker.config import (
    DatabaseSettings,
    DetectorSettings,
    DiscordSettings,
    PolygonSettings,
    PolymarketSettings,
    RedisSettings,
    Settings,
    TelegramSettings,
    TradesCoverage,
    WebSocketSettingDeprecationWarning,
    clear_settings_cache,
    get_settings,
    websocket_deprecation_message,
)
from polymarket_insider_tracker.storage.database_url import DatabaseUrlMigrationWarning

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic_settings import BaseSettings


def _clear_environment(mp: pytest.MonkeyPatch) -> None:
    for k in list(os.environ.keys()):
        mp.delenv(k, raising=False)


def _apply_environment(mp: pytest.MonkeyPatch, env: Mapping[str, str] | None) -> None:
    if env:
        for k, v in env.items():
            mp.setenv(k, v)


@contextlib.contextmanager
def env_context(env: Mapping[str, str] | None = None, *, clear: bool = False) -> Iterator[None]:
    """Isolated environment context using pytest monkeypatch."""
    with pytest.MonkeyPatch.context() as mp:
        if clear:
            _clear_environment(mp)
        _apply_environment(mp, env)
        yield


# Every settings group and the environment-variable prefix it documents.
SETTINGS_GROUPS: tuple[tuple[type[BaseSettings], str], ...] = (
    (DatabaseSettings, ""),
    (RedisSettings, ""),
    (PolygonSettings, "POLYGON_"),
    (PolymarketSettings, "POLYMARKET_"),
    (DiscordSettings, "DISCORD_"),
    (TelegramSettings, "TELEGRAM_"),
    (DetectorSettings, "DETECTOR_"),
    (Settings, ""),
)


@pytest.fixture(autouse=True)
def clear_cache() -> Iterator[Callable[[], None]]:
    """Reset the settings singleton around every test and expose the reset for explicit use."""
    clear_settings_cache()
    yield clear_settings_cache
    clear_settings_cache()


class TestSharedDotenvContract:
    """Every settings group must read the one documented `.env` file the same way."""

    @pytest.mark.parametrize(("settings_class", "env_prefix"), SETTINGS_GROUPS)
    def test_group_loads_the_shared_dotenv_and_ignores_unrelated_keys(
        self, settings_class: type[BaseSettings], env_prefix: str
    ) -> None:
        """Pydantic consumes `model_config` by name; pin the loading policy it reads."""
        loading_policy = settings_class.model_config

        assert loading_policy["env_file"] == ".env"
        assert loading_policy["env_file_encoding"] == "utf-8"
        assert loading_policy["extra"] == "ignore"
        assert loading_policy["env_prefix"] == env_prefix

    def test_database_settings_hide_credential_bearing_input_in_errors(self) -> None:
        """A rejected DATABASE_URL must not be echoed back with its credential."""
        assert DatabaseSettings.model_config["hide_input_in_errors"] is True

    def test_one_dotenv_serves_every_group(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Keys owned by other groups in the shared `.env` must be ignored, not rejected."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".env").write_text(
            "DATABASE_URL=postgresql+psycopg://tracker:secret@localhost:5432/research\n"
            "REDIS_URL=redis://localhost:6379\n"
            "POLYGON_RPC_URL=https://polygon.invalid\n"
            "POLYMARKET_WS_URL=wss://polymarket.invalid/ws\n"
            "DISCORD_WEBHOOK_URL=https://discord.invalid/webhook\n"
            "TELEGRAM_BOT_TOKEN=token\n"
            "DETECTOR_ALERT_THRESHOLD=0.9\n"
            "LOG_LEVEL=DEBUG\n",
            encoding="utf-8",
        )
        scrubbed_environment = {
            key: value
            for key, value in os.environ.items()
            if key not in {"DATABASE_URL", "REDIS_URL", "LOG_LEVEL"}
            and not key.startswith(
                ("POLYGON_", "POLYMARKET_", "DISCORD_", "TELEGRAM_", "DETECTOR_")
            )
        }
        with env_context(scrubbed_environment, clear=True):
            loaded = [settings_class() for settings_class, _ in SETTINGS_GROUPS]

        assert len(loaded) == len(SETTINGS_GROUPS)


class TestDatabaseSettings:
    """Tests for DatabaseSettings."""

    def test_valid_postgresql_url(self) -> None:
        """Test valid PostgreSQL URL."""
        with (
            env_context({"DATABASE_URL": "postgresql://user:pass@localhost/db"}),
            pytest.warns(DatabaseUrlMigrationWarning, match="postgresql.*psycopg"),
        ):
            settings = DatabaseSettings()
        assert settings.url == "postgresql+psycopg://user:pass@localhost/db"

    def test_valid_asyncpg_url(self) -> None:
        """Test valid asyncpg URL."""
        with (
            env_context({"DATABASE_URL": "postgresql+asyncpg://user:pass@localhost/db"}),
            pytest.warns(DatabaseUrlMigrationWarning, match="asyncpg.*psycopg"),
        ):
            settings = DatabaseSettings()
        assert settings.url == "postgresql+psycopg://user:pass@localhost/db"

    def test_canonical_url_does_not_warn(self) -> None:
        """Test that the canonical URL is accepted without a migration warning."""
        with (
            env_context({"DATABASE_URL": "postgresql+psycopg://user:pass@localhost/db"}),
            warnings.catch_warnings(record=True) as caught,
        ):
            warnings.simplefilter("always")
            settings = DatabaseSettings()

        assert settings.url == "postgresql+psycopg://user:pass@localhost/db"
        assert caught == []

    def test_invalid_url_raises(self) -> None:
        """Test that invalid database URL raises validation error."""
        with (
            env_context({"DATABASE_URL": "mysql://user:pass@localhost/db"}),
            pytest.raises(ValidationError, match="DATABASE_URL"),
        ):
            DatabaseSettings()

    def test_incompatible_legacy_query_is_redacted(self) -> None:
        """Test that driver migration failures never echo credentials."""
        secret = "never-print-this-password"
        with (
            env_context(
                {
                    "DATABASE_URL": (
                        f"postgresql+asyncpg://user:{secret}@localhost/db"
                        "?prepared_statement_cache_size=0"
                    )
                },
            ),
            pytest.raises(ValidationError) as exc_info,
        ):
            DatabaseSettings()

        message = str(exc_info.value)
        assert "prepared_statement_cache_size" in message
        assert secret not in message


class TestRedisSettings:
    """Tests for RedisSettings."""

    def test_default_url(self) -> None:
        """Test default Redis URL."""
        with env_context({}, clear=True):
            settings = RedisSettings()
            assert settings.url == "redis://localhost:6379"

    def test_custom_url(self) -> None:
        """Test custom Redis URL."""
        with env_context({"REDIS_URL": "redis://redis:6380"}):
            settings = RedisSettings()
            assert settings.url == "redis://redis:6380"

    def test_invalid_url_raises(self) -> None:
        """Test that invalid Redis URL raises validation error."""
        with (
            env_context({"REDIS_URL": "http://localhost:6379"}),
            pytest.raises(ValidationError, match="redis://"),
        ):
            RedisSettings()


class TestPolygonSettings:
    """Tests for PolygonSettings."""

    def test_default_rpc_url(self) -> None:
        """Test default Polygon RPC URL."""
        with env_context({}, clear=True):
            settings = PolygonSettings()
            assert settings.rpc_url == "https://polygon-rpc.com"
            assert settings.fallback_rpc_url is None

    def test_custom_urls(self) -> None:
        """Test custom Polygon RPC URLs."""
        with env_context(
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
            env_context({"POLYGON_RPC_URL": "ws://polygon.io"}),
            pytest.raises(ValidationError, match="HTTP"),
        ):
            PolygonSettings()

    @pytest.mark.parametrize(
        "bad_url",
        [
            "https://wss://polygon.io",
            "https://http://polygon.io",
            "http://",
            "https://",
            "https:///path",
        ],
    )
    def test_malformed_http_url_diagnostics(self, bad_url: str) -> None:
        """Malformed or multi-protocol URLs must be rejected with actionable diagnostics."""
        with (
            env_context({"POLYGON_RPC_URL": bad_url}),
            pytest.raises(ValidationError),
        ):
            PolygonSettings()


TRADES_REPLACEMENT_VARIABLES = (
    "POLYMARKET_TRADES_URL",
    "POLYMARKET_TRADES_COVERAGE",
    "POLYMARKET_TRADES_POLL_INTERVAL_SECONDS",
    "POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS",
)


class TestPolymarketSettings:
    """Tests for PolymarketSettings."""

    def test_defaults_need_no_websocket_host(self) -> None:
        """The supported source is the documented trades query; nothing WebSocket is required."""
        with env_context({}, clear=True), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            settings = PolymarketSettings()

        assert settings.trades_url == "https://data-api.polymarket.com/trades"
        assert settings.trades_coverage is TradesCoverage.ALL
        assert settings.trades_poll_interval_seconds == 5
        assert settings.trades_recovery_horizon_seconds == 600
        assert settings.ws_url is None
        assert settings.api_key is None
        assert caught == []

    def test_trades_settings_load_from_the_environment(self) -> None:
        with env_context(
            {
                "POLYMARKET_TRADES_URL": "https://trades.invalid/trades",
                "POLYMARKET_TRADES_COVERAGE": "taker-only",
                "POLYMARKET_TRADES_POLL_INTERVAL_SECONDS": "1",
                "POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS": "3600",
            },
            clear=True,
        ):
            settings = PolymarketSettings()

        assert settings.trades_url == "https://trades.invalid/trades"
        assert settings.trades_coverage is TradesCoverage.TAKER_ONLY
        assert settings.trades_poll_interval_seconds == 1
        assert settings.trades_recovery_horizon_seconds == 3600

    @pytest.mark.parametrize(
        ("variable", "value"),
        [
            ("POLYMARKET_TRADES_POLL_INTERVAL_SECONDS", "60"),
            ("POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS", "60"),
        ],
    )
    def test_trades_range_bounds_are_inclusive(self, variable: str, value: str) -> None:
        with env_context({variable: value}, clear=True):
            settings = PolymarketSettings()

        assert value in {
            str(settings.trades_poll_interval_seconds),
            str(settings.trades_recovery_horizon_seconds),
        }

    @pytest.mark.parametrize(
        ("variable", "value", "match"),
        [
            ("POLYMARKET_TRADES_POLL_INTERVAL_SECONDS", "0", "greater than or equal to 1"),
            ("POLYMARKET_TRADES_POLL_INTERVAL_SECONDS", "61", "less than or equal to 60"),
            ("POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS", "59", "greater than or equal to 60"),
            ("POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS", "3601", "less than or equal to 3600"),
            ("POLYMARKET_TRADES_COVERAGE", "makers", "'all' or 'taker-only'"),
            ("POLYMARKET_TRADES_URL", "wss://trades.invalid/trades", "HTTP"),
        ],
    )
    def test_invalid_trades_settings_are_rejected_actionably(
        self, variable: str, value: str, match: str
    ) -> None:
        with (
            env_context({variable: value}, clear=True),
            pytest.raises(ValidationError, match=match),
        ):
            PolymarketSettings()

    def test_legacy_websocket_url_warns_once_and_is_never_reinterpreted(self) -> None:
        with (
            env_context({"POLYMARKET_WS_URL": "wss://legacy.invalid/ws"}, clear=True),
            warnings.catch_warnings(record=True) as caught,
        ):
            warnings.simplefilter("always")
            settings = PolymarketSettings()

        assert settings.ws_url == "wss://legacy.invalid/ws"
        assert settings.trades_url == "https://data-api.polymarket.com/trades"
        deprecations = [w for w in caught if w.category is WebSocketSettingDeprecationWarning]
        assert len(deprecations) == 1
        message = str(deprecations[0].message)
        assert message == websocket_deprecation_message()
        assert "POLYMARKET_WS_URL" in message
        assert "not used" in message
        assert all(variable in message for variable in TRADES_REPLACEMENT_VARIABLES)
        assert "legacy.invalid" not in message
        assert issubclass(WebSocketSettingDeprecationWarning, UserWarning)

    def test_custom_api_key(self) -> None:
        """Test custom API key (secret)."""
        with env_context({"POLYMARKET_API_KEY": "secret-key-123"}):
            settings = PolymarketSettings()
            assert settings.api_key is not None
            assert settings.api_key.get_secret_value() == "secret-key-123"

    def test_invalid_ws_url_raises(self) -> None:
        """Test that invalid WebSocket URL raises validation error."""
        with (
            env_context({"POLYMARKET_WS_URL": "http://polymarket.com"}),
            pytest.raises(ValidationError, match="ws://"),
        ):
            PolymarketSettings()

    @pytest.mark.parametrize(
        "bad_ws_url",
        [
            "wss://https://legacy.invalid/ws",
            "ws://wss://legacy.invalid/ws",
            "ws://",
            "wss://",
        ],
    )
    def test_malformed_ws_url_raises(self, bad_ws_url: str) -> None:
        """Test that malformed WebSocket URL raises validation error."""
        with (
            env_context({"POLYMARKET_WS_URL": bad_ws_url}),
            pytest.raises(ValidationError),
        ):
            PolymarketSettings()


class TestDiscordSettings:
    """Tests for DiscordSettings."""

    def test_disabled_by_default(self) -> None:
        """Test Discord is disabled when no webhook URL."""
        with env_context({}, clear=True):
            settings = DiscordSettings()
            assert not settings.enabled
            assert settings.webhook_url is None

    def test_enabled_with_webhook(self) -> None:
        """Test Discord is enabled with webhook URL."""
        with env_context({"DISCORD_WEBHOOK_URL": "https://discord.com/webhook/123"}):
            settings = DiscordSettings()
            assert settings.enabled
            assert settings.webhook_url is not None


class TestTelegramSettings:
    """Tests for TelegramSettings."""

    def test_disabled_by_default(self) -> None:
        """Test Telegram is disabled when no credentials."""
        with env_context({}, clear=True):
            settings = TelegramSettings()
            assert not settings.enabled

    def test_disabled_with_partial_config(self) -> None:
        """Test Telegram is disabled with only token or chat_id."""
        with env_context({"TELEGRAM_BOT_TOKEN": "token123"}):
            settings = TelegramSettings()
            assert not settings.enabled

        with env_context({"TELEGRAM_CHAT_ID": "12345"}):
            settings = TelegramSettings()
            assert not settings.enabled

    def test_enabled_with_full_config(self) -> None:
        """Test Telegram is enabled with both token and chat_id."""
        with env_context(
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
        with env_context(
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "REDIS_URL": "redis://localhost:6379",
            },
        ):
            settings = Settings()
            assert settings.database.url == "postgresql+psycopg://user:pass@localhost/db"
            assert settings.redis.url == "redis://localhost:6379"

    def test_default_log_level(self) -> None:
        """Test default log level is INFO."""
        with env_context(
            {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
        ):
            settings = Settings()
            assert settings.log_level == "INFO"

    def test_custom_log_level(self) -> None:
        """Test custom log level."""
        with env_context(
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
            env_context(
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
            env_context(
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

        with env_context(
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "LOG_LEVEL": "WARNING",
            },
        ):
            settings = Settings()
            assert settings.get_logging_level() == logging.WARNING

    def test_redacted_summary(self) -> None:
        """Test redacted_summary masks sensitive data."""
        with env_context(
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

    def test_redacted_summary_describes_the_trades_source(self) -> None:
        """The polymarket block names the supported source and the legacy disposition."""
        with env_context(
            {"DATABASE_URL": "postgresql+psycopg://user:pass@localhost/db"}, clear=True
        ):
            summary = Settings().redacted_summary()["polymarket"]

        assert summary == {
            "trades_url": "https://data-api.polymarket.com/trades",
            "coverage": "all",
            "poll_interval_seconds": "5",
            "recovery_horizon_seconds": "600",
            "ws_url": "(not set)",
            "api_key": "(not set)",
        }

    def test_redacted_summary_marks_a_set_legacy_url_without_its_value(self) -> None:
        with (
            env_context(
                {
                    "DATABASE_URL": "postgresql+psycopg://user:pass@localhost/db",
                    "POLYMARKET_WS_URL": "wss://legacy.invalid/ws",
                    "POLYMARKET_API_KEY": "secret",
                },
                clear=True,
            ),
            pytest.warns(WebSocketSettingDeprecationWarning),
        ):
            summary = Settings().redacted_summary()["polymarket"]

        assert isinstance(summary, dict)
        assert summary["ws_url"] == "(deprecated, set)"
        assert summary["api_key"] == "(set)"
        assert "legacy.invalid" not in str(summary)
        assert "secret" not in str(summary)


class TestGetSettings:
    """Tests for get_settings singleton."""

    def test_returns_same_instance(self) -> None:
        """Test get_settings returns cached instance."""
        with env_context(
            {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
        ):
            settings1 = get_settings()
            settings2 = get_settings()
            assert settings1 is settings2

    def test_clear_cache_allows_reload(self, clear_cache: Callable[[], None]) -> None:
        """Test clear_settings_cache allows reloading settings."""
        with env_context(
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "LOG_LEVEL": "INFO",
            },
        ):
            settings1 = get_settings()
            assert settings1.log_level == "INFO"

        clear_cache()

        with env_context(
            {
                "DATABASE_URL": "postgresql://user:pass@localhost/db",
                "LOG_LEVEL": "DEBUG",
            },
        ):
            settings2 = get_settings()
            assert settings2.log_level == "DEBUG"
            assert settings1 is not settings2
