"""Tests for the CLI entry point."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from polymarket_insider_tracker.__main__ import (
    EXIT_CONFIG_ERROR,
    EXIT_SUCCESS,
    configure_logging,
    create_parser,
    main,
    print_banner,
    run_config_check,
    validate_config,
)


class TestCreateParser:
    """Tests for argument parser creation."""

    def test_parser_has_version(self):
        """Parser should have version flag."""
        parser = create_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--version"])
        assert exc_info.value.code == 0

    def test_parser_config_check(self):
        """Parser should accept --config-check flag."""
        parser = create_parser()
        args = parser.parse_args(["--config-check"])
        assert args.config_check is True

    def test_parser_log_level(self):
        """Parser should accept --log-level option."""
        parser = create_parser()
        args = parser.parse_args(["--log-level", "DEBUG"])
        assert args.log_level == "DEBUG"

    def test_parser_dry_run(self):
        """Parser should accept --dry-run flag."""
        parser = create_parser()
        args = parser.parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_parser_health_port(self):
        """Parser should accept --health-port option."""
        parser = create_parser()
        args = parser.parse_args(["--health-port", "9090"])
        assert args.health_port == 9090

    def test_parser_default_values(self):
        """Parser should have correct defaults."""
        parser = create_parser()
        args = parser.parse_args([])
        assert args.config_check is False
        assert args.log_level is None
        assert args.dry_run is False
        assert args.health_port is None


class TestConfigureLogging:
    """Tests for logging configuration."""

    def test_configure_logging_info(self):
        """Should configure logging at INFO level."""
        configure_logging("INFO")
        import logging

        assert logging.getLogger().level == logging.INFO

    def test_configure_logging_debug(self):
        """Should configure logging at DEBUG level."""
        configure_logging("DEBUG")
        import logging

        assert logging.getLogger().level == logging.DEBUG


class TestPrintBanner:
    """Tests for banner printing."""

    def test_banner_contains_app_name(self, capsys):
        """Banner should contain application name."""
        print_banner()
        captured = capsys.readouterr()
        assert "Polymarket Insider Tracker" in captured.out

    def test_banner_contains_version(self, capsys):
        """Banner should contain version."""
        print_banner()
        captured = capsys.readouterr()
        assert "v0.1.0" in captured.out


class TestValidateConfig:
    """Tests for configuration validation."""

    def test_validate_config_success(self, monkeypatch):
        """Should return settings on valid config."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")

        settings = validate_config()
        assert settings is not None

    def test_validate_config_failure(self, monkeypatch, capsys):
        """Should return None on invalid config."""
        # Clear any existing DATABASE_URL
        monkeypatch.delenv("DATABASE_URL", raising=False)

        settings = validate_config()
        assert settings is None

        captured = capsys.readouterr()
        assert "Configuration validation failed" in captured.err


class TestRunConfigCheck:
    """Tests for config check mode."""

    def test_config_check_prints_summary(self, monkeypatch, capsys):
        """Config check should print configuration summary."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")

        settings = validate_config()
        assert settings is not None

        result = run_config_check(settings)
        assert result == EXIT_SUCCESS

        captured = capsys.readouterr()
        assert "Configuration is valid!" in captured.out
        assert "Configuration:" in captured.out


class TestMain:
    """Tests for main entry point."""

    def test_main_with_config_check(self, monkeypatch):
        """Main should exit successfully with --config-check."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-check"])

        assert exc_info.value.code == EXIT_SUCCESS

    def test_main_with_invalid_config(self, monkeypatch):
        """Main should exit with config error on invalid config."""
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(SystemExit) as exc_info:
            main([])

        assert exc_info.value.code == EXIT_CONFIG_ERROR

    def test_main_with_dry_run_and_config_check(self, monkeypatch):
        """Main should handle dry-run with config-check."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-check", "--dry-run"])

        assert exc_info.value.code == EXIT_SUCCESS

    @patch("polymarket_insider_tracker.__main__.run_pipeline")
    @patch("polymarket_insider_tracker.__main__.asyncio.run")
    def test_main_runs_pipeline(self, mock_asyncio_run, _mock_run_pipeline, monkeypatch):
        """Main should run pipeline when not in config-check mode."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        mock_asyncio_run.return_value = EXIT_SUCCESS

        with pytest.raises(SystemExit) as exc_info:
            main([])

        assert exc_info.value.code == EXIT_SUCCESS
        mock_asyncio_run.assert_called_once()


class TestIntegration:
    """Integration tests for CLI invocation."""

    def test_cli_help_option(self, capsys):
        """CLI should display help with -h option."""
        with pytest.raises(SystemExit) as exc_info:
            main(["-h"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "polymarket-insider-tracker" in captured.out
        assert "--config-check" in captured.out
        assert "--dry-run" in captured.out
        assert "--log-level" in captured.out

    def test_cli_version_option(self, capsys):
        """CLI should display version with --version option."""
        with pytest.raises(SystemExit) as exc_info:
            main(["--version"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "0.1.0" in captured.out

    def test_cli_invalid_log_level(self, capsys):
        """CLI should reject invalid log level."""
        with pytest.raises(SystemExit) as exc_info:
            main(["--log-level", "INVALID"])

        assert exc_info.value.code != 0
        captured = capsys.readouterr()
        assert "invalid choice" in captured.err
