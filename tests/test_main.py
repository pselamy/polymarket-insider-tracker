"""Tests for the CLI entry point."""

from __future__ import annotations

import asyncio

import pytest

import polymarket_insider_tracker.__main__ as cli
from polymarket_insider_tracker.__main__ import (
    EXIT_CONFIG_ERROR,
    EXIT_ERROR,
    EXIT_SUCCESS,
    configure_logging,
    create_parser,
    main,
    print_banner,
    run_config_check,
    validate_config,
)
from polymarket_insider_tracker.config import (
    Settings,
    WebSocketSettingDeprecationWarning,
    websocket_deprecation_message,
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

    def test_validation_stderr_never_echoes_a_credential_bearing_url(self, monkeypatch, capsys):
        """A malformed URL that carries a secret must be rejected without echoing it."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        monkeypatch.setenv("POLYGON_RPC_URL", "https://https://user:TOPSECRET@example.com/hook")

        settings = validate_config()
        assert settings is None

        captured = capsys.readouterr()
        assert "Configuration validation failed" in captured.err
        assert "TOPSECRET" not in captured.err
        assert "TOPSECRET" not in captured.out


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

    def test_config_check_prints_the_trades_source_and_legacy_disposition(
        self, monkeypatch, capsys
    ):
        """The check names the supported source settings and reports no WebSocket host."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        monkeypatch.delenv("POLYMARKET_WS_URL", raising=False)
        settings = validate_config()
        assert settings is not None

        run_config_check(settings)

        out = capsys.readouterr().out
        assert "Trades URL: https://data-api.polymarket.com/trades" in out
        assert "Trades Coverage: all" in out
        assert "Trades Poll Interval: 5s" in out
        assert "Trades Recovery Horizon: 600s" in out
        assert "WebSocket URL: (not set)" in out
        assert "deprecated" not in out.lower()

    def test_config_check_repeats_the_deprecation_warning_when_legacy_url_is_set(
        self, monkeypatch, capsys
    ):
        """A set POLYMARKET_WS_URL is reported as deprecated with the replacement variables."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        monkeypatch.setenv("POLYMARKET_WS_URL", "wss://legacy.invalid/ws")
        with pytest.warns(WebSocketSettingDeprecationWarning):
            settings = validate_config()
        assert settings is not None

        run_config_check(settings)

        out = capsys.readouterr().out
        assert "WebSocket URL: (deprecated, set)" in out
        assert websocket_deprecation_message() in out
        assert "legacy.invalid" not in out

    def test_config_check_states_offline_syntax_only(self, monkeypatch, capsys):
        """Config check must explicitly state it is an offline syntax check and not claim ready to run."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        settings = validate_config()
        assert settings is not None

        result = run_config_check(settings)
        assert result == EXIT_SUCCESS

        out = capsys.readouterr().out
        assert "Offline syntax and structure checks passed." in out
        assert (
            "Note: --config-check validates syntax and configuration only; runtime readiness" in out
        )
        assert "All checks passed. Ready to run." not in out


class TestMain:
    """Tests for main entry point."""

    def test_main_with_health_port_override(self, monkeypatch):
        """Main should pass overridden health port to settings and run_pipeline."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        runs: list[tuple[Settings, bool]] = []

        async def run_pipeline(settings: Settings, dry_run: bool) -> int:
            runs.append((settings, dry_run))
            return EXIT_SUCCESS

        monkeypatch.setattr(cli, "run_pipeline", run_pipeline)

        with pytest.raises(SystemExit) as exc_info:
            main(["--health-port", "9090", "--dry-run"])

        assert exc_info.value.code == EXIT_SUCCESS
        assert len(runs) == 1
        settings, dry_run = runs[0]
        assert settings.health_port == 9090
        assert dry_run is True

    def test_main_with_invalid_health_port(self, monkeypatch, capsys):
        """Main should reject out-of-range health port and exit with config error."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")

        with pytest.raises(SystemExit) as exc_info:
            main(["--health-port", "99999"])

        assert exc_info.value.code == EXIT_CONFIG_ERROR
        err = capsys.readouterr().err
        assert "health_port" in err

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

    def test_main_runs_pipeline(self, monkeypatch):
        """Main should run the pipeline with the validated settings and dry-run flag."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        runs: list[tuple[Settings, bool]] = []

        async def run_pipeline(settings: Settings, dry_run: bool) -> int:
            runs.append((settings, dry_run))
            return EXIT_SUCCESS

        monkeypatch.setattr(cli, "run_pipeline", run_pipeline)

        with pytest.raises(SystemExit) as exc_info:
            main(["--dry-run"])

        assert exc_info.value.code == EXIT_SUCCESS
        assert [(settings.database.url, dry_run) for settings, dry_run in runs] == [
            ("postgresql+psycopg://localhost/test", True)
        ]

    def test_main_exits_with_error_when_pipeline_encounters_worker_error(self, monkeypatch):
        """Main should exit with EXIT_ERROR (code 1) when run_pipeline reports worker error."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")

        async def run_pipeline(_settings: Settings, _dry_run: bool) -> int:
            return EXIT_ERROR

        monkeypatch.setattr(cli, "run_pipeline", run_pipeline)

        with pytest.raises(SystemExit) as exc_info:
            main([])

        assert exc_info.value.code == EXIT_ERROR

    def test_exit_code_for_pipeline_returns_error_on_pipeline_error_state(self):
        """_exit_code_for_pipeline returns EXIT_ERROR when pipeline state is ERROR."""
        from polymarket_insider_tracker.__main__ import _exit_code_for_pipeline
        from polymarket_insider_tracker.pipeline import Pipeline, PipelineState

        pipeline = Pipeline()
        pipeline._state = PipelineState.ERROR
        assert _exit_code_for_pipeline(pipeline) == EXIT_ERROR

    def test_exit_code_is_success_after_graceful_stop_with_recoverable_errors(self):
        """Recoverable processing errors must not turn a graceful shutdown into exit 1."""
        from polymarket_insider_tracker.__main__ import _exit_code_for_pipeline
        from polymarket_insider_tracker.pipeline import Pipeline, PipelineState

        pipeline = Pipeline()
        pipeline._state = PipelineState.STOPPED
        pipeline._stats.errors = 3
        pipeline._stats.last_error = "transient trade-processing error"
        assert _exit_code_for_pipeline(pipeline) == EXIT_SUCCESS


class HangingStopPipeline:
    """Working fake whose graceful stop never finishes on its own."""

    def __init__(self, _settings: Settings, *, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.state = None
        self._stop_event = asyncio.Event()
        self.stop_calls = 0

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop_event

    async def start(self) -> None:
        # Simulate an immediate internal stop request so run_pipeline reaches stop().
        self._stop_event.set()

    async def stop(self) -> None:
        self.stop_calls += 1
        await asyncio.sleep(3600)


class CountingStopPipeline(HangingStopPipeline):
    """Working fake whose stop succeeds instantly, for counting stop attempts."""

    async def stop(self) -> None:
        self.stop_calls += 1


class TestRunPipelineShutdownTimeout:
    """run_pipeline must enforce shutdown_timeout around pipeline.stop (US3 scenario 4)."""

    async def test_hanging_pipeline_stop_is_bounded_and_exits_error(self, monkeypatch):
        """A hung stop is one attempt under one deadline: exit 1 in ~timeout, not 2x."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        settings = validate_config()
        assert settings is not None

        from polymarket_insider_tracker.__main__ import run_pipeline

        created: list[HangingStopPipeline] = []

        def factory(settings: Settings, *, dry_run: bool = False) -> HangingStopPipeline:
            created.append(HangingStopPipeline(settings, dry_run=dry_run))
            return created[-1]

        start = asyncio.get_running_loop().time()
        exit_code = await asyncio.wait_for(
            run_pipeline(
                settings,
                dry_run=True,
                shutdown_timeout=0.2,
                pipeline_factory=factory,
            ),
            timeout=5.0,
        )
        elapsed = asyncio.get_running_loop().time() - start

        assert exit_code == EXIT_ERROR
        assert created[0].stop_calls == 1
        # One 0.2s deadline, not the doubled 0.4s the cleanup callback used to add.
        assert elapsed < 0.35

    async def test_successful_stop_is_not_repeated_by_cleanup(self, monkeypatch):
        """After the explicit stop finishes, shutdown cleanup must not stop again."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        settings = validate_config()
        assert settings is not None

        from polymarket_insider_tracker.__main__ import EXIT_SUCCESS, run_pipeline

        created: list[CountingStopPipeline] = []

        def factory(settings: Settings, *, dry_run: bool = False) -> CountingStopPipeline:
            created.append(CountingStopPipeline(settings, dry_run=dry_run))
            return created[-1]

        exit_code = await asyncio.wait_for(
            run_pipeline(
                settings,
                dry_run=True,
                shutdown_timeout=1.0,
                pipeline_factory=factory,
            ),
            timeout=5.0,
        )

        assert exit_code == EXIT_SUCCESS
        assert created[0].stop_calls == 1


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
