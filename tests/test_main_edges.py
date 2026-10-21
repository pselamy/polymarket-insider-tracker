"""CLI edges: diagnostics classes, notification summary, shutdown triggers, log-record shapes."""

from __future__ import annotations

import logging
import runpy
import signal
import sys
from collections.abc import Mapping

import pytest
from pydantic import AnyUrl, TypeAdapter, ValidationError

import polymarket_insider_tracker.__main__ as cli
from polymarket_insider_tracker.__main__ import (
    EXIT_SUCCESS,
    run_config_check,
    validate_config,
)
from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.redaction import MASK
from tests.console_capture import capture_console_output, production_console_handler

SECRET = "EDGESECRET_R7"


@pytest.fixture(autouse=True)
def database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
    monkeypatch.setenv("POLYMARKET_TRADES_URL", "https://trades.invalid/trades")


def _validation_stderr(capsys: pytest.CaptureFixture[str]) -> str:
    assert validate_config() is None
    captured = capsys.readouterr()
    assert SECRET not in captured.err + captured.out
    return captured.err


class TestValidationDiagnostics:
    def test_an_unsupported_database_driver_is_reported_as_a_postgresql_url_failure(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setenv("DATABASE_URL", f"mysql://user:{SECRET}@host/db")

        assert "DATABASE_URL: is not a valid PostgreSQL URL" in _validation_stderr(capsys)

    def test_a_non_http_endpoint_is_reported_as_an_http_endpoint_failure(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setenv("POLYGON_RPC_URL", f"ftp://user:{SECRET}@host/path")

        assert "POLYGON_RPC_URL: must be an HTTP(S) endpoint" in _validation_stderr(capsys)

    def test_a_generic_url_syntax_failure_is_reported_without_the_value(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(ValidationError) as rejected:
            TypeAdapter(AnyUrl).validate_python(f"not a url {SECRET}")

        def reject_settings() -> Settings:
            raise rejected.value

        monkeypatch.setattr(cli, "get_settings", reject_settings)

        assert "is not a valid URL" in _validation_stderr(capsys)


class TestConfigCheckNotifications:
    def test_configured_channels_are_reported_as_configured(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setenv("DISCORD_WEBHOOK_URL", f"https://discord.invalid/hooks/{SECRET}")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", SECRET)
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
        settings = validate_config()
        assert settings is not None

        assert run_config_check(settings) == EXIT_SUCCESS

        out = capsys.readouterr().out
        assert "  Discord: configured" in out
        assert "  Telegram: configured" in out
        assert SECRET not in out


class SignalOnlyPipeline(Pipeline):
    """A pipeline that never exposes a stop event; only a signal can end the run."""

    def __init__(self, settings: Settings, *, dry_run: bool = False) -> None:
        super().__init__(settings, dry_run=dry_run)
        self.stops = 0

    async def start(self) -> None:
        signal.raise_signal(signal.SIGTERM)

    async def stop(self) -> None:
        self.stops += 1


class InterruptedStartPipeline(Pipeline):
    """A pipeline whose start is interrupted from the keyboard."""

    async def start(self) -> None:
        raise KeyboardInterrupt


class TestRunPipelineTriggers:
    async def test_a_pipeline_without_a_stop_event_stops_on_a_shutdown_signal(self) -> None:
        settings = validate_config()
        assert settings is not None
        built: list[SignalOnlyPipeline] = []

        def factory(settings: Settings, *, dry_run: bool = False) -> SignalOnlyPipeline:
            built.append(SignalOnlyPipeline(settings, dry_run=dry_run))
            return built[0]

        code = await cli.run_pipeline(settings, True, 1.0, pipeline_factory=factory)

        assert code == EXIT_SUCCESS
        assert built[0].stop_event is None
        assert built[0].stops == 1

    async def test_a_keyboard_interrupt_exits_with_the_interrupted_code(self) -> None:
        settings = validate_config()
        assert settings is not None

        def factory(settings: Settings, *, dry_run: bool = False) -> InterruptedStartPipeline:
            return InterruptedStartPipeline(settings, dry_run=dry_run)

        code = await cli.run_pipeline(settings, True, 1.0, pipeline_factory=factory)

        assert code == cli.EXIT_INTERRUPTED


def _filtered_record(msg: object, args: Mapping[str, object]) -> logging.LogRecord:
    record = logging.LogRecord("edge", logging.INFO, __file__, 1, "", None, None)
    record.msg = msg
    record.args = args
    production_console_handler().filter(record)
    return record


class TestLogRecordShapes:
    def test_a_container_message_object_is_collapsed_to_the_placeholder(self) -> None:
        output = capture_console_output(lambda: logging.getLogger("edge").info({"k": SECRET}))

        assert SECRET not in output
        assert "***" in output

    def test_a_non_string_message_keeps_named_arguments_and_masks_their_containers(self) -> None:
        record = _filtered_record(42, {"names": [SECRET], "count": 3})

        assert record.msg == 42
        assert record.args == {"names": MASK, "count": 3}


class TestModuleEntryPoint:
    def test_running_the_package_as_a_script_dispatches_to_main(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(sys, "argv", ["polymarket-insider-tracker", "--version"])

        with pytest.raises(SystemExit) as exit_info:
            runpy.run_module("polymarket_insider_tracker.__main__", run_name="__main__")

        assert exit_info.value.code == 0
        assert "polymarket" in capsys.readouterr().out.lower()
