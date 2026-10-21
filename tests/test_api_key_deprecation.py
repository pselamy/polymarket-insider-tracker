"""POLYMARKET_API_KEY is accepted with a warning, ignored, and never logged or sent."""

from __future__ import annotations

import inspect
import logging
import warnings

import pytest

import polymarket_insider_tracker.ingestor.clob_client as clob_module
from polymarket_insider_tracker.__main__ import run_config_check, validate_config
from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.deprecated_api_key import (
    ApiKeySettingDeprecationWarning,
    api_key_deprecation_message,
)
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from tests.fakes import FakeBaseClobClient

SECRET = "synthetic-api-key-value"


def test_setting_the_key_warns_once_without_the_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLYMARKET_API_KEY", SECRET)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        settings = PolymarketSettings()

    messages = [str(w.message) for w in caught if w.category is ApiKeySettingDeprecationWarning]
    assert messages == [api_key_deprecation_message()]
    assert SECRET not in messages[0]
    assert settings.api_key is not None
    assert SECRET not in repr(settings)


def test_unset_key_does_not_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POLYMARKET_API_KEY", raising=False)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ApiKeySettingDeprecationWarning)
        assert PolymarketSettings().api_key is None


def test_empty_key_is_treated_as_unset(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
    monkeypatch.setenv("POLYMARKET_API_KEY", "")
    with warnings.catch_warnings():
        warnings.simplefilter("error", ApiKeySettingDeprecationWarning)
        settings = validate_config()
    assert settings is not None

    run_config_check(settings)

    assert api_key_deprecation_message() not in capsys.readouterr().out


def test_config_check_reports_the_key_as_ignored_without_printing_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
    monkeypatch.setenv("POLYMARKET_API_KEY", SECRET)
    with pytest.warns(ApiKeySettingDeprecationWarning):
        settings = validate_config()
    assert settings is not None

    run_config_check(settings)

    out = capsys.readouterr().out
    assert api_key_deprecation_message() in out
    assert SECRET not in out


def test_clob_client_takes_no_key_and_ignores_the_environment(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    hosts: list[str] = []

    def build(host: str) -> FakeBaseClobClient:
        hosts.append(host)
        return FakeBaseClobClient()

    monkeypatch.setattr(clob_module, "BaseClobClient", build)
    monkeypatch.setenv("POLYMARKET_API_KEY", SECRET)

    with caplog.at_level(logging.DEBUG):
        ClobClient()

    assert "api_key" not in inspect.signature(ClobClient).parameters
    assert hosts == ["https://clob.polymarket.com"]
    assert SECRET not in caplog.text
    assert "api_key" not in caplog.text
