"""Tests for canonical PostgreSQL URL handling."""

from __future__ import annotations

import importlib.util
import warnings
from pathlib import Path
from types import ModuleType

import pytest

MODULE_PATH = (
    Path(__file__).parents[2] / "src" / "polymarket_insider_tracker" / "storage" / "database_url.py"
)


def _load_module() -> ModuleType:
    assert MODULE_PATH.exists(), "database URL normalization module is not implemented"
    spec = importlib.util.spec_from_file_location("database_url_under_test", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "postgresql+psycopg://user:pass@localhost:5432/tracker",
            "postgresql+psycopg://user:pass@localhost:5432/tracker",
        ),
        (
            "postgresql://user:pass@localhost:5432/tracker",
            "postgresql+psycopg://user:pass@localhost:5432/tracker",
        ),
        (
            "postgresql+asyncpg://user:pass@localhost:5432/tracker",
            "postgresql+psycopg://user:pass@localhost:5432/tracker",
        ),
    ],
)
def test_normalizes_supported_driver_spellings(source: str, expected: str) -> None:
    module = _load_module()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = module.normalize_database_url(source)

    assert result == expected
    assert len(caught) == (0 if "+psycopg" in source else 1)
    if caught:
        assert source.split(":", 1)[0] in str(caught[0].message)
        assert "postgresql+psycopg" in str(caught[0].message)
        assert "pass" not in str(caught[0].message)


def test_preserves_portable_query_and_encoded_credentials() -> None:
    module = _load_module()
    source = (
        "postgresql://user:p%40ss@localhost:5432/tracker"
        "?application_name=insider-tracker&sslmode=require"
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = module.normalize_database_url(source)

    assert result.startswith("postgresql+psycopg://user:p%40ss@localhost:5432/tracker?")
    assert "application_name=insider-tracker" in result
    assert "sslmode=require" in result


@pytest.mark.parametrize(
    "query_key",
    ["prepared_statement_cache_size", "prepared_statement_name_func", "server_settings", "ssl"],
)
def test_rejects_asyncpg_specific_query_options_without_leaking_secret(query_key: str) -> None:
    module = _load_module()
    secret = "do-not-print-this"
    source = f"postgresql+asyncpg://user:{secret}@localhost/tracker?{query_key}=value"

    with pytest.raises(module.DatabaseUrlError) as exc_info:
        module.normalize_database_url(source)

    message = str(exc_info.value)
    assert query_key in message
    assert "Psycopg" in message
    assert secret not in message


@pytest.mark.parametrize(
    "source",
    [
        "mysql://user:pass@localhost/tracker",
        "postgresql+psycopg://localhost",
        "postgresql+psycopg:///tracker",
        "postgresql+psycopg://localhost:99999/tracker",
        "not a URL",
    ],
)
def test_rejects_malformed_or_incomplete_urls(source: str) -> None:
    module = _load_module()

    with pytest.raises(module.DatabaseUrlError, match="DATABASE_URL"):
        module.normalize_database_url(source)


def test_safe_rendering_hides_password_and_preserves_location() -> None:
    module = _load_module()
    secret = "never-render-this"
    source = f"postgresql+psycopg://tracker:{secret}@localhost:5432/research"

    rendered = module.render_database_url_safe(source)

    assert secret not in rendered
    assert "***" in rendered
    assert "tracker" in rendered
    assert "localhost:5432/research" in rendered
