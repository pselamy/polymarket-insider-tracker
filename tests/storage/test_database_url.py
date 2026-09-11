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


def test_safe_rendering_masks_credentials_and_path_under_central_policy() -> None:
    """Round-18: diagnostics rendering follows the central fail-closed policy.

    SQLAlchemy's ``hide_password`` rendering kept the username, database path,
    and query values readable; the central policy masks the password, the
    path, and every query value while the host and port stay diagnosable.
    """
    module = _load_module()
    secret = "never-render-this"
    query_secret = "never-render-this-query"
    source = (
        f"postgresql+psycopg://tracker:{secret}@localhost:5432/research"
        f"?sslpassword={query_secret}"
    )

    rendered = module.render_database_url_safe(source)

    assert secret not in rendered
    assert query_secret not in rendered
    assert "research" not in rendered
    assert "***" in rendered
    assert "tracker" in rendered
    assert "localhost:5432" in rendered


def test_safe_rendering_fails_closed_on_unparseable_input() -> None:
    module = _load_module()

    rendered = module.render_database_url_safe("postgresql://user:pw@[::1")

    assert "user:pw" not in rendered
    assert "***" in rendered
