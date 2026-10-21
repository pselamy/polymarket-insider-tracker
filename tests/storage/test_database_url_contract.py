"""Exact refusal messages, port bounds and warning attribution of the DATABASE_URL contract."""

from __future__ import annotations

import sys
import warnings
from collections.abc import Callable
from types import FrameType

import pytest

from polymarket_insider_tracker.storage.database_url import (
    DatabaseUrlError,
    DatabaseUrlMigrationWarning,
    normalize_database_url,
    render_database_url_safe,
)

BASE = "postgresql+psycopg://user:pw@localhost"


def _refusal(source: str) -> str:
    with pytest.raises(DatabaseUrlError) as caught:
        normalize_database_url(source)
    return str(caught.value)


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (f"{BASE}:0/db", "DATABASE_URL port must be between 1 and 65535"),
        (f"{BASE}:65536/db", "DATABASE_URL port must be between 1 and 65535"),
        (
            "mysql://user:pw@localhost/db",
            "DATABASE_URL must use postgresql+psycopg, postgresql, or postgresql+asyncpg",
        ),
        ("postgresql+psycopg:///db", "DATABASE_URL must include a PostgreSQL host"),
        (f"{BASE}", "DATABASE_URL must include a database name"),
        (
            f"{BASE}/db name",
            "DATABASE_URL contains raw whitespace, control, or quote/angle characters",
        ),
        ("notaurl", "DATABASE_URL is not a valid PostgreSQL URL"),
        (
            "postgresql+asyncpg://user:pw@localhost/db?ssl=require",
            "DATABASE_URL query parameter 'ssl' is specific to asyncpg; "
            "remove it or replace it with a Psycopg-compatible option",
        ),
    ],
)
def test_each_refusal_has_its_exact_message(source: str, message: str) -> None:
    assert _refusal(source) == message


@pytest.mark.parametrize("port", [1, 5432, 65534, 65535])
def test_ports_inside_the_range_are_kept(port: int) -> None:
    assert normalize_database_url(f"{BASE}:{port}/db") == f"{BASE}:{port}/db"


def test_the_first_asyncpg_only_key_alphabetically_is_the_one_reported() -> None:
    source = "postgresql+asyncpg://user:pw@localhost/db?ssl=require&server_settings=x"

    assert "'server_settings'" in _refusal(source)


def test_asyncpg_only_keys_are_accepted_on_other_drivers() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = normalize_database_url("postgresql://user:pw@localhost/db?ssl=require")

    assert result == f"{BASE}/db?ssl=require"


def test_the_password_is_kept_verbatim_for_the_driver() -> None:
    assert normalize_database_url(f"{BASE}/db") == f"{BASE}/db"
    assert "pw" not in render_database_url_safe(f"{BASE}/db")


def test_the_modern_driver_spelling_does_not_warn() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        normalize_database_url(f"{BASE}/db")


@pytest.mark.parametrize("driver", ["postgresql", "postgresql+asyncpg"])
def test_a_legacy_driver_warns_with_the_exact_message(driver: str) -> None:
    with pytest.warns(DatabaseUrlMigrationWarning) as caught:
        normalize_database_url(f"{driver}://user:pw@localhost/db")

    assert [str(w.message) for w in caught] == [
        f"DATABASE_URL driver '{driver}' is deprecated; use 'postgresql+psycopg'"
    ]


def test_safe_rendering_is_the_central_redaction_of_exactly_the_given_url() -> None:
    rendered = render_database_url_safe(f"{BASE}:5432/research")

    assert rendered.startswith("postgresql+psycopg://user:")
    assert "@localhost:5432" in rendered
    assert "research" not in rendered
    assert "pw" not in rendered


def _body_callers(call: Callable[[], object]) -> list[tuple[str, int]]:
    """Where each call of ``normalize_database_url`` (or its mutation-harness body) came from."""
    callers: list[tuple[str, int]] = []

    def record(frame: FrameType, event: str, _arg: object) -> None:
        caller = frame.f_back
        if event == "call" and "normalize_database_url" in frame.f_code.co_name and caller:
            callers.append((caller.f_code.co_filename, caller.f_lineno))

    sys.setprofile(record)
    try:
        call()
    finally:
        sys.setprofile(None)
    return callers


def test_the_warning_is_attributed_to_the_caller_of_the_normalizer() -> None:
    """``stacklevel=2``: the warning points one frame above the function, not at the module."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        callers = _body_callers(lambda: normalize_database_url("postgresql://u:p@localhost/db"))

    assert [(w.filename, w.lineno) for w in caught] == [callers[-1]]
