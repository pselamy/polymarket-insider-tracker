"""Canonical, credential-safe PostgreSQL URL handling."""

from __future__ import annotations

import warnings

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

from polymarket_insider_tracker.redaction import is_scannable_url_text, redact_url

CANONICAL_DRIVER = "postgresql+psycopg"
LEGACY_DRIVERS = frozenset({"postgresql", "postgresql+asyncpg"})
SUPPORTED_DRIVERS = LEGACY_DRIVERS | {CANONICAL_DRIVER}
ASYNCPG_ONLY_QUERY_KEYS = frozenset(
    {
        "prepared_statement_cache_size",
        "prepared_statement_name_func",
        "server_settings",
        "ssl",
    }
)


class DatabaseUrlError(ValueError):
    """Raised when DATABASE_URL cannot satisfy the supported runtime contract."""


class DatabaseUrlMigrationWarning(UserWarning):
    """Warn that a legacy PostgreSQL driver spelling was normalized."""


def _validate_port(port: int | None) -> None:
    if port is not None and not 1 <= port <= 65535:
        raise DatabaseUrlError("DATABASE_URL port must be between 1 and 65535")


def _validate_url_fields(url: URL) -> None:
    if url.drivername not in SUPPORTED_DRIVERS:
        raise DatabaseUrlError(
            "DATABASE_URL must use postgresql+psycopg, postgresql, or postgresql+asyncpg"
        )
    if not url.host:
        raise DatabaseUrlError("DATABASE_URL must include a PostgreSQL host")
    if not url.database:
        raise DatabaseUrlError("DATABASE_URL must include a database name")


def _check_scannable(value: str) -> None:
    """Reject raw characters the text-redaction scanner treats as URL boundaries.

    Inside diagnostic text the central policy can only mask this URL while it
    stays one scan match; a raw whitespace, control, double-quote, or angle
    character would end the match early and let the tail — possibly a
    credential — escape as prose. No such character is valid raw URI text;
    percent-encoded forms remain accepted. The message never echoes the value.
    """
    if not is_scannable_url_text(value):
        raise DatabaseUrlError(
            "DATABASE_URL contains raw whitespace, control, or quote/angle characters"
        )


def _parse_database_url(value: str) -> URL:
    _check_scannable(value)
    try:
        url = make_url(value)
        port = url.port
    except (ArgumentError, TypeError, ValueError) as exc:
        raise DatabaseUrlError("DATABASE_URL is not a valid PostgreSQL URL") from exc

    _validate_url_fields(url)
    _validate_port(port)
    return url


def normalize_database_url(value: str) -> str:
    """Return the supported Psycopg URL while preserving portable URL components."""
    url = _parse_database_url(value)
    original_driver = url.drivername

    if original_driver == "postgresql+asyncpg":
        incompatible_keys = sorted(ASYNCPG_ONLY_QUERY_KEYS.intersection(url.query))
        if incompatible_keys:
            key = incompatible_keys[0]
            raise DatabaseUrlError(
                f"DATABASE_URL query parameter {key!r} is specific to asyncpg; "
                "remove it or replace it with a Psycopg-compatible option"
            )

    if original_driver in LEGACY_DRIVERS:
        warnings.warn(
            f"DATABASE_URL driver {original_driver!r} is deprecated; use {CANONICAL_DRIVER!r}",
            DatabaseUrlMigrationWarning,
            stacklevel=2,
        )

    return url.set(drivername=CANONICAL_DRIVER).render_as_string(hide_password=False)


def render_database_url_safe(value: str) -> str:
    """Render a URL for diagnostics under the central fail-closed redaction policy.

    SQLAlchemy's ``hide_password`` rendering keeps the database path and query
    values readable; the central policy masks the password, the path, and
    every query value because a query value or path segment may itself carry a
    credential, while the host and port stay diagnosable.
    """
    return redact_url(value)
