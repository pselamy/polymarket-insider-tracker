"""Canonical, credential-safe PostgreSQL URL handling."""

from __future__ import annotations

import warnings

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

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


def _parse_database_url(value: str) -> URL:
    try:
        url = make_url(value)
        port = url.port
    except (ArgumentError, TypeError, ValueError) as exc:
        raise DatabaseUrlError("DATABASE_URL is not a valid PostgreSQL URL") from exc

    if url.drivername not in SUPPORTED_DRIVERS:
        raise DatabaseUrlError(
            "DATABASE_URL must use postgresql+psycopg, postgresql, or postgresql+asyncpg"
        )
    if not url.host:
        raise DatabaseUrlError("DATABASE_URL must include a PostgreSQL host")
    if not url.database:
        raise DatabaseUrlError("DATABASE_URL must include a database name")
    if port is not None and not 1 <= port <= 65535:
        raise DatabaseUrlError("DATABASE_URL port must be between 1 and 65535")
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
    """Render a URL for diagnostics without exposing its password."""
    try:
        return make_url(value).render_as_string(hide_password=True)
    except (ArgumentError, TypeError, ValueError):
        return "<invalid DATABASE_URL>"
