#!/usr/bin/env python3
"""Verify local PostgreSQL, Redis, and disposable migration behavior."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress
import json
import os
import re
import socket
import subprocess
import sys
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Protocol
from urllib.parse import SplitResult, unquote, urlsplit

import psycopg
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg import sql
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url

from polymarket_insider_tracker.storage.database import create_async_db_engine
from polymarket_insider_tracker.storage.database_url import (
    DatabaseUrlError,
    normalize_database_url,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PHASES = ("probe", "migrations", "all")
REQUIRED_ENVIRONMENT: Mapping[str, tuple[str, ...]] = {
    "probe": ("DATABASE_URL", "REDIS_URL"),
    "migrations": ("DATABASE_URL",),
    "all": ("DATABASE_URL", "REDIS_URL"),
}
REDIS_SCHEME = "redis"
PREREQUISITE_EXIT_CODE = 2
VERIFICATION_FAILURE_EXIT_CODE = 1

# scheme://authority[path][?query][#fragment]; the authority ends at the first "/", "?", or "#".
_URL_PATTERN = re.compile(
    r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://(?P<authority>[^/?#]*)(?P<path>[^?#]*)(?P<tail>[?#].*)?$",
    re.DOTALL,
)


class ServicePrerequisiteError(RuntimeError):
    """Raised before a destructive service-verification operation begins."""


class ServiceVerificationError(RuntimeError):
    """Raised when a real service or migration verification step fails."""


class MigrationBackend(Protocol):
    """Operations needed to prove a disposable migration lifecycle."""

    def expected_revisions(self) -> tuple[str, str]: ...

    def create_database(self, name: str) -> None: ...

    def run_alembic(self, database_url: str, command: str, target: str) -> None: ...

    def current_revision(self, database_url: str) -> str: ...

    async def async_query(self, database_url: str) -> None: ...

    def drop_database(self, name: str) -> None: ...


ProbeRunner = Callable[[str, str], Awaitable[dict[str, object]]]
MigrationRunner = Callable[[str], Awaitable[dict[str, object]]]
BackendFactory = Callable[[str], MigrationBackend]


def _resolved_addresses(
    host: str, variable: str = "DATABASE_URL"
) -> set[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        records = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ServicePrerequisiteError(
            f"{variable} host could not be resolved for loopback verification"
        ) from exc
    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for record in records:
        try:
            addresses.add(ipaddress.ip_address(record[4][0]))
        except ValueError as exc:
            raise ServicePrerequisiteError(
                f"{variable} host returned an invalid address during loopback verification"
            ) from exc
    return addresses


def _require_loopback(host: str, variable: str) -> None:
    addresses = _resolved_addresses(host, variable)
    if not addresses or any(not address.is_loopback for address in addresses):
        raise ServicePrerequisiteError(
            f"{variable} must resolve only to loopback addresses for service verification"
        )


def validate_loopback_database_url(database_url: str) -> str:
    """Return a canonical URL only when every resolved host address is loopback."""
    if not database_url:
        raise ServicePrerequisiteError("DATABASE_URL is required")
    try:
        canonical = normalize_database_url(database_url)
    except DatabaseUrlError as exc:
        # The product error names the offending component or key without echoing the value.
        raise ServicePrerequisiteError(str(exc)) from exc
    host = make_url(canonical).host
    if host is None:
        raise ServicePrerequisiteError("DATABASE_URL must include a loopback host")
    _require_loopback(host, "DATABASE_URL")
    return canonical


def _validate_redis_port(parsed: SplitResult) -> None:
    try:
        port = parsed.port
    except ValueError as exc:
        raise ServicePrerequisiteError(
            "REDIS_URL port must be an integer between 1 and 65535"
        ) from exc
    if port is not None and not 1 <= port <= 65535:
        raise ServicePrerequisiteError("REDIS_URL port must be an integer between 1 and 65535")


def validate_redis_url(redis_url: str) -> str:
    """Return ``redis_url`` unchanged when it is usable, or fail without echoing any value."""
    if not redis_url:
        raise ServicePrerequisiteError("REDIS_URL is required")
    try:
        parsed = urlsplit(redis_url)
    except ValueError as exc:
        raise ServicePrerequisiteError("REDIS_URL could not be parsed as a URL") from exc
    if parsed.scheme != REDIS_SCHEME:
        raise ServicePrerequisiteError("REDIS_URL must use the redis:// scheme")
    if not parsed.hostname:
        raise ServicePrerequisiteError("REDIS_URL must include a host")
    _validate_redis_port(parsed)
    return redis_url


def validate_loopback_redis_url(redis_url: str) -> str:
    """Return ``redis_url`` only when it is usable and every resolved host address is loopback.

    Shared Redis contract tests write and delete keys, so they may only ever target a disposable
    local service; an arbitrary external ``REDIS_URL`` is rejected before any client exists.
    """
    validated = validate_redis_url(redis_url)
    host = urlsplit(validated).hostname
    assert host is not None
    _require_loopback(host, "REDIS_URL")
    return validated


def _psycopg_dsn(url: URL) -> str:
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def _split_userinfo(authority: str) -> tuple[str | None, str | None, str]:
    """Split an authority into (username, password, hostinfo) exactly like URL consumers do."""
    userinfo, separator, hostinfo = authority.rpartition("@")
    if not separator:
        return None, None, hostinfo
    username, separator, password = userinfo.partition(":")
    return username, (password if separator else None), hostinfo


def _extract_query_password(pair: str) -> str | None:
    name, separator, raw_value = pair.partition("=")
    if not separator or not raw_value:
        return None
    return raw_value if name.casefold() == "password" else None


def _query_credentials(tail: str) -> list[str]:
    if not tail.startswith("?"):
        return []
    query = tail[1:].partition("#")[0]
    results: list[str] = []
    for pair in query.split("&"):
        pwd = _extract_query_password(pair)
        if pwd:
            results.append(pwd)
    return results


def _url_credentials(value: str) -> tuple[str, ...]:
    """Return the raw credentials a consumer would read from ``value``.

    Both the userinfo password and any ``password`` query parameter count, because redis-py and
    libpq accept either spelling. Parsing never requires a well-formed URL.
    """
    match = _URL_PATTERN.match(value)
    if match is None:
        return ()
    credentials: list[str] = []
    password = _split_userinfo(match.group("authority"))[1]
    if password:
        credentials.append(password)
    credentials.extend(_query_credentials(match.group("tail") or ""))
    return tuple(credentials)


def _credential_forms(value: str) -> tuple[str, ...]:
    """Return every encoded or decoded credential spelling a tool might echo from ``value``."""
    raw_forms: list[str] = []
    for credential in _url_credentials(value):
        raw_forms.extend((credential, unquote(credential)))
    return tuple(dict.fromkeys(raw_forms))


def _redacted_url(value: str) -> str:
    """Render a URL with its credential and query hidden, or ``***`` when it is unparseable."""
    match = _URL_PATTERN.match(value)
    if match is None:
        return "***"
    username, password, hostinfo = _split_userinfo(match.group("authority"))
    if username is None:
        credential = ""
    elif password is None:
        credential = f"{username}@"
    else:
        credential = f"{username}:***@"
    query = "?***" if match.group("tail") else ""
    return f"{match.group('scheme')}://{credential}{hostinfo}{match.group('path')}{query}"


def _redact_text(value: str, *sensitive_urls: str) -> str:
    """Hide every given URL, and every spelling of its password, inside diagnostic text."""
    redacted = value
    for url in sensitive_urls:
        if not url:
            continue
        redacted = redacted.replace(url, _redacted_url(url))
        for form in _credential_forms(url):
            redacted = redacted.replace(form, "***")
    return redacted


class RealMigrationBackend:
    """PostgreSQL and Alembic implementation of the migration proof contract."""

    def __init__(self, base_url: str) -> None:
        self._base_url = make_url(base_url)
        self._admin_url = self._base_url.set(database="postgres")

    def expected_revisions(self) -> tuple[str, str]:
        config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
        script = ScriptDirectory.from_config(config)
        head = script.get_current_head()
        if head is None:
            raise ServiceVerificationError("Alembic has no current head revision")
        revision = script.get_revision(head)
        if revision is None or not isinstance(revision.down_revision, str):
            raise ServiceVerificationError("Alembic head has no single previous revision")
        return head, revision.down_revision

    def create_database(self, name: str) -> None:
        with psycopg.connect(_psycopg_dsn(self._admin_url), autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))

    def run_alembic(self, database_url: str, command: str, target: str) -> None:
        environment = os.environ.copy()
        environment["DATABASE_URL"] = database_url
        result = subprocess.run(
            [sys.executable, "-m", "alembic", command, target],
            cwd=REPOSITORY_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            output = _redact_text(result.stdout + result.stderr, database_url).strip()
            raise ServiceVerificationError(
                f"Alembic {command} {target} failed (exit {result.returncode}): {output}"
            )

    def current_revision(self, database_url: str) -> str:
        with psycopg.connect(_psycopg_dsn(make_url(database_url))) as connection:
            revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()
        if revision is None or not isinstance(revision[0], str):
            raise ServiceVerificationError("Alembic revision query returned no current revision")
        return revision[0]

    async def async_query(self, database_url: str) -> None:
        engine = create_async_db_engine(database_url)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(text("SELECT 1"))
                if result.scalar_one() != 1:
                    raise ServiceVerificationError(
                        "asynchronous PostgreSQL query returned wrong value"
                    )
        finally:
            await engine.dispose()

    def drop_database(self, name: str) -> None:
        with psycopg.connect(_psycopg_dsn(self._admin_url), autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


async def probe_services(database_url: str, redis_url: str) -> dict[str, object]:
    """Run a real async PostgreSQL query and Redis PING against local services.

    Both inputs are validated before any client exists. Every constructed client is registered
    for cleanup immediately, so a failure while building or using the second one still disposes
    the first, and every diagnostic is redacted before it leaves this function.
    """
    canonical = validate_loopback_database_url(database_url)
    validated_redis_url = validate_redis_url(redis_url)
    try:
        async with contextlib.AsyncExitStack() as cleanup:
            engine = create_async_db_engine(canonical)
            cleanup.push_async_callback(engine.dispose)
            redis = Redis.from_url(validated_redis_url)
            cleanup.push_async_callback(redis.aclose)
            async with engine.connect() as connection:
                result = await connection.execute(text("SELECT 1"))
                if result.scalar_one() != 1:
                    raise ServiceVerificationError("PostgreSQL probe returned an unexpected value")
            if await redis.ping() is not True:
                raise ServiceVerificationError("Redis PING did not return PONG")
    except ServiceVerificationError:
        raise
    except Exception as exc:
        message = _redact_text(str(exc), database_url, canonical, validated_redis_url)
        raise ServiceVerificationError(f"service probe failed: {message}") from exc
    return {"postgres_reachable": True, "redis_reachable": True}


def _temporary_database_url(base_url: str, database_name: str) -> str:
    return make_url(base_url).set(database=database_name).render_as_string(hide_password=False)


async def _execute_migration_sequence(
    backend: MigrationBackend, temporary_url: str, head: str, previous: str
) -> list[str]:
    states: list[str] = []
    backend.run_alembic(temporary_url, "upgrade", "head")
    states.append(backend.current_revision(temporary_url))
    if states[-1] != head:
        raise ServiceVerificationError("upgrade did not reach the Alembic head revision")

    backend.run_alembic(temporary_url, "downgrade", "-1")
    states.append(backend.current_revision(temporary_url))
    if states[-1] != previous:
        raise ServiceVerificationError("downgrade did not reach the previous Alembic revision")

    backend.run_alembic(temporary_url, "upgrade", "head")
    states.append(backend.current_revision(temporary_url))
    if states[-1] != head:
        raise ServiceVerificationError("re-upgrade did not return to the Alembic head revision")
    await backend.async_query(temporary_url)
    return states


def _safe_drop_database(backend: MigrationBackend, name: str) -> Exception | None:
    try:
        backend.drop_database(name)
        return None
    except Exception as exc:
        return exc


def _raise_if_migration_errors(
    cleanup_error: Exception | None,
    primary_error: Exception | None,
    sensitive_urls: tuple[str, ...],
) -> None:
    if cleanup_error is not None:
        message = _redact_text(str(cleanup_error), *sensitive_urls)
        raise ServiceVerificationError(
            f"temporary database cleanup failed: {message}"
        ) from cleanup_error
    if primary_error is not None:
        message = _redact_text(str(primary_error), *sensitive_urls)
        raise ServiceVerificationError(
            f"migration verification failed: {message}"
        ) from primary_error


async def run_migration_cycle(
    database_url: str,
    *,
    backend_factory: BackendFactory = RealMigrationBackend,
) -> dict[str, object]:
    """Prove upgrade/downgrade/re-upgrade in an always-cleaned sibling database."""
    canonical = validate_loopback_database_url(database_url)
    database_name = f"pit_verify_{uuid.uuid4().hex}"
    temporary_url = _temporary_database_url(canonical, database_name)
    backend = backend_factory(canonical)
    created = False
    primary_error: Exception | None = None
    cleanup_error: Exception | None = None
    migration_states: list[str] = []

    try:
        head, previous = backend.expected_revisions()
        backend.create_database(database_name)
        created = True
        migration_states = await _execute_migration_sequence(backend, temporary_url, head, previous)
    except Exception as exc:
        primary_error = exc
    finally:
        if created:
            cleanup_error = _safe_drop_database(backend, database_name)

    _raise_if_migration_errors(
        cleanup_error, primary_error, (database_url, canonical, temporary_url)
    )

    return {
        "temporary_database": database_name,
        "migration_states": migration_states,
        "async_query_succeeded": True,
        "cleanup_succeeded": True,
    }


async def execute_phase(
    phase: str,
    database_url: str,
    redis_url: str = "",
    *,
    probe_runner: ProbeRunner | None = None,
    migration_runner: MigrationRunner | None = None,
) -> dict[str, object]:
    """Execute one explicit service-verification phase and return merged evidence.

    ``redis_url`` is only consulted by the ``probe`` and ``all`` phases; ``migrations`` proves the
    disposable database cycle from ``database_url`` alone.
    """
    if phase not in PHASES:
        raise ServicePrerequisiteError(f"unknown phase {phase!r}; choose probe, migrations, or all")
    run_probe = probe_runner or probe_services
    run_migrations = migration_runner or run_migration_cycle
    evidence: dict[str, object] = {}
    if phase in {"probe", "all"}:
        evidence.update(await run_probe(database_url, redis_url))
    if phase in {"migrations", "all"}:
        evidence.update(await run_migrations(database_url))
    return evidence


def missing_environment(phase: str, environment: Mapping[str, str]) -> tuple[str, ...]:
    """Return the variables ``phase`` needs that are absent or empty in ``environment``."""
    return tuple(name for name in REQUIRED_ENVIRONMENT[phase] if not environment.get(name))


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=PHASES, default="all")
    parser.add_argument("--json", action="store_true", help="emit one JSON result")
    return parser.parse_args(argv)


def _emit_result(phase: str, status: str, evidence: dict[str, object], as_json: bool) -> None:
    result = {"phase": phase, "status": status, **evidence}
    if as_json:
        print(json.dumps(result, sort_keys=True))
        return
    print(f"[PASS] runtime-{phase}")
    for key, value in evidence.items():
        print(f"  {key}: {value}")


def _emit_failure(phase: str, status: str, message: str, as_json: bool) -> None:
    if as_json:
        print(json.dumps({"phase": phase, "status": status, "error": message}))
        return
    print(f"[FAIL] runtime-{phase}: {message}", file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the selected phase and map failures to the documented exit semantics."""
    args = _parse_args(argv)
    missing = missing_environment(args.phase, os.environ)
    if missing:
        message = f"{' and '.join(missing)} must be set for --phase {args.phase}"
        _emit_failure(args.phase, "error", message, args.json)
        return PREREQUISITE_EXIT_CODE
    database_url = os.environ["DATABASE_URL"]
    redis_url = os.environ.get("REDIS_URL", "")
    try:
        evidence = asyncio.run(execute_phase(args.phase, database_url, redis_url))
    except ServicePrerequisiteError as exc:
        _emit_failure(args.phase, "error", str(exc), args.json)
        return PREREQUISITE_EXIT_CODE
    except ServiceVerificationError as exc:
        _emit_failure(args.phase, "failed", str(exc), args.json)
        return VERIFICATION_FAILURE_EXIT_CODE
    except Exception as exc:
        # An unexpected error must still fail closed without a traceback that echoes the URLs.
        message = _redact_text(f"unexpected {type(exc).__name__}: {exc}", database_url, redis_url)
        _emit_failure(args.phase, "failed", message, args.json)
        return VERIFICATION_FAILURE_EXIT_CODE
    _emit_result(args.phase, "passed", evidence, args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
