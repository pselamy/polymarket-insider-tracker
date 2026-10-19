#!/usr/bin/env python3
"""Verify local PostgreSQL, Redis, and disposable migration behavior."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress as ipaddress
import json
import os
import re as re
import runpy
import socket as socket
import subprocess
import sys
import uuid
from collections.abc import Awaitable as Awaitable
from collections.abc import Callable as Callable
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol as Protocol
from urllib.parse import SplitResult as SplitResult
from urllib.parse import unquote as unquote
from urllib.parse import urlsplit as urlsplit

import psycopg
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg import sql
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url

from polymarket_insider_tracker.storage.database import create_async_db_engine
from polymarket_insider_tracker.storage.database_url import (
    DatabaseUrlError as DatabaseUrlError,
)
from polymarket_insider_tracker.storage.database_url import (
    normalize_database_url as normalize_database_url,
)

# Bind extracted helpers to this entry point, even under cached foreign packages or aliases.
_sibling_imports = runpy.run_path(str(Path(__file__).with_name("_entry_bootstrap.py")))[
    "sibling_imports"
]
with _sibling_imports(globals(), "_polymarket_services"):
    from ._service_models import BackendFactory as BackendFactory
    from ._service_models import MigrationBackend as MigrationBackend
    from ._service_models import MigrationRunner as MigrationRunner
    from ._service_models import ProbeRunner as ProbeRunner
    from ._service_models import ServicePrerequisiteError as ServicePrerequisiteError
    from ._service_models import ServiceVerificationError as ServiceVerificationError
    from ._service_redaction import redact_text as _redact_text
    from ._service_safety import LIBPQ_ROUTING_ENVIRONMENT as LIBPQ_ROUTING_ENVIRONMENT
    from ._service_safety import LIBPQ_ROUTING_QUERY_KEYS as LIBPQ_ROUTING_QUERY_KEYS
    from ._service_safety import REDIS_SCHEME as REDIS_SCHEME
    from ._service_safety import validate_loopback_database_url as validate_loopback_database_url
    from ._service_safety import validate_loopback_redis_url as validate_loopback_redis_url
    from ._service_safety import validate_redis_url as validate_redis_url

# Preserve public classes/functions and private redaction serialization through this entry point.
for _export in (
    ServicePrerequisiteError,
    ServiceVerificationError,
    MigrationBackend,
    validate_loopback_database_url,
    validate_redis_url,
    validate_loopback_redis_url,
    _redact_text,
):
    _export.__module__ = __name__

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PHASES = ("probe", "migrations", "all")
REQUIRED_ENVIRONMENT: Mapping[str, tuple[str, ...]] = {
    "probe": ("DATABASE_URL", "REDIS_URL"),
    "migrations": ("DATABASE_URL",),
    "all": ("DATABASE_URL", "REDIS_URL"),
}
PREREQUISITE_EXIT_CODE = 2
VERIFICATION_FAILURE_EXIT_CODE = 1


def _psycopg_dsn(url: URL) -> str:
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def alembic_subprocess_environment(database_url: str) -> dict[str, str]:
    """Build the migration subprocess environment with every libpq variable removed.

    The disposable-database URL carries the complete credentials, so the subprocess never
    needs ``PG*`` variables; dropping them all prevents any inherited libpq default (such
    as ``PGHOSTADDR`` or ``PGSERVICE``) from rerouting the migration off the loopback URL.
    """
    environment = {name: value for name, value in os.environ.items() if not name.startswith("PG")}
    environment["DATABASE_URL"] = database_url
    return environment


class RealMigrationBackend:
    """PostgreSQL and Alembic implementation of the migration proof contract."""

    def __init__(self, base_url: str) -> None:
        self._base_url = make_url(base_url)
        self._admin_url = self._base_url.set(database="postgres")

    def expected_revisions(self) -> tuple[str, str]:
        config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
        # The ini file's script_location is relative to the process working directory,
        # which the test harness moves; pin it to the repository's migration tree.
        config.set_main_option("script_location", str(REPOSITORY_ROOT / "alembic"))
        script = ScriptDirectory.from_config(config)
        head = script.get_current_head()
        if head is None:
            raise ServiceVerificationError("Alembic has no current head revision")
        revision = script.get_revision(head)
        if not isinstance(revision.down_revision, str):
            raise ServiceVerificationError("Alembic head has no single previous revision")
        return head, revision.down_revision

    def create_database(self, name: str) -> None:
        with psycopg.connect(_psycopg_dsn(self._admin_url), autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))

    def run_alembic(self, database_url: str, command: str, target: str) -> None:
        environment = alembic_subprocess_environment(database_url)
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


class _RedisFactory(Protocol):
    """Redis construction from a URL, typed without the client's untyped keyword arguments."""

    @classmethod
    def from_url(cls, url: str) -> Redis: ...


class _RedisCommands(Protocol):
    """The one Redis command the probe issues, typed without the client's untyped kwargs."""

    async def execute_command(self, *args: str) -> object: ...


def _redis_factory() -> type[_RedisFactory]:
    return Redis


def _redis_commands(client: Redis) -> _RedisCommands:
    return client


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
            redis = _redis_factory().from_url(validated_redis_url)
            cleanup.push_async_callback(redis.aclose)
            async with engine.connect() as connection:
                result = await connection.execute(text("SELECT 1"))
                if result.scalar_one() != 1:
                    raise ServiceVerificationError("PostgreSQL probe returned an unexpected value")
            if await _redis_commands(redis).execute_command("PING") is not True:
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
