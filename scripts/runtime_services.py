#!/usr/bin/env python3
"""Verify local PostgreSQL, Redis, and disposable migration behavior."""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
import socket
import subprocess
import sys
import uuid
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Protocol

import psycopg
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg import sql
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url

from polymarket_insider_tracker.storage.database import create_async_db_engine
from polymarket_insider_tracker.storage.database_url import (
    normalize_database_url,
    render_database_url_safe,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PHASES = ("probe", "migrations", "all")


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


def _resolved_addresses(host: str) -> set[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        records = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ServicePrerequisiteError(
            "DATABASE_URL host could not be resolved for loopback verification"
        ) from exc
    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for record in records:
        try:
            addresses.add(ipaddress.ip_address(record[4][0]))
        except ValueError as exc:
            raise ServicePrerequisiteError(
                "DATABASE_URL host returned an invalid address during loopback verification"
            ) from exc
    return addresses


def validate_loopback_database_url(database_url: str) -> str:
    """Return a canonical URL only when every resolved host address is loopback."""
    canonical = normalize_database_url(database_url)
    host = make_url(canonical).host
    if host is None:
        raise ServicePrerequisiteError("DATABASE_URL must include a loopback host")
    addresses = _resolved_addresses(host)
    if not addresses or any(not address.is_loopback for address in addresses):
        raise ServicePrerequisiteError(
            "DATABASE_URL must resolve only to loopback addresses for service verification"
        )
    return canonical


def _psycopg_dsn(url: URL) -> str:
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def _redact_text(value: str, database_url: str) -> str:
    redacted = value.replace(database_url, render_database_url_safe(database_url))
    password = make_url(database_url).password
    if password:
        redacted = redacted.replace(password, "***")
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
    """Run a real async PostgreSQL query and Redis PING against local services."""
    canonical = validate_loopback_database_url(database_url)
    engine = create_async_db_engine(canonical)
    redis = Redis.from_url(redis_url)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(text("SELECT 1"))
            if result.scalar_one() != 1:
                raise ServiceVerificationError("PostgreSQL probe returned an unexpected value")
        if await redis.ping() is not True:
            raise ServiceVerificationError("Redis PING did not return PONG")
    except ServiceVerificationError:
        raise
    except Exception as exc:
        message = _redact_text(str(exc), canonical)
        raise ServiceVerificationError(f"service probe failed: {message}") from exc
    finally:
        await engine.dispose()
        await redis.aclose()
    return {"postgres_reachable": True, "redis_reachable": True}


def _temporary_database_url(base_url: str, database_name: str) -> str:
    return make_url(base_url).set(database=database_name).render_as_string(hide_password=False)


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

        backend.run_alembic(temporary_url, "upgrade", "head")
        migration_states.append(backend.current_revision(temporary_url))
        if migration_states[-1] != head:
            raise ServiceVerificationError("upgrade did not reach the Alembic head revision")

        backend.run_alembic(temporary_url, "downgrade", "-1")
        migration_states.append(backend.current_revision(temporary_url))
        if migration_states[-1] != previous:
            raise ServiceVerificationError("downgrade did not reach the previous Alembic revision")

        backend.run_alembic(temporary_url, "upgrade", "head")
        migration_states.append(backend.current_revision(temporary_url))
        if migration_states[-1] != head:
            raise ServiceVerificationError("re-upgrade did not return to the Alembic head revision")
        await backend.async_query(temporary_url)
    except Exception as exc:
        primary_error = exc
    finally:
        if created:
            try:
                backend.drop_database(database_name)
            except Exception as exc:
                cleanup_error = exc

    if cleanup_error is not None:
        message = _redact_text(str(cleanup_error), canonical)
        raise ServiceVerificationError(
            f"temporary database cleanup failed: {message}"
        ) from cleanup_error
    if primary_error is not None:
        message = _redact_text(str(primary_error), canonical)
        raise ServiceVerificationError(
            f"migration verification failed: {message}"
        ) from primary_error

    return {
        "temporary_database": database_name,
        "migration_states": migration_states,
        "async_query_succeeded": True,
        "cleanup_succeeded": True,
    }


async def execute_phase(
    phase: str,
    database_url: str,
    redis_url: str,
    *,
    probe_runner: ProbeRunner | None = None,
    migration_runner: MigrationRunner | None = None,
) -> dict[str, object]:
    """Execute one explicit service-verification phase and return merged evidence."""
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


def main(argv: Sequence[str] | None = None) -> int:
    """Run the selected phase and map failures to the documented exit semantics."""
    args = _parse_args(argv)
    database_url = os.environ.get("DATABASE_URL")
    redis_url = os.environ.get("REDIS_URL")
    if not database_url or not redis_url:
        message = "DATABASE_URL and REDIS_URL are required"
        if args.json:
            print(json.dumps({"phase": args.phase, "status": "error", "error": message}))
        else:
            print(f"[FAIL] runtime-{args.phase}: {message}", file=sys.stderr)
        return 2
    try:
        evidence = asyncio.run(execute_phase(args.phase, database_url, redis_url))
    except ServicePrerequisiteError as exc:
        if args.json:
            print(json.dumps({"phase": args.phase, "status": "error", "error": str(exc)}))
        else:
            print(f"[FAIL] runtime-{args.phase}: {exc}", file=sys.stderr)
        return 2
    except ServiceVerificationError as exc:
        if args.json:
            print(json.dumps({"phase": args.phase, "status": "failed", "error": str(exc)}))
        else:
            print(f"[FAIL] runtime-{args.phase}: {exc}", file=sys.stderr)
        return 1
    _emit_result(args.phase, "passed", evidence, args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
