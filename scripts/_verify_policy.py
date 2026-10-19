"""Canonical commands, ordered profiles and direct equivalents."""

from __future__ import annotations

import sys
from collections.abc import Mapping

from ._verify_models import Gate

GATES: Mapping[str, Gate] = {
    "lock": Gate("lock", ("uv", "lock", "--check")),
    "format": Gate("format", (sys.executable, "-m", "black", "--check", ".")),
    "lint": Gate("lint", (sys.executable, "-m", "ruff", "check", "src", "tests", "scripts")),
    "strict-types": Gate(
        "strict-types",
        (
            "uv",
            "run",
            "--isolated",
            "--locked",
            "--all-extras",
            "--python",
            "3.11",
            "mypy",
        ),
    ),
    "pyright": Gate(
        "pyright",
        (
            "uv",
            "run",
            "--isolated",
            "--locked",
            "--all-extras",
            "--python",
            "3.11",
            "pyright",
            "src/polymarket_insider_tracker",
        ),
    ),
    "vulture": Gate(
        "vulture",
        (
            "uv",
            "run",
            "--isolated",
            "--locked",
            "--all-extras",
            "--python",
            "3.11",
            "vulture",
            "src",
            "tests",
            "scripts",
            "alembic",
            "conftest.py",
            "quality",
        ),
    ),
    "complexipy": Gate(
        "complexipy",
        (
            "uv",
            "run",
            "--isolated",
            "--locked",
            "--all-extras",
            "--python",
            "3.11",
            "python",
            "scripts/complexipy_gate.py",
            "src",
            "tests",
            "scripts",
            "alembic",
            "conftest.py",
            "quality",
            "--max-complexity-allowed",
            "5",
            "--no-ignore",
            "--ignore-complexity=false",
            "--snapshot-ignore=true",
            "--snapshot-create=false",
            "--exclude=.",
            "--check-script=true",
        ),
    ),
    "cyclomatic": Gate(
        "cyclomatic",
        (
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--isolated",
            "--select",
            "C901,E902",
            "--config",
            "lint.mccabe.max-complexity=10",
            "--ignore-noqa",
            "--no-respect-gitignore",
            "--exclude",
            "",
            "src",
            "tests",
            "scripts",
            "alembic",
            "conftest.py",
        ),
    ),
    "imports": Gate(
        "imports",
        (
            sys.executable,
            "-c",
            (
                "import alembic, greenlet, psycopg, redis, sqlalchemy; "
                "import polymarket_insider_tracker; "
                "from sqlalchemy.ext.asyncio import create_async_engine"
            ),
        ),
    ),
    "tests": Gate("tests", (sys.executable, "-m", "pytest")),
    "services": Gate(
        "services",
        (sys.executable, "scripts/runtime_services.py", "--phase", "probe"),
        needs_services=True,
    ),
    "redis-contract": Gate(
        "redis-contract",
        (sys.executable, "-m", "pytest", "tests/integration/test_redis_contract.py"),
        needs_services=True,
        environment=(("RUN_SERVICE_TESTS", "1"),),
    ),
    "migrations": Gate(
        "migrations",
        (sys.executable, "scripts/runtime_services.py", "--phase", "migrations"),
        needs_services=True,
    ),
}


BASE_PROFILES: Mapping[str, tuple[str, ...]] = {
    "static": (
        "lock",
        "format",
        "lint",
        "strict-types",
        "pyright",
        "vulture",
        "complexipy",
        "cyclomatic",
    ),
    "compatibility": ("lock", "imports", "tests"),
    "services": ("services", "redis-contract", "migrations"),
}


PROFILE_NAMES = ("static", "compatibility", "services", "all")


DIRECT_GATE_COMMANDS: tuple[tuple[str, str], ...] = (
    (
        "cyclomatic",
        "uv run python -m ruff check --isolated --select C901,E902 "
        "--config lint.mccabe.max-complexity=10 --ignore-noqa --no-respect-gitignore "
        "--exclude '' src tests scripts alembic conftest.py",
    ),
    ("lock", "uv lock --check"),
    ("format", "uv run black --check ."),
    ("lint", "uv run ruff check src tests scripts"),
    ("strict-types", "uv run --isolated --locked --all-extras --python 3.11 mypy"),
    (
        "pyright",
        "uv run --isolated --locked --all-extras --python 3.11 "
        "pyright src/polymarket_insider_tracker",
    ),
    (
        "vulture",
        "uv run --isolated --locked --all-extras --python 3.11 vulture "
        "src tests scripts alembic conftest.py quality",
    ),
    (
        "complexipy",
        "uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py "
        "src tests scripts alembic conftest.py quality --max-complexity-allowed 5 --no-ignore "
        "--ignore-complexity=false --snapshot-ignore=true --snapshot-create=false --exclude=. "
        "--check-script=true",
    ),
    (
        "imports",
        'uv run python -c "import alembic, greenlet, psycopg, redis, sqlalchemy; '
        "import polymarket_insider_tracker; "
        'from sqlalchemy.ext.asyncio import create_async_engine"',
    ),
    ("tests", "uv run pytest"),
    ("services", "uv run --env-file .env python scripts/runtime_services.py --phase probe"),
    (
        "redis-contract",
        "RUN_SERVICE_TESTS=1 uv run --env-file .env pytest tests/integration/test_redis_contract.py",
    ),
    (
        "migrations",
        "uv run --env-file .env python scripts/runtime_services.py --phase migrations",
    ),
)


APPLY_MIGRATIONS_COMMAND = "uv run --env-file .env alembic upgrade head"


def help_epilog() -> str:
    width = max(len(gate_id) for gate_id, _ in DIRECT_GATE_COMMANDS)
    lines = ["direct gate commands (every aggregate gate remains directly runnable):"]
    lines.extend(f"  {gate_id:<{width}}  {command}" for gate_id, command in DIRECT_GATE_COMMANDS)
    lines.append("")
    lines.append("related commands:")
    lines.append(f"  {'apply-migrations':<{width}}  {APPLY_MIGRATIONS_COMMAND}")
    lines.append("")
    lines.append(
        "services and migrations read DATABASE_URL and REDIS_URL; load them with --env-file .env."
    )
    lines.append(
        "exit status: 0 every selected gate passed; 1 a gate ran and failed; "
        "2 invocation or prerequisite error."
    )
    return "\n".join(lines)
