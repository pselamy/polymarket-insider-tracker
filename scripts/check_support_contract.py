#!/usr/bin/env python3
"""Check that every tracked runtime-support surface tells the same story."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from collections.abc import Sequence
from pathlib import Path

PYTHON_SPECIFIER = ">=3.11,<3.14"
LOCK_PYTHON_SPECIFIER = ">=3.11,<3.14"
PYTHON_MINORS = ("3.11", "3.12", "3.13")
UV_SPECIFIER = ">=0.11,<0.12"
CI_UV_VERSION = "0.11.26"
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
IMAGE_PATTERN = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")


def _compact_specifier(value: object) -> str:
    return re.sub(r"\s+", "", str(value))


def _read_text(root: Path, relative_path: str, findings: list[str]) -> str:
    path = root / relative_path
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        findings.append(f"missing tracked support surface: {relative_path}")
        return ""


def _read_toml(root: Path, relative_path: str, findings: list[str]) -> dict[str, object]:
    text = _read_text(root, relative_path, findings)
    if not text:
        return {}
    try:
        return tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        findings.append(f"invalid TOML in {relative_path}")
        return {}


def _dependency_present(dependencies: Sequence[object], required_prefix: str) -> bool:
    prefix = required_prefix.casefold()
    return any(str(dependency).casefold().startswith(prefix) for dependency in dependencies)


def _job_blocks(workflow: str) -> dict[str, str]:
    blocks: dict[str, list[str]] = {}
    current: str | None = None
    in_jobs = False
    for line in workflow.splitlines():
        if line == "jobs:":
            in_jobs = True
            continue
        if not in_jobs:
            continue
        match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if match:
            current = match.group(1)
            blocks[current] = [line]
            continue
        if current is not None:
            blocks[current].append(line)
    return {name: "\n".join(lines) for name, lines in blocks.items()}


def _workflow_image(workflow: str, name: str, major: int) -> str | None:
    match = re.search(rf"image:\s*({re.escape(name)}:{major}@sha256:[0-9a-f]{{64}})", workflow)
    return match.group(1) if match else None


def _check_project(root: Path, findings: list[str]) -> None:
    project = _read_toml(root, "pyproject.toml", findings)
    project_table = project.get("project", {})
    if not isinstance(project_table, dict):
        findings.append("python range: [project] metadata is missing")
        return

    if _compact_specifier(project_table.get("requires-python")) != PYTHON_SPECIFIER:
        findings.append(f"python range: pyproject.toml must declare exactly {PYTHON_SPECIFIER}")

    dependencies = project_table.get("dependencies", [])
    if not isinstance(dependencies, list):
        dependencies = []
    if not _dependency_present(dependencies, "sqlalchemy[asyncio]"):
        findings.append("SQLAlchemy asyncio: project dependency must require sqlalchemy[asyncio]")
    if not _dependency_present(dependencies, "psycopg[binary]"):
        findings.append("PostgreSQL driver: project dependency must require psycopg[binary]")

    tool_table = project.get("tool", {})
    if not isinstance(tool_table, dict):
        tool_table = {}
    uv_table = tool_table.get("uv", {})
    ruff_table = tool_table.get("ruff", {})
    mypy_table = tool_table.get("mypy", {})
    if not isinstance(uv_table, dict) or uv_table.get("required-version") != UV_SPECIFIER:
        findings.append(f"uv version: pyproject.toml must require exactly {UV_SPECIFIER}")
    if not isinstance(ruff_table, dict) or ruff_table.get("target-version") != "py311":
        findings.append("tool baseline: Ruff must target the lowest supported minor, py311")
    if not isinstance(mypy_table, dict) or mypy_table.get("python_version") != "3.11":
        findings.append("tool baseline: mypy must target the lowest supported minor, 3.11")


def _check_lock(root: Path, findings: list[str]) -> None:
    lock = _read_toml(root, "uv.lock", findings)
    if _compact_specifier(lock.get("requires-python")) != LOCK_PYTHON_SPECIFIER:
        findings.append(f"python range: uv.lock must declare exactly {LOCK_PYTHON_SPECIFIER}")
    packages = lock.get("package", [])
    if not isinstance(packages, list):
        packages = []
    names = {
        package.get("name")
        for package in packages
        if isinstance(package, dict) and isinstance(package.get("name"), str)
    }
    for required_name in ("greenlet", "psycopg", "sqlalchemy"):
        if required_name not in names:
            findings.append(f"locked dependency: uv.lock is missing {required_name}")


def _check_workflow(root: Path, findings: list[str]) -> tuple[str | None, str | None]:
    workflow = _read_text(root, ".github/workflows/ci.yml", findings)
    if not workflow:
        return None, None

    if not re.search(r"(?m)^permissions:\s*\n  contents: read\s*$", workflow):
        findings.append("workflow permissions: CI must grant only contents: read")
    if re.search(r"(?m)^\s+[A-Za-z_-]+:\s*write\s*$", workflow):
        findings.append("workflow permissions: write permission is prohibited")
    if not re.search(r"(?m)^  push:\s*$", workflow):
        findings.append("feature-branch trigger: CI must run on unfiltered branch pushes")
    if (
        re.search(r"(?m)^concurrency:\s*$", workflow) is None
        or "cancel-in-progress: true" not in workflow
    ):
        findings.append("workflow concurrency: superseded runs must be cancelled")

    uses_lines = [line for line in workflow.splitlines() if re.search(r"\buses:\s*", line)]
    for line in uses_lines:
        match = re.search(r"uses:\s*[^@\s]+@([^\s#]+)", line)
        if match is None or SHA_PATTERN.fullmatch(match.group(1)) is None or "#" not in line:
            findings.append(
                "action pin: every action must use a full commit SHA and release comment"
            )
            break

    if f'version: "{CI_UV_VERSION}"' not in workflow:
        findings.append(f"uv version: CI must install exact version {CI_UV_VERSION}")
    if "uv sync --locked --all-extras" not in workflow:
        findings.append("locked install: CI must use uv sync --locked --all-extras")

    matrix_match = re.search(r"python-version:\s*\[([^]]+)\]", workflow)
    matrix = tuple(re.findall(r"3\.\d+", matrix_match.group(1))) if matrix_match else ()
    if matrix != PYTHON_MINORS:
        findings.append("python matrix: CI must run exactly Python 3.11, 3.12, and 3.13")

    jobs = _job_blocks(workflow)
    for required_job in ("static", "compatibility", "services", "required"):
        block = jobs.get(required_job)
        if block is None:
            findings.append(f"required job: CI is missing {required_job}")
            continue
        if "runs-on: ubuntu-24.04" not in block:
            findings.append(f"Linux reference: {required_job} must run on ubuntu-24.04")
        if "continue-on-error: true" in block or "|| true" in block:
            findings.append(f"ignored failure: required job {required_job} must fail closed")

    required_block = jobs.get("required", "")
    for predecessor in ("static", "compatibility", "services"):
        if predecessor not in required_block:
            findings.append(f"required summary: stable required job must depend on {predecessor}")

    apple = jobs.get("apple", "")
    if not all(
        token in apple
        for token in (
            "runs-on: macos-14",
            "continue-on-error: true",
            "uname -m",
            "arm64",
            "--profile compatibility",
        )
    ):
        findings.append("Apple automation: advisory macos-14 arm64 compatibility job is incomplete")

    postgres = _workflow_image(workflow, "postgres", 15)
    redis = _workflow_image(workflow, "redis", 7)
    if postgres is None or redis is None:
        findings.append("service image: CI PostgreSQL 15 and Redis 7 must be digest-pinned")
    return postgres, redis


def _check_compose(
    root: Path,
    workflow_postgres: str | None,
    workflow_redis: str | None,
    findings: list[str],
) -> None:
    compose = _read_text(root, "docker-compose.yml", findings)
    compose_postgres = _workflow_image(compose, "postgres", 15)
    compose_redis = _workflow_image(compose, "redis", 7)
    if compose_postgres is None or compose_redis is None:
        findings.append("service image: Compose PostgreSQL 15 and Redis 7 must be digest-pinned")
        return
    if workflow_postgres and compose_postgres != workflow_postgres:
        findings.append("service image: PostgreSQL identities differ between Compose and CI")
    if workflow_redis and compose_redis != workflow_redis:
        findings.append("service image: Redis identities differ between Compose and CI")


def _check_documentation(root: Path, findings: list[str]) -> None:
    readme = _read_text(root, "README.md", findings)
    lowered = readme.casefold()
    required_fragments = {
        "finite Python versions": ("3.11", "3.12", "3.13"),
        "Linux reference": ("ubuntu 24.04", "x86_64"),
        "Apple support": ("apple silicon",),
        "locked install": ("uv sync --locked --all-extras",),
        "aggregate command": ("scripts/verify.py --profile all",),
        "canonical database URL": ("postgresql+psycopg://",),
        "timing boundary": ("under 5 minutes", "container image"),
    }
    for label, fragments in required_fragments.items():
        if not all(fragment.casefold() in lowered for fragment in fragments):
            findings.append(f"README {label}: documented support contract is incomplete")
    if "python 3.11+" in lowered:
        findings.append("README python range: open-ended Python 3.11+ promise is prohibited")

    env_text = _read_text(root, ".env.example", findings)
    env_values = {}
    for line in env_text.splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env_values[key] = value
    if not env_values.get("DATABASE_URL", "").startswith("postgresql+psycopg://"):
        findings.append("example database URL: .env.example must use postgresql+psycopg")
    if not env_values.get("REDIS_URL", "").startswith("redis://"):
        findings.append("example Redis URL: .env.example must provide a redis:// value")

    alembic_ini = _read_text(root, "alembic.ini", findings)
    if re.search(r"(?m)^sqlalchemy\.url\s*=\s*\S+", alembic_ini):
        findings.append(
            "Alembic configuration: alembic.ini must not contain a fallback database URL"
        )
    alembic_env = _read_text(root, "alembic/env.py", findings)
    if "DATABASE_URL" not in alembic_env or "set_main_option" not in alembic_env:
        findings.append("Alembic configuration: env.py must require and apply DATABASE_URL")


def find_contradictions(root: Path) -> list[str]:
    """Return every deterministic support-contract contradiction under ``root``."""
    findings: list[str] = []
    _check_project(root, findings)
    _check_lock(root, findings)
    postgres, redis = _check_workflow(root, findings)
    _check_compose(root, postgres, redis, findings)
    _check_documentation(root, findings)
    return findings


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="repository root")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the checker and return a stable process exit status."""
    args = _parse_args(argv)
    root = args.root.resolve()
    findings = find_contradictions(root)
    if findings:
        print(f"Support contract failed with {len(findings)} contradiction(s):")
        for index, finding in enumerate(findings, start=1):
            print(f"{index}. {finding}")
        return 1
    print("Support contract passed: tracked runtime surfaces are consistent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
