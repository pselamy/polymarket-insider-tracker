#!/usr/bin/env python3
"""Check that every tracked runtime-support surface tells the same story."""

from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlsplit

PYTHON_SPECIFIER = ">=3.11,<3.14"
LOCK_PYTHON_SPECIFIER = ">=3.11,<3.14"
PYTHON_MINORS = ("3.11", "3.12", "3.13")
UV_SPECIFIER = ">=0.11,<0.12"
CI_UV_VERSION = "0.11.26"
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
IMAGE_PATTERN = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
COMPOSE_DEFAULT_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*):-([^}]*)\}")

# The verifier's public gate contract; a drift here changes what CI and contributors prove.
EXPECTED_PROFILES: Mapping[str, tuple[str, ...]] = {
    "static": ("lock", "support-contract", "format", "lint", "strict-types"),
    "compatibility": ("lock", "imports", "tests"),
    "services": ("services", "migrations"),
}
EXPECTED_GATE_IDS = frozenset(
    gate_id for gate_ids in EXPECTED_PROFILES.values() for gate_id in gate_ids
)
STRICT_TYPES_COMMAND = (
    "uv",
    "run",
    "--isolated",
    "--locked",
    "--all-extras",
    "--python",
    "3.11",
    "mypy",
)


class _UrlComponents(NamedTuple):
    """URL components compared against the discrete service settings; values are never printed."""

    username: str | None
    password: str | None
    hostname: str | None
    port: str | None
    database: str


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
    compose: str,
    workflow_postgres: str | None,
    workflow_redis: str | None,
    findings: list[str],
) -> None:
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

    alembic_ini = _read_text(root, "alembic.ini", findings)
    if re.search(r"(?m)^sqlalchemy\.url\s*=\s*\S+", alembic_ini):
        findings.append(
            "Alembic configuration: alembic.ini must not contain a fallback database URL"
        )
    alembic_env = _read_text(root, "alembic/env.py", findings)
    if "DATABASE_URL" not in alembic_env or "set_main_option" not in alembic_env:
        findings.append("Alembic configuration: env.py must require and apply DATABASE_URL")


def _env_values(env_text: str) -> dict[str, str]:
    """Parse ``KEY=value`` lines from a dotenv-style file, ignoring comments and blanks."""
    values: dict[str, str] = {}
    for raw_line in env_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def _compose_defaults(compose: str, findings: list[str]) -> dict[str, str]:
    """Collect every ``${VAR:-default}`` in Compose and reject conflicting defaults for one VAR."""
    defaults: dict[str, str] = {}
    for key, value in COMPOSE_DEFAULT_PATTERN.findall(compose):
        if key in defaults and defaults[key] != value:
            findings.append(f"service settings: docker-compose.yml declares two defaults for {key}")
        defaults.setdefault(key, value)
    return defaults


def _url_components(value: str) -> _UrlComponents | None:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return None
    return _UrlComponents(
        username=parsed.username,
        password=parsed.password,
        hostname=parsed.hostname,
        port=None if port is None else str(port),
        database=parsed.path.lstrip("/"),
    )


def _check_service_url(
    label: str,
    url: str,
    expected: Mapping[str, tuple[str, str | None]],
    findings: list[str],
) -> None:
    """Compare URL components with the discrete settings, naming keys but never values."""
    components = _url_components(url)
    if components is None:
        findings.append(f"service settings: .env.example {label} is not a valid URL")
        return
    for component, (setting_name, expected_value) in expected.items():
        if expected_value is None:
            continue
        if getattr(components, component) != expected_value:
            findings.append(
                f"service settings: .env.example {label} {component} does not match {setting_name}"
            )


def _check_environment_example(
    env_values: Mapping[str, str],
    compose_defaults: Mapping[str, str],
    findings: list[str],
) -> None:
    """Require one consistent local service story across .env.example and Compose."""
    for key in sorted(env_values.keys() & compose_defaults.keys()):
        if env_values[key] != compose_defaults[key]:
            findings.append(
                f"service settings: .env.example {key} does not match the docker-compose.yml default"
            )

    def setting(key: str, fallback: str | None = None) -> str | None:
        return env_values.get(key) or compose_defaults.get(key) or fallback

    database_url = env_values.get("DATABASE_URL", "")
    if not database_url.startswith("postgresql+psycopg://"):
        findings.append("example database URL: .env.example must use postgresql+psycopg")
    if database_url:
        _check_service_url(
            "DATABASE_URL",
            database_url,
            {
                "username": ("POSTGRES_USER", setting("POSTGRES_USER")),
                "password": ("POSTGRES_PASSWORD", setting("POSTGRES_PASSWORD")),
                "hostname": ("POSTGRES_HOST", setting("POSTGRES_HOST", "localhost")),
                "port": ("POSTGRES_PORT", setting("POSTGRES_PORT")),
                "database": ("POSTGRES_DB", setting("POSTGRES_DB")),
            },
            findings,
        )

    redis_url = env_values.get("REDIS_URL", "")
    if not redis_url.startswith("redis://"):
        findings.append("example Redis URL: .env.example must provide a redis:// value")
    if redis_url:
        _check_service_url(
            "REDIS_URL",
            redis_url,
            {
                "hostname": ("REDIS_HOST", setting("REDIS_HOST", "localhost")),
                "port": ("REDIS_PORT", setting("REDIS_PORT")),
            },
            findings,
        )


def _module_assignment(tree: ast.Module, name: str) -> ast.expr | None:
    """Return the value assigned to module-level ``name``, with or without an annotation."""
    for node in tree.body:
        if isinstance(node, ast.AnnAssign):
            target: ast.expr | None = node.target
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        else:
            continue
        if isinstance(target, ast.Name) and target.id == name and node.value is not None:
            return node.value
    return None


def _string_tuple(node: ast.expr) -> tuple[str, ...] | None:
    """Return a tuple/list literal as strings; non-literal elements become ``<expression>``."""
    if not isinstance(node, (ast.Tuple, ast.List)):
        return None
    return tuple(
        element.value
        if isinstance(element, ast.Constant) and isinstance(element.value, str)
        else "<expression>"
        for element in node.elts
    )


def _gate_command(node: ast.expr) -> tuple[str, ...] | None:
    """Extract the command from ``Gate(id, command, ...)``, ``Gate(command=...)``, or a tuple."""
    if not isinstance(node, ast.Call):
        return _string_tuple(node)
    if len(node.args) >= 2:
        return _string_tuple(node.args[1])
    for keyword in node.keywords:
        if keyword.arg == "command":
            return _string_tuple(keyword.value)
    return None


def _profile_table(node: ast.expr) -> dict[str, tuple[str, ...]] | None:
    if not isinstance(node, ast.Dict):
        return None
    profiles: dict[str, tuple[str, ...]] = {}
    for key, value in zip(node.keys, node.values, strict=True):
        gate_ids = _string_tuple(value)
        if not (isinstance(key, ast.Constant) and isinstance(key.value, str)) or gate_ids is None:
            return None
        profiles[key.value] = gate_ids
    return profiles


def _gate_table(node: ast.expr) -> dict[str, tuple[str, ...]] | None:
    if not isinstance(node, ast.Dict):
        return None
    gates: dict[str, tuple[str, ...]] = {}
    for key, value in zip(node.keys, node.values, strict=True):
        if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
            return None
        gates[key.value] = _gate_command(value) or ()
    return gates


def _check_verifier(root: Path, findings: list[str]) -> None:
    """Read the verifier's literal gate and profile tables without executing it."""
    verifier = _read_text(root, "scripts/verify.py", findings)
    if not verifier:
        return
    try:
        tree = ast.parse(verifier)
    except SyntaxError:
        findings.append("verification profile: scripts/verify.py is not valid Python")
        return

    profiles_node = _module_assignment(tree, "BASE_PROFILES")
    profiles = _profile_table(profiles_node) if profiles_node is not None else None
    if profiles is None:
        findings.append(
            "verification profile: scripts/verify.py must define BASE_PROFILES as a literal mapping"
        )
    else:
        for profile, expected in EXPECTED_PROFILES.items():
            if profiles.get(profile) != expected:
                findings.append(
                    f"verification profile: {profile} must be exactly {', '.join(expected)}"
                )
        for profile in sorted(profiles.keys() - EXPECTED_PROFILES.keys()):
            findings.append(f"verification profile: unexpected base profile {profile}")

    gates_node = _module_assignment(tree, "GATES")
    gates = _gate_table(gates_node) if gates_node is not None else None
    if gates is None:
        findings.append(
            "verification profile: scripts/verify.py must define GATES as a literal mapping"
        )
        return
    if set(gates) != EXPECTED_GATE_IDS:
        findings.append(
            "verification profile: GATES must define exactly "
            + ", ".join(sorted(EXPECTED_GATE_IDS))
        )
    if gates.get("strict-types") != STRICT_TYPES_COMMAND:
        findings.append(
            "tool baseline: the strict-types gate must run " + " ".join(STRICT_TYPES_COMMAND)
        )


def _filesystem_claude_skills(root: Path) -> list[str]:
    """Collect .claude/skills paths from the filesystem for non-git fixture roots."""
    claude_skills = root / ".claude" / "skills"
    if claude_skills.is_file():
        return [str(claude_skills.relative_to(root).as_posix())]
    if claude_skills.is_dir():
        entries = [
            str(path.relative_to(root).as_posix())
            for path in claude_skills.rglob("*")
            if path.is_file()
        ]
        return entries if entries else [str(claude_skills.relative_to(root).as_posix())]
    return []


def _check_hygiene(root: Path, findings: list[str]) -> None:
    """Ensure noncanonical surfaces such as tracked .claude/skills are prohibited."""
    git_surface = root / ".git"
    tracked_entries: list[str] = []
    if git_surface.exists():
        try:
            proc = subprocess.run(
                ["git", "-C", str(root), "ls-files", "--", ".claude/skills"],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0:
                tracked_entries = [
                    line.strip() for line in proc.stdout.splitlines() if line.strip()
                ]
            else:
                findings.append(
                    f"repository hygiene: git ls-files failed with exit code {proc.returncode}"
                )
                return
        except OSError:
            tracked_entries = _filesystem_claude_skills(root)
    else:
        tracked_entries = _filesystem_claude_skills(root)

    for entry in sorted(tracked_entries):
        findings.append(
            f"repository hygiene: tracked .claude/skills entries are prohibited: {entry}"
        )


def find_contradictions(root: Path) -> list[str]:
    """Return every deterministic support-contract contradiction under ``root``."""
    root = root.resolve()
    findings: list[str] = []
    _check_hygiene(root, findings)
    _check_project(root, findings)
    _check_lock(root, findings)
    postgres, redis = _check_workflow(root, findings)
    compose = _read_text(root, "docker-compose.yml", findings)
    _check_compose(compose, postgres, redis, findings)
    _check_documentation(root, findings)
    env_values = _env_values(_read_text(root, ".env.example", findings))
    _check_environment_example(env_values, _compose_defaults(compose, findings), findings)
    _check_verifier(root, findings)
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
