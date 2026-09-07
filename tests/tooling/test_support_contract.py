"""Contract tests for repository-wide runtime support declarations."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from textwrap import dedent

CHECKER = Path(__file__).parents[2] / "scripts" / "check_support_contract.py"
REPOSITORY_ROOT = Path(__file__).parents[2]
POSTGRES_DIGEST = "a" * 64
REDIS_DIGEST = "b" * 64


def _write(root: Path, relative_path: str, content: str) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(content).lstrip(), encoding="utf-8")


def _valid_repository(root: Path) -> None:
    _write(
        root,
        "pyproject.toml",
        """
        [project]
        requires-python = ">=3.11,<3.14"
        dependencies = [
          "sqlalchemy[asyncio]>=2.0.0",
          "psycopg[binary]>=3.1.0",
        ]

        [tool.uv]
        required-version = ">=0.11,<0.12"

        [tool.ruff]
        target-version = "py311"

        [tool.mypy]
        python_version = "3.11"
        """,
    )
    _write(
        root,
        "uv.lock",
        """
        version = 1
        requires-python = ">=3.11, <3.14"

        [[package]]
        name = "greenlet"
        version = "3.3.0"

        [[package]]
        name = "psycopg"
        version = "3.3.5"

        [[package]]
        name = "sqlalchemy"
        version = "2.0.45"
        """,
    )
    _write(
        root,
        ".github/workflows/ci.yml",
        f"""
        name: CI
        on:
          push:
          pull_request:
            branches: [main]
        concurrency:
          group: ci-${{{{ github.workflow }}}}-${{{{ github.ref }}}}
          cancel-in-progress: true
        permissions:
          contents: read
        jobs:
          static:
            runs-on: ubuntu-24.04
            steps:
              - uses: actions/checkout@{"1" * 40} # v4.3.0
              - uses: astral-sh/setup-uv@{"2" * 40} # v7.2.1
                with:
                  version: "0.11.26"
              - run: uv sync --locked --all-extras
              - run: uv run python scripts/verify.py --profile static
          compatibility:
            runs-on: ubuntu-24.04
            strategy:
              matrix:
                python-version: ["3.11", "3.12", "3.13"]
            steps:
              - uses: actions/checkout@{"1" * 40} # v4.3.0
              - uses: astral-sh/setup-uv@{"2" * 40} # v7.2.1
                with:
                  version: "0.11.26"
              - run: uv sync --locked --all-extras --python ${{{{ matrix.python-version }}}}
              - run: uv run python scripts/verify.py --profile compatibility
          services:
            runs-on: ubuntu-24.04
            services:
              postgres:
                image: postgres:15@sha256:{POSTGRES_DIGEST}
              redis:
                image: redis:7@sha256:{REDIS_DIGEST}
            steps:
              - uses: actions/checkout@{"1" * 40} # v4.3.0
              - uses: astral-sh/setup-uv@{"2" * 40} # v7.2.1
                with:
                  version: "0.11.26"
              - run: uv sync --locked --all-extras --python 3.13
              - run: uv run python scripts/verify.py --profile services
          apple:
            runs-on: macos-14
            continue-on-error: true
            steps:
              - uses: actions/checkout@{"1" * 40} # v4.3.0
              - uses: astral-sh/setup-uv@{"2" * 40} # v7.2.1
                with:
                  version: "0.11.26"
              - run: test "$(uname -m)" = arm64
              - run: uv sync --locked --all-extras --python 3.13
              - run: uv run python scripts/verify.py --profile compatibility
          required:
            if: always()
            needs: [static, compatibility, services]
            runs-on: ubuntu-24.04
            steps:
              - run: test "${{{{ needs.static.result }}}}" = success
        """,
    )
    _write(
        root,
        "docker-compose.yml",
        f"""
        services:
          postgres:
            image: postgres:15@sha256:{POSTGRES_DIGEST}
            ports:
              - "${{POSTGRES_PORT:-5432}}:5432"
            environment:
              POSTGRES_DB: ${{POSTGRES_DB:-polymarket_tracker}}
              POSTGRES_USER: ${{POSTGRES_USER:-tracker}}
              POSTGRES_PASSWORD: ${{POSTGRES_PASSWORD:-dev_password}}
          redis:
            image: redis:7@sha256:{REDIS_DIGEST}
            ports:
              - "${{REDIS_PORT:-6379}}:6379"
        """,
    )
    _write(
        root,
        "README.md",
        """
        Supported Python versions are 3.11, 3.12, and 3.13.
        Linux is verified on Ubuntu 24.04 x86_64; Apple Silicon macOS is also supported.
        Run `uv sync --locked --all-extras` and `uv run python scripts/verify.py --profile all`.
        Use `postgresql+psycopg://tracker:dev_password@localhost:5432/polymarket_tracker`.
        Setup completes in under 5 minutes after prerequisites and initial container image downloads.
        """,
    )
    _write(
        root,
        ".env.example",
        "POSTGRES_HOST=localhost\n"
        "POSTGRES_PORT=5432\n"
        "POSTGRES_DB=polymarket_tracker\n"
        "POSTGRES_USER=tracker\n"
        "POSTGRES_PASSWORD=dev_password\n"
        "DATABASE_URL=postgresql+psycopg://tracker:dev_password@localhost:5432/polymarket_tracker\n"
        "REDIS_HOST=localhost\n"
        "REDIS_PORT=6379\n"
        "REDIS_URL=redis://localhost:6379\n",
    )
    _write(root, "alembic.ini", "[alembic]\nscript_location = alembic\n")
    _write(
        root,
        "alembic/env.py",
        """
        import os
        database_url = os.environ["DATABASE_URL"]
        config.set_main_option("sqlalchemy.url", database_url)
        """,
    )
    _write(
        root,
        "scripts/verify.py",
        """
        GATES = {
            "lock": ("uv", "lock", "--check"),
            "support-contract": ("python", "scripts/check_support_contract.py"),
            "format": ("python", "-m", "ruff", "format", "--check"),
            "lint": ("python", "-m", "ruff", "check"),
            "strict-types": (
                "uv", "run", "--isolated", "--locked", "--all-extras",
                "--python", "3.11", "mypy",
            ),
            "imports": ("python", "-c", "import polymarket_insider_tracker"),
            "tests": ("python", "-m", "pytest"),
            "services": ("python", "scripts/runtime_services.py", "--phase", "probe"),
            "migrations": ("python", "scripts/runtime_services.py", "--phase", "migrations"),
        }
        BASE_PROFILES = {
            "static": ("lock", "support-contract", "format", "lint", "strict-types"),
            "compatibility": ("lock", "imports", "tests"),
            "services": ("services", "migrations"),
        }
        """,
    )


def _run(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECKER), "--root", str(root)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_valid_repository_contract_passes(tmp_path: Path) -> None:
    _valid_repository(tmp_path)

    result = _run(tmp_path)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "support contract passed" in result.stdout.lower()


def test_repository_support_contract_is_complete() -> None:
    result = _run(REPOSITORY_ROOT)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == (
        "Support contract passed: tracked runtime surfaces are consistent."
    )


def test_tracked_quickstart_loads_environment_and_reflects_implementation() -> None:
    quickstart = (REPOSITORY_ROOT / "specs/002-reproducible-runtime/quickstart.md").read_text(
        encoding="utf-8"
    )

    assert "contractual design evidence until" not in quickstart
    assert "uv run --env-file .env python scripts/verify.py --profile all" in quickstart


def test_verifier_profiles_and_compose_defaults_cannot_drift(tmp_path: Path) -> None:
    _valid_repository(tmp_path)
    verifier_path = tmp_path / "scripts/verify.py"
    verifier_path.write_text(
        verifier_path.read_text(encoding="utf-8").replace(
            '"services": ("services", "migrations")', '"services": ("services",)'
        ),
        encoding="utf-8",
    )
    env_path = tmp_path / ".env.example"
    env_path.write_text(
        env_path.read_text(encoding="utf-8").replace("POSTGRES_PORT=5432", "POSTGRES_PORT=6543"),
        encoding="utf-8",
    )

    result = _run(tmp_path)

    assert result.returncode == 1
    assert "verification profile" in result.stdout.lower()
    assert "service settings" in result.stdout.lower()


def test_reports_all_cross_surface_contradictions(tmp_path: Path) -> None:
    _valid_repository(tmp_path)
    pyproject = (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        pyproject.replace(">=3.11,<3.14", ">=3.11").replace("sqlalchemy[asyncio]", "sqlalchemy"),
        encoding="utf-8",
    )
    workflow = (tmp_path / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    (tmp_path / ".github/workflows/ci.yml").write_text(
        workflow.replace('python-version: ["3.11", "3.12", "3.13"]', 'python-version: ["3.11"]')
        .replace(f"redis:7@sha256:{REDIS_DIGEST}", "redis:7")
        .replace("permissions:\n  contents: read", "permissions:\n  contents: write")
        .replace(f"actions/checkout@{'1' * 40}", "actions/checkout@v4")
        .replace("concurrency:\n", "missing-concurrency:\n"),
        encoding="utf-8",
    )
    readme = (tmp_path / "README.md").read_text(encoding="utf-8")
    (tmp_path / "README.md").write_text(
        readme.replace("3.11, 3.12, and 3.13", "3.11+")
        .replace("Ubuntu 24.04 x86_64", "Linux")
        .replace("postgresql+psycopg://", "postgresql://"),
        encoding="utf-8",
    )
    (tmp_path / "alembic.ini").write_text(
        "[alembic]\nsqlalchemy.url = postgresql://localhost/unsafe\n", encoding="utf-8"
    )

    result = _run(tmp_path)

    assert result.returncode == 1
    output = result.stdout.lower()
    for category in (
        "python range",
        "sqlalchemy asyncio",
        "python matrix",
        "permissions",
        "action pin",
        "concurrency",
        "service image",
        "readme",
        "alembic",
    ):
        assert category in output


def test_required_jobs_cannot_ignore_failures(tmp_path: Path) -> None:
    _valid_repository(tmp_path)
    workflow_path = tmp_path / ".github/workflows/ci.yml"
    workflow = workflow_path.read_text(encoding="utf-8")
    workflow_path.write_text(
        workflow.replace(
            "static:\n    runs-on: ubuntu-24.04",
            "static:\n    continue-on-error: true\n    runs-on: ubuntu-24.04",
        ),
        encoding="utf-8",
    )

    result = _run(tmp_path)

    assert result.returncode == 1
    assert "static" in result.stdout.lower()
    assert "ignored failure" in result.stdout.lower()


def test_diagnostics_never_echo_credentials(tmp_path: Path) -> None:
    _valid_repository(tmp_path)
    secret = "never-print-this-password"
    (tmp_path / ".env.example").write_text(
        f"DATABASE_URL=postgresql://tracker:{secret}@localhost:5432/db\n",
        encoding="utf-8",
    )

    result = _run(tmp_path)

    assert result.returncode == 1
    assert secret not in result.stdout
    assert secret not in result.stderr
