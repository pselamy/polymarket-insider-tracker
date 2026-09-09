"""Contract tests for the aggregate runtime verifier."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tomllib
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

MODULE_PATH = Path(__file__).parents[2] / "scripts" / "verify.py"


def _load_module() -> ModuleType:
    assert MODULE_PATH.exists(), "aggregate verifier is not implemented"
    spec = importlib.util.spec_from_file_location("verify_under_test", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _passing_runner(module: ModuleType) -> Callable[[Any], Any]:
    def run(_gate: Any) -> Any:
        return module.CommandExecution(exit_code=0, stdout="ok\n", duration_seconds=0.25)

    return run


def test_profile_membership_and_ordering_are_exact() -> None:
    module = _load_module()

    assert module.gate_ids_for_profile("static") == (
        "lock",
        "format",
        "lint",
        "strict-types",
        "pyright",
        "vulture",
        "complexipy",
    )
    assert module.gate_ids_for_profile("compatibility") == ("lock", "imports", "tests")
    assert module.gate_ids_for_profile("services") == ("services", "migrations")


def test_all_profile_preserves_first_seen_order_and_deduplicates() -> None:
    module = _load_module()

    assert module.gate_ids_for_profile("all") == (
        "lock",
        "format",
        "lint",
        "strict-types",
        "pyright",
        "vulture",
        "complexipy",
        "imports",
        "tests",
        "services",
        "migrations",
    )


def test_vulture_gate_is_unfiltered_and_names_its_complete_scope() -> None:
    module = _load_module()
    canonical_scope = ("src", "tests", "scripts", "alembic", "conftest.py")

    assert module.GATES["vulture"].command == (
        "uv",
        "run",
        "--isolated",
        "--locked",
        "--all-extras",
        "--python",
        "3.11",
        "vulture",
        *canonical_scope,
    )
    with (MODULE_PATH.parents[1] / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    assert tuple(pyproject["tool"]["vulture"]["paths"]) == canonical_scope
    assert (
        dict(module.DIRECT_GATE_COMMANDS)["vulture"]
        == f"uv run --isolated --locked --all-extras --python 3.11 vulture {' '.join(canonical_scope)}"
    )


def test_complexipy_gate_is_configured_and_matches_canonical_policy() -> None:
    module = _load_module()
    canonical_scope = ("src", "tests", "scripts", "alembic", "conftest.py")

    assert module.GATES["complexipy"].command == (
        "uv",
        "run",
        "--isolated",
        "--locked",
        "--all-extras",
        "--python",
        "3.11",
        "complexipy",
        *canonical_scope,
        "--max-complexity-allowed",
        "5",
        "--no-ignore",
        "--ignore-complexity=false",
    )
    with (MODULE_PATH.parents[1] / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    assert tuple(pyproject["tool"]["complexipy"]["paths"]) == canonical_scope
    assert pyproject["tool"]["complexipy"]["max-complexity-allowed"] == 5
    assert pyproject["tool"]["complexipy"]["no-ignore"] is True
    assert dict(module.DIRECT_GATE_COMMANDS)["complexipy"] == (
        "uv run --isolated --locked --all-extras --python 3.11 complexipy "
        "src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore "
        "--ignore-complexity=false"
    )


def _match_scope_entry(file_path: str, scope: Sequence[str]) -> str:
    matches = [entry for entry in scope if file_path == entry or file_path.startswith(f"{entry}/")]
    assert (
        len(matches) == 1
    ), f"tracked file {file_path} must match exactly one entry, got {matches}"
    return matches[0]


def _assert_all_tracked_files_covered(scope: Sequence[str], root: Path) -> None:
    completed = subprocess.run(
        ["git", "ls-files", "*.py"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    tracked_files = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    assert tracked_files, "expected tracked python files in repository"
    covered: set[str] = {_match_scope_entry(path, scope) for path in tracked_files}
    for entry in scope:
        assert entry in covered, f"scope entry {entry} covers no tracked files"


def test_all_tracked_python_files_are_covered_by_vulture_scope() -> None:
    module = _load_module()
    vulture_command = module.GATES["vulture"].command
    vulture_index = vulture_command.index("vulture")
    canonical_scope = vulture_command[vulture_index + 1 :]
    root = MODULE_PATH.parents[1]
    _assert_all_tracked_files_covered(canonical_scope, root)


def test_all_tracked_python_files_are_covered_by_complexipy_scope() -> None:
    root = MODULE_PATH.parents[1]
    with (root / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    canonical_scope = tuple(pyproject["tool"]["complexipy"]["paths"])
    _assert_all_tracked_files_covered(canonical_scope, root)


def test_mypy_gate_uses_the_minimum_supported_dependency_resolution() -> None:
    module = _load_module()

    assert module.GATES["strict-types"].command == (
        "uv",
        "run",
        "--isolated",
        "--locked",
        "--all-extras",
        "--python",
        "3.11",
        "mypy",
    )


def test_pyright_gate_is_strict_and_covers_the_complete_first_party_package() -> None:
    module = _load_module()

    assert module.GATES["pyright"].command == (
        "uv",
        "run",
        "--isolated",
        "--locked",
        "--all-extras",
        "--python",
        "3.11",
        "pyright",
        "src/polymarket_insider_tracker",
    )


def test_format_gate_uses_black_against_the_entire_repository() -> None:
    module = _load_module()

    assert module.GATES["format"].command == (
        module.sys.executable,
        "-m",
        "black",
        "--check",
        ".",
    )


def test_gate_definitions_expose_prerequisites_and_redaction_policy() -> None:
    module = _load_module()

    assert set(module.GATES) == set(module.gate_ids_for_profile("all"))
    for gate in module.GATES.values():
        assert gate.redaction_policy == "configured-secrets"
        assert gate.needs_services is (gate.id in {"services", "migrations"})


def test_runtime_gates_stay_in_the_selected_python_environment() -> None:
    module = _load_module()

    for gate_id in (
        "format",
        "lint",
        "imports",
        "tests",
        "services",
        "migrations",
    ):
        assert module.GATES[gate_id].command[0] == module.sys.executable
        assert "uv" not in module.GATES[gate_id].command


def test_tests_gate_scrubs_application_configuration_but_service_gate_keeps_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_module()
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://tracker:secret@localhost/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    monkeypatch.setenv("POLYGON_FALLBACK_RPC_URL", "https://fallback.invalid")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.invalid/secret")
    monkeypatch.setenv("RUN_SERVICE_TESTS", "1")
    monkeypatch.setenv("PATH", "/usr/bin")
    environments: list[dict[str, str] | None] = []

    def run(*_args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        environments.append(kwargs.get("env"))
        return subprocess.CompletedProcess([], 0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", run)

    module._run_command(module.GATES["tests"])
    module._run_command(module.GATES["services"])

    tests_environment, services_environment = environments
    assert tests_environment is not None
    assert tests_environment["PATH"] == "/usr/bin"
    for key in (
        "DATABASE_URL",
        "REDIS_URL",
        "POLYGON_FALLBACK_RPC_URL",
        "DISCORD_WEBHOOK_URL",
        "RUN_SERVICE_TESTS",
    ):
        assert key not in tests_environment
    assert services_environment is None


@pytest.mark.parametrize(
    "failed_gate",
    [
        "lock",
        "format",
        "lint",
        "strict-types",
        "pyright",
        "vulture",
        "complexipy",
        "imports",
        "tests",
        "services",
        "migrations",
    ],
)
def test_each_required_gate_failure_fails_closed(failed_gate: str) -> None:
    module = _load_module()
    calls: list[str] = []

    def runner(gate: Any) -> Any:
        calls.append(gate.id)
        exit_code = 17 if gate.id == failed_gate else 0
        return module.CommandExecution(
            exit_code=exit_code,
            stdout=f"output from {gate.id}\n",
            duration_seconds=0.1,
        )

    result = module.run_verification("all", runner=runner)
    result_by_id = {gate.id: gate for gate in result.gates}

    assert result.status == "failed"
    assert result.exit_code == 1
    assert result.first_failed_gate == failed_gate
    assert result_by_id[failed_gate].status == "failed"
    assert calls[-1] == failed_gate
    selected_ids = module.gate_ids_for_profile("all")
    for gate_id in selected_ids[selected_ids.index(failed_gate) + 1 :]:
        assert result_by_id[gate_id].status == "not-run"


def test_human_output_names_commands_and_results() -> None:
    module = _load_module()

    rendered = module.render_human(
        module.run_verification("services", runner=_passing_runner(module))
    )

    assert "[RUN ] services:" in rendered
    assert "scripts/runtime_services.py --phase probe" in rendered
    assert "[PASS] services (0.25s)" in rendered
    assert "[RUN ] migrations:" in rendered
    assert "scripts/runtime_services.py --phase migrations" in rendered
    assert "status: passed" in rendered


def test_json_output_is_one_machine_readable_aggregate() -> None:
    module = _load_module()
    result = module.run_verification("compatibility", runner=_passing_runner(module))

    rendered = module.render_json(result)
    parsed = json.loads(rendered)

    assert rendered.count("\n") == 0
    assert parsed["profile"] == "compatibility"
    assert parsed["status"] == "passed"
    assert parsed["exit_code"] == 0
    assert [gate["id"] for gate in parsed["gates"]] == ["lock", "imports", "tests"]


def test_first_failure_output_includes_not_run_gates() -> None:
    module = _load_module()

    def runner(gate: Any) -> Any:
        return module.CommandExecution(
            exit_code=1 if gate.id == "format" else 0,
            stderr="deliberate failure\n" if gate.id == "format" else "",
            duration_seconds=0.2,
        )

    rendered = module.render_human(module.run_verification("static", runner=runner))

    assert "[FAIL] format (exit 1, 0.20s): deliberate failure" in rendered
    assert "[SKIP] lint: not run after format failed" in rendered
    assert "[SKIP] strict-types: not run after format failed" in rendered
    assert "[SKIP] pyright: not run after format failed" in rendered
    assert "[SKIP] vulture: not run after format failed" in rendered
    assert "[SKIP] complexipy: not run after format failed" in rendered
    assert "first failed gate: format" in rendered


def test_prerequisite_exit_code_is_preserved_in_human_and_json_results() -> None:
    module = _load_module()

    def runner(gate: Any) -> Any:
        return module.CommandExecution(
            exit_code=2 if gate.id == "services" else 0,
            stderr="missing service configuration\n" if gate.id == "services" else "",
            duration_seconds=0.1,
        )

    result = module.run_verification("services", runner=runner)

    assert result.status == "failed"
    assert result.exit_code == 2
    assert result.first_failed_gate == "services"
    assert result.gates[1].status == "not-run"
    assert "exit 2" in module.render_human(result)
    assert json.loads(module.render_json(result))["exit_code"] == 2


def test_invalid_profile_is_an_invocation_error_without_running_gates(capsys: Any) -> None:
    module = _load_module()
    calls: list[str] = []

    def runner(gate: Any) -> Any:
        calls.append(gate.id)
        return module.CommandExecution(exit_code=0)

    exit_code = module.main(["--profile", "unknown"], runner=runner)
    captured = capsys.readouterr()

    assert exit_code == 2
    assert calls == []
    assert captured.out == ""
    assert "choose from" in captured.err.lower()


@pytest.mark.parametrize(
    "argv",
    [
        ["--json"],
        ["--profile", "unknown", "--json"],
        ["--profile", "static", "--json", "--unknown-option"],
    ],
)
def test_json_invocation_errors_emit_one_machine_readable_object(
    argv: list[str], capsys: Any
) -> None:
    module = _load_module()

    exit_code = module.main(argv, runner=_passing_runner(module))
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 2
    assert captured.out.count("\n") == 1
    assert captured.err == ""
    assert parsed["status"] == "error"
    assert parsed["exit_code"] == 2
    assert parsed["gates"] == []
    assert parsed["error"]


def test_help_lists_every_direct_gate_command() -> None:
    result = subprocess.run(
        [sys.executable, str(MODULE_PATH), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    for command in (
        "uv run black --check .",
        "uv run ruff check src tests scripts",
        "uv run --isolated --locked --all-extras --python 3.11 mypy",
        "uv run --isolated --locked --all-extras --python 3.11 "
        "pyright src/polymarket_insider_tracker",
        "uv run --isolated --locked --all-extras --python 3.11 vulture "
        "src tests scripts alembic conftest.py",
        "uv run --isolated --locked --all-extras --python 3.11 complexipy "
        "src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore "
        "--ignore-complexity=false",
        "uv run pytest",
        "uv run --env-file .env alembic upgrade head",
    ):
        assert command in result.stdout


@pytest.mark.parametrize("as_json", [False, True])
def test_outputs_redact_database_credentials(
    as_json: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_module()
    secret = "do-not-print-this-password"
    database_url = f"postgresql+psycopg://tracker:{secret}@localhost:5432/research"
    monkeypatch.setenv("DATABASE_URL", database_url)

    def runner(_gate: Any) -> Any:
        return module.CommandExecution(
            exit_code=1,
            stdout=f"connection failed: {database_url}\n",
            stderr=f"password={secret}\n",
            duration_seconds=0.1,
        )

    result = module.run_verification("services", runner=runner)
    rendered = module.render_json(result) if as_json else module.render_human(result)

    assert secret not in rendered
    assert database_url not in rendered
    assert "***" in rendered


def test_malformed_sensitive_url_cannot_break_redaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_module()
    secret = "malformed-url-password"
    redis_url = f"redis://tracker:{secret}@localhost:99999/0"
    monkeypatch.setenv("REDIS_URL", redis_url)

    rendered = module.redact_text(f"could not parse {redis_url}")

    assert secret not in rendered
    assert redis_url not in rendered
    assert "***" in rendered


def test_non_url_secrets_are_redacted_as_complete_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_module()
    webhook = "https://discord.com/api/webhooks/public-id/private-token"
    api_key = "polymarket-private-key"
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", webhook)
    monkeypatch.setenv("POLYMARKET_API_KEY", api_key)

    rendered = module.redact_text(f"webhook={webhook} api_key={api_key}")

    assert webhook not in rendered
    assert "private-token" not in rendered
    assert api_key not in rendered
    assert rendered == "webhook=*** api_key=***"
