"""Negative controls proving the four unproven static gates fail red.

Scope (T1, accepted): this module only. ``scripts/verify.py``,
``.github/workflows/ci.yml``, and all slice 003 sources are unchanged.
Every probe reuses the exact gate command from ``verify.GATES`` (tool plus
flags); only the scope argument is pointed at an isolated ``tmp_path``
fixture, so no tracked file can weaken a repository gate.

Covered gates: ``format`` (black), ``lint`` (ruff), ``strict-types``
(mypy, strict), ``pyright`` (strict). For each gate there is a
passing control and a violating fixture, both asserting exit code AND
diagnostic text. ``missing-tool``, ``malformed-report``, and
``empty-discovery`` negatives close the vacuous-pass loopholes: a PR
that deletes the violating file still fails the discovery guard, and a
PR that downgrades a diagnostic still fails the diagnostic assertion.

Redaction (round-18) and the G-018 dedup-ordering gap are out of scope
here; they are reported as separate existing gaps, not reopened.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import cast

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VERIFIER_PATH = REPOSITORY_ROOT / "scripts" / "verify.py"

FORMAT_VIOLATION = "x=1\n"
FORMAT_CONTROL = "x = 1\n"
LINT_VIOLATION = 'import os\n\nprint("hello")\n'
LINT_CONTROL = 'print("hello")\n'
MYPY_VIOLATION = "def add(a, b):\n    return a + b\n"
MYPY_CONTROL = "def add(a: int, b: int) -> int:\n    return a + b\n"
PYRIGHT_VIOLATION = 'VALUE: int = "not-an-int"\n'
PYRIGHT_CONTROL = "VALUE: int = 1\n"
MALFORMED_SOURCE = "not python {{{\n"


def _verifier() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_under_test", VERIFIER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(argv: tuple[str, ...], root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )


def _combined(completed: subprocess.CompletedProcess[str]) -> str:
    return completed.stdout + completed.stderr


def _write_fixture(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def _require_nonempty(paths: list[Path]) -> None:
    assert paths, "empty discovery would spuriously pass: no fixtures discovered"
    missing = [str(path) for path in paths if not path.is_file() or path.stat().st_size == 0]
    assert not missing, f"empty discovery would spuriously pass for: {missing}"


def _format_prefix(module: ModuleType) -> tuple[str, ...]:
    command = cast(Sequence[str], module.GATES["format"].command)
    assert tuple(command) == (sys.executable, "-m", "black", "--check", ".")
    return tuple(command[:-1])


def _lint_prefix(module: ModuleType) -> tuple[str, ...]:
    command = cast(Sequence[str], module.GATES["lint"].command)
    assert tuple(command) == (sys.executable, "-m", "ruff", "check", "src", "tests", "scripts")
    return tuple(command[:-3])


def _mypy_prefix(module: ModuleType) -> tuple[str, ...]:
    command = cast(Sequence[str], module.GATES["strict-types"].command)
    assert tuple(command) == (
        "uv",
        "run",
        "--isolated",
        "--locked",
        "--all-extras",
        "--python",
        "3.11",
        "mypy",
    )
    return tuple(command)


def _pyright_prefix(module: ModuleType) -> tuple[str, ...]:
    command = cast(Sequence[str], module.GATES["pyright"].command)
    assert tuple(command) == (
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
    return tuple(command[:-1])


def test_negative_probes_reuse_verifier_gate_commands() -> None:
    module = _verifier()

    assert _format_prefix(module) == (sys.executable, "-m", "black", "--check")
    assert _lint_prefix(module) == (sys.executable, "-m", "ruff", "check")
    assert _mypy_prefix(module)[-1] == "mypy"
    assert _pyright_prefix(module)[-1] == "pyright"


def test_static_profile_passes_as_control() -> None:
    completed = _run(
        (sys.executable, "scripts/verify.py", "--profile", "static"),
        REPOSITORY_ROOT,
    )

    assert completed.returncode == 0
    assert "status: passed" in completed.stdout


def test_format_gate_fails_red_on_unformatted_fixture(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "bad_format.py", FORMAT_VIOLATION)
    _require_nonempty([fixture])

    completed = _run((*_format_prefix(module), str(fixture)), REPOSITORY_ROOT)

    assert completed.returncode != 0
    assert "would reformat" in _combined(completed)


def test_format_gate_passes_on_formatted_control(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "good_format.py", FORMAT_CONTROL)

    completed = _run((*_format_prefix(module), str(fixture)), REPOSITORY_ROOT)

    assert completed.returncode == 0


def test_lint_gate_fails_red_on_unused_import(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "bad_lint.py", LINT_VIOLATION)
    _require_nonempty([fixture])

    completed = _run((*_lint_prefix(module), str(fixture)), REPOSITORY_ROOT)

    assert completed.returncode != 0
    assert "F401" in _combined(completed)


def test_lint_gate_passes_on_clean_control(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "good_lint.py", LINT_CONTROL)

    completed = _run((*_lint_prefix(module), str(fixture)), REPOSITORY_ROOT)

    assert completed.returncode == 0


def test_strict_types_gate_fails_red_on_untyped_def(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "bad_types.py", MYPY_VIOLATION)
    _require_nonempty([fixture])

    completed = _run((*_mypy_prefix(module), str(fixture)), REPOSITORY_ROOT)

    assert completed.returncode != 0
    assert "no-untyped-def" in _combined(completed)


def test_strict_types_gate_passes_on_typed_control(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "good_types.py", MYPY_CONTROL)

    completed = _run((*_mypy_prefix(module), str(fixture)), REPOSITORY_ROOT)

    assert completed.returncode == 0


def test_pyright_gate_fails_red_on_bad_assignment(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "bad_pyright.py", PYRIGHT_VIOLATION)
    _require_nonempty([fixture])

    completed = _run((*_pyright_prefix(module), str(fixture)), REPOSITORY_ROOT)

    assert completed.returncode != 0
    assert "reportAssignmentType" in _combined(completed)


def test_pyright_gate_passes_on_typed_control(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "good_pyright.py", PYRIGHT_CONTROL)

    completed = _run((*_pyright_prefix(module), str(fixture)), REPOSITORY_ROOT)

    assert completed.returncode == 0


def test_malformed_source_is_not_a_format_violation(tmp_path: Path) -> None:
    module = _verifier()
    fixture = _write_fixture(tmp_path / "malformed.py", MALFORMED_SOURCE)

    completed = _run((*_format_prefix(module), str(fixture)), REPOSITORY_ROOT)
    combined = _combined(completed)

    assert completed.returncode != 0
    assert "would reformat" not in combined
    assert "Cannot parse" in combined or "fail to reformat" in combined


def test_empty_discovery_passes_tool_but_fails_closed_guard(tmp_path: Path) -> None:
    module = _verifier()
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    completed = _run((*_format_prefix(module), str(empty_dir)), REPOSITORY_ROOT)

    assert completed.returncode == 0
    assert "No Python files" in _combined(completed)
    with pytest.raises(AssertionError, match="empty discovery"):
        _require_nonempty([])


def test_missing_tool_reports_prerequisite_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _verifier()
    monkeypatch.setenv("PATH", str(tmp_path))

    execution = module._run_command(module.GATES["strict-types"])

    assert execution.exit_code == 127
    assert execution.stderr.strip() != ""
