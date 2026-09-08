"""Contract tests for the Complexipy cognitive complexity gate.

These tests prove:
1. Every tracked repository Python file maps to exactly one configured scope entry.
2. No inline suppression comments (# complexipy: ignore or # noqa: complexipy) exist in any tracked file.
3. No forbidden escape-hatch configuration exists in pyproject.toml or external config files.
4. The configured Complexipy command exits nonzero on complexity > 5 and exits zero at the boundary (<= 5).
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
import tomllib
from collections.abc import Sequence
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_SCOPE = ("src", "tests", "scripts", "alembic", "conftest.py")
CANONICAL_COMMAND = (
    "uv",
    "run",
    "--isolated",
    "--locked",
    "--all-extras",
    "--python",
    "3.11",
    "complexipy",
)

# Construct markers without embedding literal suppression comments in this source file
SUPPRESSION_PATTERNS = (
    re.compile(r"#\s*" + r"complexipy\s*:\s*ignore", re.IGNORECASE),
    re.compile(r"#\s*" + r"noqa\s*:\s*complexipy", re.IGNORECASE),
)

ALLOWED_CONFIG_KEYS = frozenset({"paths", "max-complexity-allowed", "no-ignore"})


def _match_scope_entry(file_path: str, scope: Sequence[str]) -> str:
    matches = [entry for entry in scope if file_path == entry or file_path.startswith(f"{entry}/")]
    assert (
        len(matches) == 1
    ), f"tracked file {file_path} must match exactly one entry, got {matches}"
    return matches[0]


def _tracked_python_files() -> list[str]:
    completed = subprocess.run(
        ["git", "ls-files", "*.py"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    files = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    assert files, "expected tracked python files in repository"
    return files


def test_all_tracked_python_files_are_covered_by_complexipy_scope() -> None:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    scope = tuple(pyproject["tool"]["complexipy"]["paths"])
    assert scope == CANONICAL_SCOPE

    tracked_files = _tracked_python_files()
    covered = {_match_scope_entry(path, scope) for path in tracked_files}
    for entry in scope:
        assert entry in covered, f"scope entry {entry} covers no tracked files"


def _line_has_suppression(line: str) -> bool:
    return any(pattern.search(line) for pattern in SUPPRESSION_PATTERNS)


def _check_file_for_suppression(file_path: Path) -> list[str]:
    content = file_path.read_text(encoding="utf-8")
    violations: list[str] = []
    for line_num, line in enumerate(content.splitlines(), start=1):
        if _line_has_suppression(line):
            violations.append(f"{file_path}:{line_num}: {line.strip()}")
    return violations


def test_no_inline_suppression_comments_in_tracked_files() -> None:
    violations: list[str] = []
    for relative_path in _tracked_python_files():
        full_path = REPOSITORY_ROOT / relative_path
        violations.extend(_check_file_for_suppression(full_path))

    assert not violations, "found forbidden inline suppression comments:\n" + "\n".join(violations)


def test_suppression_pattern_detection() -> None:
    marker1 = "# " + "complexipy: ignore"
    marker2 = "# " + "noqa: complexipy"
    assert any(p.search(f"def foo(): {marker1}") for p in SUPPRESSION_PATTERNS)
    assert any(p.search(f"def bar(): {marker2}") for p in SUPPRESSION_PATTERNS)


def test_no_forbidden_complexipy_escape_hatches() -> None:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)

    complexipy_config = pyproject["tool"]["complexipy"]
    actual_keys = set(complexipy_config.keys())
    assert (
        actual_keys == ALLOWED_CONFIG_KEYS
    ), f"forbidden or unexpected keys in [tool.complexipy]: {actual_keys - ALLOWED_CONFIG_KEYS}"

    assert complexipy_config["max-complexity-allowed"] == 5
    assert complexipy_config["no-ignore"] is True
    assert tuple(complexipy_config["paths"]) == CANONICAL_SCOPE

    assert not (REPOSITORY_ROOT / ".complexipy.toml").exists()
    assert not (REPOSITORY_ROOT / "complexipy.toml").exists()


def _run_complexipy_on_code(code: str) -> int:
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(code)
        temp_path = f.name
    try:
        completed = subprocess.run(
            [*CANONICAL_COMMAND, temp_path],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        return completed.returncode
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def test_complexipy_exits_nonzero_on_score_6_function() -> None:
    score_6_code = (
        "def sample_func(x: int) -> int:\n"
        "    if x > 1: return 1\n"
        "    if x > 2: return 2\n"
        "    if x > 3: return 3\n"
        "    if x > 4: return 4\n"
        "    if x > 5: return 5\n"
        "    if x > 6: return 6\n"
        "    return 0\n"
    )
    exit_code = _run_complexipy_on_code(score_6_code)
    assert exit_code != 0, f"expected nonzero exit, got {exit_code}"


def test_complexipy_exits_zero_on_score_5_boundary_function() -> None:
    score_5_code = (
        "def sample_func(x: int) -> int:\n"
        "    if x > 1: return 1\n"
        "    if x > 2: return 2\n"
        "    if x > 3: return 3\n"
        "    if x > 4: return 4\n"
        "    if x > 5: return 5\n"
        "    return 0\n"
    )
    exit_code = _run_complexipy_on_code(score_5_code)
    assert exit_code == 0, f"expected zero exit, got {exit_code}"


def test_complexipy_no_ignore_prevents_inline_suppression_on_score_6() -> None:
    marker = "# " + "complexipy: ignore"
    score_6_suppressed = (
        f"def sample_suppressed(x: int) -> int:  {marker}\n"
        "    if x > 1: return 1\n"
        "    if x > 2: return 2\n"
        "    if x > 3: return 3\n"
        "    if x > 4: return 4\n"
        "    if x > 5: return 5\n"
        "    if x > 6: return 6\n"
        "    return 0\n"
    )
    exit_code = _run_complexipy_on_code(score_6_suppressed)
    assert exit_code != 0, f"expected nonzero exit even with suppression comment, got {exit_code}"
