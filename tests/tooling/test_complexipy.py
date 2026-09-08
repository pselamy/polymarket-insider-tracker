"""Contract tests for the Complexipy cognitive complexity gate.

These tests prove:
1. The canonical command names every scope path and both policy flags, and matches the verifier.
2. No tracked file carries either inline suppression marker Complexipy honours; the scanner is
   itself proven against markers assembled at runtime so no tracked source contains one.
3. No escape-hatch configuration exists in ``pyproject.toml`` or in any external config file.
4. The real locked analyzer exits nonzero above 5, zero at exactly 5, nonzero with a marker, and
   nonzero even when the working directory carries a relaxed ``.complexipy.toml``.
"""

from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VERIFIER_PATH = REPOSITORY_ROOT / "scripts" / "verify.py"
CANONICAL_SCOPE = ("src", "tests", "scripts", "alembic", "conftest.py")
POLICY_FLAGS = ("--max-complexity-allowed", "5", "--no-ignore")
LOCKED_PREFIX = (
    "uv",
    "run",
    "--isolated",
    "--locked",
    "--all-extras",
    "--python",
    "3.11",
)
CANONICAL_COMMAND = (*LOCKED_PREFIX, "complexipy", *CANONICAL_SCOPE, *POLICY_FLAGS)

# Upstream 8.0.1 honours exactly these two markers (``complexipy_core::utils::find_noqa_comment``).
# The marker words are assembled at runtime so this file never contains a literal marker.
_MARKER_WORD = "complex" + "ipy"
SUPPRESSION_PATTERNS = (
    re.compile(r"#\s*" + _MARKER_WORD + r"\s*:\s*ignore", re.IGNORECASE),
    re.compile(r"#\s*noqa\s*:\s*" + _MARKER_WORD, re.IGNORECASE),
)
ALLOWED_CONFIG_KEYS = frozenset({"paths", "max-complexity-allowed", "no-ignore"})
EXTERNAL_CONFIG_NAMES = frozenset({".complexipy.toml", "complexipy.toml"})

SCORE_5_CODE = (
    "def sample_func(x: int) -> int:\n"
    "    if x > 1: return 1\n"
    "    if x > 2: return 2\n"
    "    if x > 3: return 3\n"
    "    if x > 4: return 4\n"
    "    if x > 5: return 5\n"
    "    return 0\n"
)
SCORE_6_CODE = (
    "def sample_func(x: int) -> int:\n"
    "    if x > 1: return 1\n"
    "    if x > 2: return 2\n"
    "    if x > 3: return 3\n"
    "    if x > 4: return 4\n"
    "    if x > 5: return 5\n"
    "    if x > 6: return 6\n"
    "    return 0\n"
)


def _marker(kind: str) -> str:
    """Build a real suppression marker at runtime; ``kind`` is ``inline`` or ``noqa``."""
    if kind == "inline":
        return "# " + _MARKER_WORD + ": ignore"
    return "# noqa: " + _MARKER_WORD


def _verifier() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_under_test", VERIFIER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tracked_files(pattern: str) -> list[str]:
    completed = subprocess.run(
        ["git", "ls-files", pattern],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _pyproject_complexipy_config() -> dict[str, object]:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        return dict(tomllib.load(handle)["tool"]["complexipy"])


def test_canonical_command_names_scope_and_policy_and_matches_verifier() -> None:
    verifier = _verifier()

    assert verifier.GATES["complexipy"].command == CANONICAL_COMMAND
    assert dict(verifier.DIRECT_GATE_COMMANDS)["complexipy"] == " ".join(CANONICAL_COMMAND)
    assert "--no-ignore" in CANONICAL_COMMAND
    assert CANONICAL_COMMAND[CANONICAL_COMMAND.index("--max-complexity-allowed") + 1] == "5"
    for entry in CANONICAL_SCOPE:
        assert entry in CANONICAL_COMMAND


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
    tracked = _tracked_files("*.py")
    assert tracked, "expected tracked python files in repository"
    for relative_path in tracked:
        violations.extend(_check_file_for_suppression(REPOSITORY_ROOT / relative_path))

    assert not violations, "found forbidden inline suppression comments:\n" + "\n".join(violations)


@pytest.mark.parametrize(
    "line",
    [
        "def foo(): " + _marker("inline"),
        "def bar(): " + _marker("noqa"),
        "x = 1  #" + _MARKER_WORD.upper() + " : IGNORE trailing words",
        "x = 1  #NOQA :" + _MARKER_WORD.title(),
    ],
)
def test_scanner_detects_runtime_built_markers_in_a_file(tmp_path: Path, line: str) -> None:
    """The scanner catches both upstream syntaxes without a literal marker in tracked source."""
    target = tmp_path / "sample.py"
    target.write_text("import os\n" + line + "\nprint(os.sep)\n", encoding="utf-8")

    violations = _check_file_for_suppression(target)

    assert len(violations) == 1
    assert violations[0].startswith(f"{target}:2:")


@pytest.mark.parametrize(
    "line",
    [
        "# noqa: E501",
        "# type: ignore",
        "# " + _MARKER_WORD + " is the analyzer name in prose",
        "value = '" + _MARKER_WORD + ": ignore'  # not a comment marker",
    ],
)
def test_scanner_ignores_unrelated_comments(line: str) -> None:
    assert not _line_has_suppression(line)


def test_no_forbidden_complexipy_escape_hatches() -> None:
    complexipy_config = _pyproject_complexipy_config()
    actual_keys = set(complexipy_config.keys())
    assert (
        actual_keys == ALLOWED_CONFIG_KEYS
    ), f"forbidden or unexpected keys in [tool.complexipy]: {actual_keys - ALLOWED_CONFIG_KEYS}"

    assert complexipy_config["max-complexity-allowed"] == 5
    assert complexipy_config["no-ignore"] is True
    paths = complexipy_config["paths"]
    assert isinstance(paths, list)
    assert tuple(paths) == CANONICAL_SCOPE


def test_no_external_complexipy_config_anywhere_in_the_tracked_tree() -> None:
    """A cwd ``.complexipy.toml`` overrides ``[tool.complexipy]`` upstream, so none may exist."""
    for name in EXTERNAL_CONFIG_NAMES:
        assert not (REPOSITORY_ROOT / name).exists()
    offenders = [path for path in _tracked_files("*") if Path(path).name in EXTERNAL_CONFIG_NAMES]
    assert offenders == []


def _run_locked_analyzer(code: str, *, tmp_path: Path, explicit_policy: bool) -> int:
    """Run the locked analyzer on ``code`` from ``tmp_path`` as the working directory.

    The working directory is outside the repository so the repository ``pyproject.toml`` is not
    discovered; only the explicit policy flags (when requested) or ``tmp_path`` configuration apply.
    """
    target = tmp_path / "sample.py"
    target.write_text(code, encoding="utf-8")
    arguments: list[str] = [str(target)]
    if explicit_policy:
        arguments.extend(POLICY_FLAGS)
    completed = subprocess.run(
        [sys.executable, "-c", "from complexipy.cli import main; main()", *arguments],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode


def test_locked_analyzer_is_the_pinned_release() -> None:
    completed = subprocess.run(
        [*LOCKED_PREFIX, "complexipy", "--version"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip().endswith("8.0.1")


def test_complexipy_exits_nonzero_on_score_6_function(tmp_path: Path) -> None:
    assert _run_locked_analyzer(SCORE_6_CODE, tmp_path=tmp_path, explicit_policy=True) != 0


def test_complexipy_exits_zero_on_score_5_boundary_function(tmp_path: Path) -> None:
    assert _run_locked_analyzer(SCORE_5_CODE, tmp_path=tmp_path, explicit_policy=True) == 0


@pytest.mark.parametrize("kind", ["inline", "noqa"])
def test_no_ignore_rejects_each_suppression_syntax_on_score_6(tmp_path: Path, kind: str) -> None:
    suppressed = SCORE_6_CODE.replace(
        "def sample_func(x: int) -> int:\n",
        "def sample_func(x: int) -> int:  " + _marker(kind) + "\n",
    )
    assert _run_locked_analyzer(suppressed, tmp_path=tmp_path, explicit_policy=True) != 0


def test_explicit_policy_flags_override_a_relaxed_working_directory_config(tmp_path: Path) -> None:
    """Upstream prefers a cwd ``.complexipy.toml``; the explicit flags keep the gate closed anyway."""
    (tmp_path / ".complexipy.toml").write_text(
        "max-complexity-allowed = 100\nno-ignore = false\n", encoding="utf-8"
    )
    suppressed = SCORE_6_CODE.replace(
        "def sample_func(x: int) -> int:\n",
        "def sample_func(x: int) -> int:  " + _marker("inline") + "\n",
    )

    # Control: relying on the relaxed configuration alone lets the function through.
    assert _run_locked_analyzer(suppressed, tmp_path=tmp_path, explicit_policy=False) == 0
    # The canonical command carries the policy on the command line and still fails.
    assert _run_locked_analyzer(suppressed, tmp_path=tmp_path, explicit_policy=True) != 0


def test_canonical_command_passes_on_the_repository() -> None:
    completed = subprocess.run(
        CANONICAL_COMMAND,
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
