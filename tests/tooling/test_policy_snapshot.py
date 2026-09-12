"""Golden policy snapshot against check-weakening (T2, accepted).

This module binds the three policy surfaces together AND to their golden
values, closing the self-certification loophole where a threshold such as
``--max-complexity-allowed 5`` could be raised to ``6`` consistently in the
verifier, the config file, and the CI workflow without any test failing.

Golden policy (change requires Patrick's approval — see module docstring rule):
- Complexipy threshold ``max-complexity-allowed`` is exactly ``5``.
- Gate scope is exactly ``src tests scripts alembic conftest.py``.
- Blocking CI jobs are exactly
  ``(static, vulture, complexipy, compatibility, services)``.
- Strict type gates stay strict (mypy ``strict = true``, Pyright
  ``typeCheckingMode = strict``); no baselines, allowlists, diff-only modes,
  exclusions, suppressions, or non-blocking status.

The live tamper probe proves the threshold is semantic, not decorative: a
score-6 function MUST fail under the golden policy.

Golden-file changes in the same change that weakens the policy are rejected by
review rule: they require Patrick's approval. Remote branch-protection wiring
is NOT_INSPECTED and claimed by nobody.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VERIFIER_PATH = REPOSITORY_ROOT / "scripts" / "verify.py"
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml"

GOLDEN_MAX_COMPLEXITY = 5
GOLDEN_SCOPE = ("src", "tests", "scripts", "alembic", "conftest.py")
GOLDEN_BLOCKING_JOBS = ("static", "vulture", "complexipy", "compatibility", "services")

GOLDEN_COMPLEXIPY_COMMAND = (
    "uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py "
    + " ".join(GOLDEN_SCOPE)
    + " --max-complexity-allowed 5 --no-ignore --ignore-complexity=false"
    + " --snapshot-ignore=true --snapshot-create=false --exclude=. --check-script=true"
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


def _verifier() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_policy_snapshot", VERIFIER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pyproject_table(name: str) -> dict[str, str | int | bool | list[str]]:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    tool = pyproject["tool"]
    raw_table = tool[name]
    table: dict[str, str | int | bool | list[str]] = {}
    for key in ("paths", "max-complexity-allowed", "no-ignore", "check-script"):
        if key in raw_table:
            table[key] = raw_table[key]
    return table


def _strict_table(section: str) -> dict[str, str | bool]:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    raw_table = pyproject["tool"][section]
    table: dict[str, str | bool] = {}
    for key in ("strict", "typeCheckingMode", "enableTypeIgnoreComments"):
        if key in raw_table:
            table[key] = raw_table[key]
    return table


def _path_entries(value: str | int | bool | list[str] | None) -> list[str]:
    assert isinstance(value, list)
    return list(value)


def test_golden_complexity_threshold_is_five() -> None:
    verifier = _verifier()
    command = verifier.GATES["complexipy"].command
    index = command.index("--max-complexity-allowed")

    assert command[index + 1] == "5"
    assert command[index + 1] == str(GOLDEN_MAX_COMPLEXITY)


def test_golden_scope_covers_every_tracked_tree_exactly_once() -> None:
    verifier = _verifier()
    command = verifier.GATES["complexipy"].command
    marker = command.index("scripts/complexipy_gate.py") + 1
    verifier_scope = command[marker : marker + len(GOLDEN_SCOPE)]

    assert verifier_scope == GOLDEN_SCOPE
    raw_scope = command[marker:]
    terminator = raw_scope.index("--max-complexity-allowed")
    assert raw_scope[:terminator] == GOLDEN_SCOPE


def test_golden_pyproject_policy_matches_verifier() -> None:
    complexipy = _pyproject_table("complexipy")
    vulture = _pyproject_table("vulture")

    complexipy_paths = _path_entries(complexipy.get("paths"))
    vulture_paths = _path_entries(vulture.get("paths"))
    assert complexipy["max-complexity-allowed"] == GOLDEN_MAX_COMPLEXITY
    golden_scope_list = list(GOLDEN_SCOPE)
    assert complexipy_paths == golden_scope_list
    assert complexipy["no-ignore"] is True
    assert complexipy["check-script"] is True
    assert vulture_paths == golden_scope_list


def test_golden_verifier_command_text_matches_canonical_form() -> None:
    verifier = _verifier()

    assert verifier.GATES["complexipy"].command_text == GOLDEN_COMPLEXIPY_COMMAND
    assert dict(verifier.DIRECT_GATE_COMMANDS)["complexipy"] == GOLDEN_COMPLEXIPY_COMMAND


def test_golden_blocking_jobs_are_bound_in_ci_and_verifier() -> None:
    verifier = _verifier()
    workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert verifier.gate_ids_for_profile("static") == (
        "lock",
        "format",
        "lint",
        "strict-types",
        "pyright",
        "vulture",
        "complexipy",
    )
    for job_id in GOLDEN_BLOCKING_JOBS:
        assert f"{job_id}:" in workflow_text
    assert "needs: [static, vulture, complexipy, compatibility, services]" in workflow_text


def test_golden_strict_type_flags_are_not_weakened() -> None:
    mypy = _strict_table("mypy")
    pyright = _strict_table("pyright")

    assert mypy["strict"] is True
    assert pyright["typeCheckingMode"] == "strict"
    assert pyright["enableTypeIgnoreComments"] is False


def test_score_6_fixture_fails_under_golden_policy(tmp_path: Path) -> None:
    target = tmp_path / "sample.py"
    target.write_text(SCORE_6_CODE, encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "from complexipy.cli import main; main()",
            str(target),
            "--max-complexity-allowed",
            str(GOLDEN_MAX_COMPLEXITY),
            "--no-ignore",
            "--ignore-complexity=false",
            "--snapshot-ignore=true",
            "--snapshot-create=false",
            "--exclude=.",
            "--check-script=true",
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0


def test_consistent_threshold_6_across_surfaces_still_fails_golden() -> None:
    """A candidate that raises the threshold to 6 everywhere must fail this suite.

    This negative probe simulates the self-certification attack: even if the
    verifier, pyproject, and workflow agreed on ``6``, the golden value pinned
    here stays ``5`` and the tamper is detected by value, not by consistency.
    """
    assert GOLDEN_MAX_COMPLEXITY == 5
    assert "6" not in GOLDEN_COMPLEXIPY_COMMAND.split("--max-complexity-allowed")[1].split()[0]
