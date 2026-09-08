"""Bind the CI workflow to the canonical verifier and prove its aggregator fails closed.

These tests read the real workflow file and execute its real aggregator script, so the workflow
cannot drift from the verifier or from the documented fail-closed contract without failing here.
"""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).parents[2]
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml"
VERIFIER_PATH = REPOSITORY_ROOT / "scripts" / "verify.py"

# Every job the protected aggregator must depend on, in workflow order.
BLOCKING_JOBS = ("static", "vulture", "compatibility", "services")
NON_SUCCESS_RESULTS = ("failure", "cancelled", "skipped")
NEEDS_RESULT_EXPRESSION = re.compile(r"^\$\{\{ needs\.(?P<job>[A-Za-z0-9_-]+)\.result \}\}$")
# GitHub runs `run:` steps on Linux with `bash --noprofile --norc -eo pipefail {0}`.
GITHUB_BASH = ("bash", "--noprofile", "--norc", "-eo", "pipefail", "-c")


def _workflow() -> dict[str, Any]:
    with WORKFLOW_PATH.open(encoding="utf-8") as handle:
        loaded: dict[str, Any] = yaml.safe_load(handle)
    return loaded


def _verifier() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_for_ci_contract", VERIFIER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_steps(job: Mapping[str, Any]) -> list[str]:
    return [step["run"].strip() for step in job["steps"] if "run" in step]


def _required_step() -> dict[str, Any]:
    required = _workflow()["jobs"]["required"]
    assert len(required["steps"]) == 1
    step: dict[str, Any] = required["steps"][0]
    return step


def _matched_job(expression: str) -> str:
    match = NEEDS_RESULT_EXPRESSION.match(expression)
    assert match is not None, f"{expression} is not bound to a needs.<job>.result expression"
    return match.group("job")


def _run_required_script(results: Mapping[str, str]) -> int:
    """Execute the real aggregator script with simulated predecessor results."""
    step = _required_step()
    environment = {"PATH": os.environ["PATH"]}
    for name, expression in step["env"].items():
        environment[name] = results[_matched_job(expression)]
    completed = subprocess.run(
        [*GITHUB_BASH, step["run"]],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )
    return completed.returncode


def test_every_blocking_job_exists_and_is_not_advisory() -> None:
    jobs = _workflow()["jobs"]

    for job_id in BLOCKING_JOBS:
        assert job_id in jobs, f"{job_id} job missing from CI"
        assert "continue-on-error" not in jobs[job_id]
        assert jobs[job_id]["runs-on"] == "ubuntu-24.04"


def test_static_job_runs_the_verifier_static_profile_that_includes_vulture() -> None:
    verifier = _verifier()

    assert "vulture" in verifier.gate_ids_for_profile("static")
    assert _run_steps(_workflow()["jobs"]["static"])[-1] == (
        "uv run python scripts/verify.py --profile static"
    )


CANONICAL_VULTURE_SCOPE = ("src", "tests", "scripts", "alembic", "conftest.py")


def test_vulture_job_is_independent_and_runs_the_canonical_gate_command() -> None:
    verifier = _verifier()
    job = _workflow()["jobs"]["vulture"]
    run_steps = _run_steps(job)

    assert "needs" not in job and "if" not in job, "the Vulture job must not be gated by others"
    assert run_steps[-1] == verifier.GATES["vulture"].command_text
    assert run_steps[-1] == dict(verifier.DIRECT_GATE_COMMANDS)["vulture"]
    assert run_steps[-1].endswith("vulture " + " ".join(CANONICAL_VULTURE_SCOPE))
    assert "uv sync --locked --all-extras --python 3.11" in run_steps
    setup_uv_steps = [
        step for step in job["steps"] if "astral-sh/setup-uv@" in step.get("uses", "")
    ]
    assert len(setup_uv_steps) == 1
    assert setup_uv_steps[0]["with"]["cache-suffix"] == "vulture-3.11"


def test_vulture_scope_matches_between_ci_pyproject_and_verifier() -> None:
    import tomllib

    verifier = _verifier()
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        pyproject_data = tomllib.load(handle)

    pyproject_paths = tuple(pyproject_data["tool"]["vulture"]["paths"])
    verifier_command = verifier.GATES["vulture"].command
    vulture_index = verifier_command.index("vulture")
    verifier_scope = verifier_command[vulture_index + 1 :]
    direct_command = dict(verifier.DIRECT_GATE_COMMANDS)["vulture"]
    ci_step = _run_steps(_workflow()["jobs"]["vulture"])[-1]

    assert pyproject_paths == CANONICAL_VULTURE_SCOPE
    assert verifier_scope == CANONICAL_VULTURE_SCOPE
    assert (
        direct_command
        == f"uv run --isolated --locked --all-extras --python 3.11 vulture {' '.join(CANONICAL_VULTURE_SCOPE)}"
    )
    assert ci_step == direct_command


def test_required_aggregator_always_runs_and_binds_every_blocking_job_in_order() -> None:
    required = _workflow()["jobs"]["required"]
    step = _required_step()

    assert required["if"] == "always()"
    assert tuple(required["needs"]) == BLOCKING_JOBS
    bound_jobs = [_matched_job(expression) for expression in step["env"].values()]
    assert tuple(bound_jobs) == BLOCKING_JOBS
    for variable in step["env"]:
        assert f'test "${variable}" = success' in step["run"]


def test_required_script_passes_only_when_every_blocking_job_succeeded() -> None:
    assert _run_required_script(dict.fromkeys(BLOCKING_JOBS, "success")) == 0


@pytest.mark.parametrize("result", NON_SUCCESS_RESULTS)
@pytest.mark.parametrize("job_id", BLOCKING_JOBS)
def test_required_script_fails_closed_for_each_non_success_result(job_id: str, result: str) -> None:
    results = dict.fromkeys(BLOCKING_JOBS, "success")
    results[job_id] = result

    assert _run_required_script(results) != 0
