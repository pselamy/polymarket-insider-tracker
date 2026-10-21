"""Bind the local pre-commit hooks to the commands enrolled in quality/checks.json."""

from __future__ import annotations

import json
import shlex
from pathlib import Path

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).parents[2]
CONFIG_PATH = REPOSITORY_ROOT / ".pre-commit-config.yaml"
CHECKS_PATH = REPOSITORY_ROOT / "quality" / "checks.json"
LOCKED_RUN = ["uv", "run", "--locked", "--all-extras"]
# Tool hooks that must run exactly the command CI runs, under the locked runner.
TOOL_HOOKS = ("black", "ruff", "mypy")


def _repos() -> list[dict[str, object]]:
    config: object = yaml.safe_load(CONFIG_PATH.read_text())
    assert isinstance(config, dict)
    repos = config["repos"]
    assert isinstance(repos, list)
    return repos


def _join(args: object) -> str:
    assert isinstance(args, list)
    return " ".join(map(str, args))


def _runs_in_project_scope(hook: dict[str, object]) -> bool:
    return hook["language"] == "system" and hook["pass_filenames"] is False


def _hook_commands() -> dict[str, str]:
    hooks: list[dict[str, object]] = []
    for repo in _repos():
        if repo["repo"] == "local":
            assert isinstance(repo["hooks"], list)
            hooks += repo["hooks"]
    assert all(_runs_in_project_scope(hook) for hook in hooks)
    return {str(h["id"]): f"{h['entry']} {_join(h['args'])}" for h in hooks}


def _ci_command(tool: str) -> list[str]:
    enrolled: list[list[str]] = json.loads(CHECKS_PATH.read_text())
    commands = [c for c in enrolled if tool in c]
    assert len(commands) == 1, f"expected exactly one enrolled {tool} command"
    return commands[0]


def _expected(tool: str) -> list[str]:
    ci = _ci_command(tool)
    return ci if ci[0] == "uv" else LOCKED_RUN + ci


def _drifted(commands: dict[str, str]) -> list[str]:
    return [t for t in TOOL_HOOKS if shlex.split(commands[t]) != _expected(t)]


def test_local_hooks_run_the_enrolled_ci_commands() -> None:
    assert _drifted(_hook_commands()) == []


@pytest.mark.parametrize("tool", TOOL_HOOKS)
@pytest.mark.parametrize("plant", ["{} extra_scope", "{} --fix", "ruff"])
def test_planted_mismatch_is_detected(tool: str, plant: str) -> None:
    commands = _hook_commands()
    commands[tool] = plant.format(commands[tool])
    assert _drifted(commands) == [tool]


def test_no_remote_repo_pins_a_tool_the_project_locks() -> None:
    remotes = " ".join(str(r["repo"]) for r in _repos() if r["repo"] != "local")
    assert not any(tool in remotes for tool in ("ruff", "black", "mypy"))
