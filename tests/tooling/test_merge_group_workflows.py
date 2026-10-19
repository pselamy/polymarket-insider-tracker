"""Every workflow must keep the merge_group trigger the GitHub merge queue depends on."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW_DIRECTORY = Path(__file__).parents[2] / ".github" / "workflows"
WORKFLOW_FILES = ("ci.yml", "relentless-ci.yml", "mutation.yml")


def _triggers(name: str) -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load((WORKFLOW_DIRECTORY / name).read_text("utf-8"))
    # PyYAML parses the bare key `on` as boolean True.
    triggers: dict[str, Any] = loaded[True] if True in loaded else loaded["on"]
    return triggers


def test_every_workflow_file_is_covered_by_the_merge_group_contract() -> None:
    present = {path.name for path in WORKFLOW_DIRECTORY.glob("*.yml")}
    assert present == set(WORKFLOW_FILES)


@pytest.mark.parametrize("name", WORKFLOW_FILES)
def test_workflow_runs_on_merge_group(name: str) -> None:
    assert _triggers(name)["merge_group"] == {"types": ["checks_requested"]}


def test_ci_never_cancels_merge_queue_runs() -> None:
    loaded: dict[str, Any] = yaml.safe_load((WORKFLOW_DIRECTORY / "ci.yml").read_text("utf-8"))
    assert (
        loaded["concurrency"]["cancel-in-progress"] == "${{ github.event_name != 'merge_group' }}"
    )
