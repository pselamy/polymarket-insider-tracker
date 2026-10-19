"""Every workflow must keep the merge_group trigger the GitHub merge queue depends on."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW_DIRECTORY = Path(__file__).parents[2] / ".github" / "workflows"
WORKFLOW_FILES = ("ci.yml", "relentless-ci.yml", "mutation.yml")


def _load(name: str) -> dict[str, Any]:
    # BaseLoader keeps the bare key `on` a string instead of resolving it to boolean True.
    text = (WORKFLOW_DIRECTORY / name).read_text("utf-8")
    loaded: dict[str, Any] = yaml.load(text, Loader=yaml.BaseLoader)
    return loaded


def test_every_workflow_file_is_covered_by_the_merge_group_contract() -> None:
    present = {path.name for path in WORKFLOW_DIRECTORY.glob("*.yml")}
    assert present == set(WORKFLOW_FILES)


@pytest.mark.parametrize("name", WORKFLOW_FILES)
def test_workflow_runs_on_merge_group(name: str) -> None:
    assert _load(name)["on"]["merge_group"] == {"types": ["checks_requested"]}


def test_ci_never_cancels_merge_queue_runs() -> None:
    loaded: dict[str, Any] = _load("ci.yml")
    assert (
        loaded["concurrency"]["cancel-in-progress"] == "${{ github.event_name != 'merge_group' }}"
    )
