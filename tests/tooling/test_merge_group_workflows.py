"""Workflow trigger and concurrency contracts shared by every workflow.

Every workflow keeps the merge_group trigger the GitHub merge queue depends on, and only the
latest commit of a pull request runs CI: pull-request runs are grouped per pull request and cancel
in progress, while pushes to main, merge-queue entries and manual runs are grouped per commit and
never cancelled (docs/relentless-ci-adoption.md, "CI run cancellation").
"""

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
    loader = yaml.BaseLoader(text)
    try:
        loaded: dict[str, Any] = loader.get_single_data()
    finally:
        loader.dispose()
    return loaded


def test_every_workflow_file_is_covered_by_the_merge_group_contract() -> None:
    present = {path.name for path in WORKFLOW_DIRECTORY.glob("*.yml")}
    assert present == set(WORKFLOW_FILES)


@pytest.mark.parametrize("name", WORKFLOW_FILES)
def test_workflow_runs_on_merge_group(name: str) -> None:
    assert _load(name)["on"]["merge_group"] == {"types": ["checks_requested"]}


EXPECTED_CONCURRENCY = {
    "group": (
        "${{ github.workflow }}-${{ github.event_name == 'pull_request' "
        "&& format('pr-{0}', github.event.pull_request.number) "
        "|| format('{0}-{1}', github.event_name, github.sha) }}"
    ),
    # Only pull_request runs cancel in progress, so merge-queue runs are never cancelled.
    "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
}


@pytest.mark.parametrize("name", WORKFLOW_FILES)
def test_only_the_latest_pull_request_commit_runs(name: str) -> None:
    assert _load(name)["concurrency"] == EXPECTED_CONCURRENCY


@pytest.mark.parametrize("name", WORKFLOW_FILES)
def test_push_runs_only_on_main(name: str) -> None:
    # A push to a pull-request branch would otherwise start a second, uncancellable run next to
    # the pull_request run for the same commit.
    assert _load(name)["on"]["push"] == {"branches": ["main"]}
