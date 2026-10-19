"""Resolve a trusted trigger against current native PR and workflow state."""

from dataclasses import dataclass

from quality.trusted_policy.execution_policy import associated_run
from quality.trusted_policy.fork_run import unique_pull
from quality.trusted_policy.metadata_collector import Policy, repository_route
from quality.trusted_policy.metadata_pages import ReadAPI, object_inventory
from quality.trusted_policy.review_policy import (
    PolicyFailure,
    digest,
    identifier,
    record,
    same_text,
)
from quality.trusted_policy.trusted_event import Trigger


@dataclass(frozen=True)
class ReviewedPolicy:
    repository: str
    repository_id: int
    base: str
    workflow_id: int
    required_names: frozenset[str]


def current_candidate(api: ReadAPI, number: int, reviewed: ReviewedPolicy) -> dict[str, object]:
    root = repository_route(reviewed.repository)
    repository = record(api(root))
    branch = record(api(root + "/branches/main"))
    if (
        identifier(repository["id"]) != identifier(reviewed.repository_id)
        or repository["full_name"] != reviewed.repository
        or not same_text(branch["name"], "main")
        or branch["protected"] is not True
    ):
        raise PolicyFailure("trusted repository or protected base differs")
    base = digest(record(branch["commit"])["sha"])
    if base != digest(reviewed.base):
        raise PolicyFailure("reviewed policy base is no longer current")
    pull = record(api(root + "/pulls/" + str(identifier(number))))
    if digest(record(pull["base"])["sha"]) != base:
        raise PolicyFailure("candidate base differs from reviewed policy")
    return pull


def recent_run(
    api: ReadAPI,
    reviewed: ReviewedPolicy,
    pull: dict[str, object],
    number: int,
    head: str,
) -> int:
    root = repository_route(reviewed.repository)
    endpoint = root + "/actions/workflows/" + str(identifier(reviewed.workflow_id)) + "/runs"
    _, runs = object_inventory(api, endpoint, "workflow_runs")
    matches = [
        identity for run in runs if (identity := matching_run(run, reviewed, pull, number, head))
    ]
    if not matches:
        raise PolicyFailure("no successful native run associated with current PR head")
    return max(matches)


def matching_run(
    value: object,
    reviewed: ReviewedPolicy,
    pull: dict[str, object],
    number: int,
    head: str,
) -> int:
    run = record(value)
    if (
        not same_text(run["event"], "pull_request")
        or not same_text(run["status"], "completed")
        or not same_text(run["conclusion"], "success")
        or run["head_sha"] != head
        or identifier(run["workflow_id"]) != reviewed.workflow_id
        or not associated_run(run, pull, number, head, reviewed.base)
    ):
        return 0
    return identifier(run["id"])


def resolve(api: ReadAPI, trigger: Trigger, reviewed: ReviewedPolicy) -> Policy:
    if (
        trigger.repository != reviewed.repository
        or identifier(trigger.repository_id) != identifier(reviewed.repository_id)
        or not reviewed.required_names
    ):
        raise PolicyFailure("trigger differs from reviewed repository or matrix")
    number = trigger.pull_number
    if number is None:
        run_id = identifier(trigger.run_id)
        root = repository_route(reviewed.repository)
        run = api(root + "/actions/runs/" + str(run_id))
        number = unique_pull(api, root, run, run_id, reviewed.workflow_id, reviewed.base)
    pull = current_candidate(api, number, reviewed)
    head = digest(record(pull["head"])["sha"])
    run_id = (
        identifier(trigger.run_id)
        if same_text(trigger.kind, "completed_run")
        else recent_run(api, reviewed, pull, number, head)
    )
    return Policy(
        reviewed.repository,
        number,
        head,
        reviewed.base,
        reviewed.workflow_id,
        run_id,
        reviewed.required_names,
    )
