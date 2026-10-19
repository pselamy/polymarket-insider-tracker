"""Prototype complete native Actions job evidence tied to one candidate workflow."""

from collections.abc import Sequence

from quality.trusted_policy.metadata_pages import sequence
from quality.trusted_policy.review_policy import (
    PolicyFailure,
    digest,
    identifier,
    record,
    same_text,
    text,
)


def require_success(value: object) -> dict[str, object]:
    item = record(value)
    if not same_text(item["status"], "completed") or not same_text(item["conclusion"], "success"):
        raise PolicyFailure("required execution must complete successfully")
    return item


def verify_run(value: object, workflow_id: int, head: str) -> dict[str, object]:
    item = require_success(value)
    if identifier(item["workflow_id"]) != identifier(workflow_id):
        raise PolicyFailure("unrelated workflow cannot supply required evidence")
    if digest(item["head_sha"]) != digest(head):
        raise PolicyFailure("workflow evidence belongs to a different PR head")
    if not same_text(item["event"], "pull_request"):
        raise PolicyFailure("required untrusted-code evidence must be from a PR run")
    return item


def associated_pull(value: object, number: int, head: str, base: str) -> bool:
    pulls = sequence(record(value)["pull_requests"])
    if len(pulls) != 1:
        return False
    (only_pull,) = pulls
    pull = record(only_pull)
    return (
        identifier(pull["number"]) == identifier(number)
        and digest(record(pull["head"])["sha"]) == digest(head)
        and digest(record(pull["base"])["sha"]) == digest(base)
    )


def associated_run(value: object, pull_request: object, number: int, head: str, base: str) -> bool:
    run = record(value)
    pulls = sequence(run["pull_requests"])
    if pulls:
        return associated_pull(run, number, head, base)
    pull = record(pull_request)
    source = record(pull.get("head"))
    repository = record(source.get("repo"))
    native = record(run.get("head_repository"))
    return (
        identifier(pull.get("number")) == identifier(number)
        and digest(record(pull.get("base"))["sha"]) == digest(base)
        and digest(source.get("sha")) == digest(head)
        and text(source.get("ref")) == text(run.get("head_branch"))
        and identifier(repository["id"]) == identifier(native["id"])
        and repository["full_name"] == native["full_name"]
    )


def job_identity(value: object, run: dict[str, object]) -> tuple[int, str]:
    item = require_success(value)
    if identifier(item["run_id"]) != identifier(run["id"]):
        raise PolicyFailure("job belongs to a different workflow run")
    if identifier(item["run_attempt"]) != identifier(run["run_attempt"]):
        raise PolicyFailure("job belongs to a different workflow attempt")
    if digest(item["head_sha"]) != digest(run["head_sha"]):
        raise PolicyFailure("job belongs to a different candidate")
    return identifier(item["id"]), text(item["name"])


def require_inventory(jobs: Sequence[object], run: dict[str, object]) -> set[str]:
    identities: set[int] = set()
    names: set[str] = set()
    for value in jobs:
        identity, name = job_identity(value, run)
        if identity in identities or name in names:
            raise PolicyFailure("duplicate native job identity or required name")
        identities.add(identity)
        names.add(name)
    return names


def require_matrix(
    workflow_run: object,
    jobs: Sequence[object],
    workflow_id: int,
    expected_head: str,
    expected_names: set[str],
    native_total: object,
    complete: bool,
) -> None:
    """Caller must fetch all pages and bind policy and workflow ID from trusted base."""
    if complete is not True or not expected_names:
        raise PolicyFailure("complete required matrix policy and native inventory needed")
    if identifier(native_total) != len(jobs):
        raise PolicyFailure("native paginated job inventory is incomplete")
    run = verify_run(workflow_run, workflow_id, expected_head)
    if require_inventory(jobs, run) != expected_names:
        raise PolicyFailure("native job inventory differs from required matrix")
