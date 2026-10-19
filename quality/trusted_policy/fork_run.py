"""Recover a fork PR only from unique, current native run and PR metadata."""

from quality.trusted_policy.metadata_pages import ReadAPI, array_inventory
from quality.trusted_policy.review_policy import (
    PolicyFailure,
    digest,
    identifier,
    record,
    same_text,
    text,
)


def same_head(run: dict[str, object], pull: dict[str, object], base: str) -> bool:
    head = record(pull["head"])
    repository = record(head["repo"])
    native = record(run["head_repository"])
    return (
        same_text(pull["state"], "open")
        and digest(record(pull["base"])["sha"]) == digest(base)
        and digest(head["sha"]) == digest(run["head_sha"])
        and text(head["ref"]) == text(run["head_branch"])
        and identifier(repository["id"]) == identifier(native["id"])
        and repository["full_name"] == native["full_name"]
    )


def unique_pull(
    api: ReadAPI, root: str, run: object, run_id: int, workflow_id: int, base: str
) -> int:
    native = record(run)
    if (
        identifier(native["id"]) != identifier(run_id)
        or identifier(native["workflow_id"]) != identifier(workflow_id)
        or not same_text(native["event"], "pull_request")
        or not same_text(native["status"], "completed")
    ):
        raise PolicyFailure("native completed PR run identity differs")
    candidates = [
        identifier(record(pull)["number"])
        for pull in array_inventory(api, root + "/pulls")
        if same_head(native, record(pull), base)
    ]
    if len(candidates) != 1:
        raise PolicyFailure("fork run does not identify exactly one current PR")
    return candidates.pop()
