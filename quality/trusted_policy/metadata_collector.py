"""Trusted-base caller supplies policy; never evaluate or checkout candidate code."""

import re
from dataclasses import dataclass

from quality.trusted_policy.comment_policy import require_comment
from quality.trusted_policy.execution_policy import associated_run, require_matrix
from quality.trusted_policy.metadata_pages import (
    ReadAPI,
    array_inventory,
    object_inventory,
)
from quality.trusted_policy.review_policy import (
    PolicyFailure,
    digest,
    identifier,
    latest_reviews,
    record,
    require_approval,
    text,
    validate_candidate,
)


@dataclass(frozen=True)
class Policy:
    repository: str
    pull_number: int
    head: str
    base: str
    workflow_id: int
    run_id: int
    required_names: frozenset[str]


def repository_route(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+", value) is None:
        raise PolicyFailure("exact native owner/repository identity required")
    if value.split("/")[1] in {".", ".."}:
        raise PolicyFailure("repository cannot be a relative path segment")
    return "repos/" + value


def candidate(api: ReadAPI, endpoint: str, policy: Policy) -> dict[str, object]:
    pr = record(api(endpoint))
    validate_candidate(pr, policy.head, policy.base)
    return pr


def roles(api: ReadAPI, repository: str, reviews: list[object]) -> dict[int, object]:
    result: dict[int, object] = {}
    for user, review in latest_reviews(reviews).items():
        login = text(record(review["user"])["login"])
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*", login) is None:
            raise PolicyFailure("native reviewer login must be a safe path segment")
        result[user] = api(f"{repository}/collaborators/{login}/permission")
    return result


def same_candidate(first: dict[str, object], second: dict[str, object]) -> None:
    if first != second:
        raise PolicyFailure("candidate metadata changed during evaluation")


def collect_run(
    api: ReadAPI, repository: str, policy: Policy, pull_request: dict[str, object]
) -> dict[str, object]:
    endpoint = f"{repository}/actions/runs/{identifier(policy.run_id)}"
    run = record(api(endpoint))
    attempt = identifier(run["run_attempt"])
    if not associated_run(run, pull_request, policy.pull_number, policy.head, policy.base):
        raise PolicyFailure("workflow run is not associated with current PR")
    total, jobs = object_inventory(api, f"{endpoint}/attempts/{attempt}/jobs", "jobs")
    require_matrix(
        run,
        jobs,
        policy.workflow_id,
        policy.head,
        set(policy.required_names),
        total,
        True,
    )
    if record(api(endpoint)) != run:
        raise PolicyFailure("workflow run changed during job evaluation")
    return run


def evaluate_core(api: ReadAPI, policy: Policy, comment_mode: bool) -> int:
    """API must authenticate native responses; Policy must come from trusted base."""
    repository = repository_route(policy.repository)
    digest(policy.head)
    digest(policy.base)
    endpoint = f"{repository}/pulls/{identifier(policy.pull_number)}"
    before = candidate(api, endpoint, policy)
    commits = array_inventory(api, endpoint + "/commits")
    reviews = array_inventory(api, endpoint + "/reviews")
    role_inventory = roles(api, repository, reviews)
    reviewer = require_approval(
        before,
        reviews,
        role_inventory,
        policy.head,
        policy.base,
        True,
        commits,
        not comment_mode,
    )
    comments: list[object] = []
    comment_route = f"{repository}/issues/{identifier(policy.pull_number)}/comments"
    if comment_mode:
        comments = array_inventory(api, comment_route)
        issue_url = f"https://api.github.com/{repository}/issues/{policy.pull_number}"
        require_comment(comments, reviewer, policy.head, issue_url, True)
    collect_run(api, repository, policy, before)
    same_candidate(before, candidate(api, endpoint, policy))
    if array_inventory(api, endpoint + "/reviews") != reviews:
        raise PolicyFailure("review inventory changed during evaluation")
    if roles(api, repository, reviews) != role_inventory:
        raise PolicyFailure("reviewer roles changed during evaluation")
    if comment_mode and array_inventory(api, comment_route) != comments:
        raise PolicyFailure("comment inventory changed during evaluation")
    return reviewer


def evaluate(api: ReadAPI, policy: Policy) -> int:
    """Existing review-body rationale prototype."""
    return evaluate_core(api, policy, False)


def evaluate_comment(api: ReadAPI, policy: Policy) -> int:
    """Proposed independent approval plus issue-comment rationale prototype."""
    return evaluate_core(api, policy, True)
