"""Prototype decision over complete GitHub review and role metadata, not PR code."""

import re
from collections.abc import Sequence
from datetime import datetime
from typing import TypeGuard


class PolicyFailure(ValueError):
    """Incomplete or unapproved native metadata cannot authorize policy changes."""


def is_record(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict)


def record(value: object) -> dict[str, object]:
    if not is_record(value):
        raise PolicyFailure("metadata object required")
    return value


def text(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise PolicyFailure("nonempty metadata string required")
    return value


def same_text(value: object, expected: str) -> bool:
    return text(value) == text(expected)


def identifier(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise PolicyFailure("positive platform identity required")
    return value


def digest(value: object) -> str:
    source = text(value)
    if re.fullmatch(r"[a-f0-9]{40}", source) is None:
        raise PolicyFailure("full Git commit identity required")
    return source


def timestamp(value: object) -> datetime:
    source = text(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", source) is None:
        raise PolicyFailure("native UTC submission timestamp required")
    return datetime.fromisoformat(source.replace("Z", "+00:00"))


def rationale(value: object) -> str:
    source = text(value).strip()
    prefix = "Policy rationale:"
    if not source.startswith(prefix) or len(source[len(prefix) :].strip()) < 30:
        raise PolicyFailure("approval needs explicit maintainer policy rationale")
    return source


def review_identity(value: object) -> tuple[int, int, dict[str, object]]:
    item = record(value)
    return identifier(item["id"]), identifier(record(item["user"])["id"]), item


def latest_reviews(values: Sequence[object]) -> dict[int, dict[str, object]]:
    identifiers: set[int] = set()
    latest: dict[int, tuple[tuple[datetime, int], dict[str, object]]] = {}
    for value in values:
        identity, user, item = review_identity(value)
        if identity in identifiers:
            raise PolicyFailure("duplicate review identity")
        identifiers.add(identity)
        retain_review(latest, user, identity, item)
    return {user: item for user, (_, item) in latest.items()}


def retain_review(
    latest: dict[int, tuple[tuple[datetime, int], dict[str, object]]],
    user: int,
    identity: int,
    item: dict[str, object],
) -> None:
    state = text(item["state"])
    if state not in {
        "APPROVED",
        "CHANGES_REQUESTED",
        "DISMISSED",
        "COMMENTED",
        "PENDING",
    }:
        raise PolicyFailure("unknown native review state")
    if state in {"COMMENTED", "PENDING"}:
        return
    key = timestamp(item["submitted_at"]), identity
    previous = latest.get(user)
    if previous is None or key > previous[0]:
        latest[user] = key, item


def matching_role(value: object, reviewer: dict[str, object]) -> bool:
    response = record(value)
    user = record(response["user"])
    if identifier(user["id"]) != identifier(reviewer["id"]) or user["login"] != reviewer["login"]:
        raise PolicyFailure("platform role identity differs from reviewer")
    return text(response["role_name"]) in {"maintain", "admin"}


def require_approval(
    pull_request: object,
    reviews: Sequence[object],
    role_responses: dict[int, object],
    expected_head: str,
    expected_base: str,
    complete: bool,
    commits: Sequence[object],
    require_review_reason: bool = True,
) -> int:
    """Fail closed; caller must independently authenticate and fully paginate inputs."""
    if complete is not True:
        raise PolicyFailure("complete authenticated metadata inventory required")
    author = validate_candidate(pull_request, expected_head, expected_base)
    authors = contributor_ids(record(pull_request), commits, author)
    return approval_search(reviews, role_responses, authors, expected_head, require_review_reason)


def eligible(item: dict[str, object], user: int, author: set[int], head: str) -> bool:
    return user not in author and same_text(item["state"], "APPROVED") and item["commit_id"] == head


def validate_candidate(value: object, expected_head: str, expected_base: str) -> int:
    pr = record(value)
    author = identifier(record(pr["user"])["id"])
    if digest(record(pr["head"])["sha"]) != digest(expected_head):
        raise PolicyFailure("candidate head changed during evaluation")
    if digest(record(pr["base"])["sha"]) != digest(expected_base):
        raise PolicyFailure("trusted base changed during evaluation")
    if not same_text(pr["state"], "open") or pr["draft"] is not False:
        raise PolicyFailure("approval requires an open ready pull request")
    return author


def approved_role_and_reason(
    item: dict[str, object],
    role: object,
    reviewer: dict[str, object],
    require_review_reason: bool,
) -> bool:
    if not matching_role(role, reviewer):
        return False
    if not require_review_reason:
        return True
    try:
        rationale(item["body"])
    except PolicyFailure:
        return False
    return True


def approval_search(
    reviews: Sequence[object],
    roles: dict[int, object],
    author: set[int],
    expected_head: str,
    require_review_reason: bool,
) -> int:
    for user, item in latest_reviews(reviews).items():
        if qualifies(item, user, author, expected_head, roles, require_review_reason):
            return user
    raise PolicyFailure("current non-author maintainer approval is missing")


def qualifies(
    item: dict[str, object],
    user: int,
    author: set[int],
    head: str,
    roles: dict[int, object],
    require_review_reason: bool,
) -> bool:
    if not eligible(item, user, author, head):
        return False
    return approved_role_and_reason(item, roles[user], record(item["user"]), require_review_reason)


def contributor_ids(pr: dict[str, object], commits: Sequence[object], author: int) -> set[int]:
    if identifier(pr["commits"]) != len(commits):
        raise PolicyFailure("complete native commit inventory required")
    authors = {author}
    identities: set[str] = set()
    for value in commits:
        item = record(value)
        commit = digest(item["sha"])
        if commit in identities:
            raise PolicyFailure("duplicate native commit identity")
        identities.add(commit)
        authors.update(commit_authors(item))
    return authors


def commit_authors(item: dict[str, object]) -> set[int]:
    return {identifier(record(item[field])["id"]) for field in ("author", "committer")}
