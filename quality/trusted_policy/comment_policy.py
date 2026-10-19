"""Proposed native issue-comment rationale rule; not enrolled in either template."""

from collections.abc import Sequence

from quality.trusted_policy.review_policy import (
    PolicyFailure,
    digest,
    identifier,
    record,
    text,
)


def comment_reason(body: object, head: str) -> str:
    prefix = f"Policy rationale for {digest(head)}:"
    value = text(body)
    if not value.startswith(prefix):
        raise PolicyFailure("comment must name the full current head")
    reason = value[len(prefix) :].strip()
    if len(reason) < 30:
        raise PolicyFailure("comment needs substantive policy rationale")
    return reason


def comment_identity(value: object, issue_url: str) -> tuple[int, int, dict[str, object]]:
    item = record(value)
    identity = identifier(item["id"])
    if text(item["issue_url"]) != issue_url:
        raise PolicyFailure("comment belongs to another issue")
    return identity, identifier(record(item["user"])["id"]), item


def has_reason(item: dict[str, object], head: str) -> bool:
    try:
        comment_reason(item["body"], head)
    except PolicyFailure:
        return False
    return True


def remember_identity(seen: set[int], identity: int) -> None:
    if identity in seen:
        raise PolicyFailure("duplicate native comment identity")
    seen.add(identity)


def qualifying_ids(
    comments: Sequence[object], reviewer: int, head: str, issue_url: str
) -> list[int]:
    seen: set[int] = set()
    qualifying: list[int] = []
    for value in comments:
        identity, author, item = comment_identity(value, issue_url)
        remember_identity(seen, identity)
        if author == reviewer and has_reason(item, head):
            qualifying.append(identity)
    return qualifying


def require_comment(
    comments: Sequence[object],
    reviewer_id: int,
    head: str,
    issue_url: str,
    complete: object,
) -> int:
    """Accept only a current-head rationale by the eligible approved reviewer."""
    if complete is not True:
        raise PolicyFailure("complete native comment inventory required")
    qualifying = qualifying_ids(comments, identifier(reviewer_id), digest(head), text(issue_url))
    if not qualifying:
        raise PolicyFailure("current-head maintainer rationale comment is missing")
    return max(qualifying)
