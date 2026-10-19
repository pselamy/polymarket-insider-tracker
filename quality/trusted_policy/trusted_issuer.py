"""Compose native trigger resolution, trusted review evaluation and App check."""

from collections.abc import Callable

from quality.trusted_policy.issuer_resolution import (
    ReviewedPolicy,
    current_candidate,
    resolve,
)
from quality.trusted_policy.metadata_collector import evaluate_comment
from quality.trusted_policy.metadata_pages import ReadAPI
from quality.trusted_policy.review_policy import PolicyFailure, digest, record
from quality.trusted_policy.trusted_event import parse_event


def issue(
    api: ReadAPI,
    publish: Callable[[str, bool], int],
    event_name: str,
    event: object,
    reviewed: ReviewedPolicy,
) -> int:
    """ReviewedPolicy and publisher identity must come from protected base state."""
    trigger = parse_event(event_name, event, reviewed.repository, reviewed.repository_id)
    try:
        policy = resolve(api, trigger, reviewed)
    except PolicyFailure:
        if trigger.pull_number is None:
            raise
        pull = current_candidate(api, trigger.pull_number, reviewed)
        return publish(digest(record(pull["head"])["sha"]), False)
    failure = publish(policy.head, False)
    try:
        evaluate_comment(api, policy)
    except PolicyFailure:
        return failure
    return publish(policy.head, True)
