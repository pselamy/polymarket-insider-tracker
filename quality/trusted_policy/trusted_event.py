"""Parse native issuer triggers without trusting event conclusions or PR code."""

from dataclasses import dataclass
from typing import Literal

from quality.trusted_policy.metadata_pages import sequence
from quality.trusted_policy.review_policy import (
    PolicyFailure,
    identifier,
    record,
    same_text,
    text,
)


@dataclass(frozen=True)
class Trigger:
    kind: Literal["rationale", "completed_run", "dispatch"]
    repository: str
    repository_id: int
    pull_number: int | None
    run_id: int | None


def repository(value: object, expected_name: str, expected_id: int) -> None:
    native = record(value)
    if text(native["full_name"]) != text(expected_name) or identifier(native["id"]) != identifier(
        expected_id
    ):
        raise PolicyFailure("issuer event belongs to another repository")


def rationale_event(event: dict[str, object], name: str, identity: int) -> Trigger:
    if event["action"] not in {"created", "edited", "deleted"}:
        raise PolicyFailure("unsupported rationale event action")
    issue = record(event.get("issue"))
    record(issue.get("pull_request"))
    return Trigger("rationale", name, identity, identifier(issue.get("number")), None)


def completed_run_event(event: dict[str, object], name: str, identity: int) -> Trigger:
    if not same_text(event["action"], "completed"):
        raise PolicyFailure("only a completed workflow run can initiate evaluation")
    run = record(event["workflow_run"])
    if not same_text(run["event"], "pull_request"):
        raise PolicyFailure("issuer needs a pull-request workflow run")
    pulls = sequence(run["pull_requests"])
    if len(pulls) > 1:
        raise PolicyFailure("run must not identify multiple pull requests")
    if not pulls:
        return Trigger("completed_run", name, identity, None, identifier(run["id"]))
    (only_pull,) = pulls
    pull = record(only_pull)
    return Trigger(
        "completed_run",
        name,
        identity,
        identifier(pull["number"]),
        identifier(run["id"]),
    )


def dispatch_event(event: dict[str, object], name: str, identity: int) -> Trigger:
    if not same_text(event.get("action"), "relentless-policy-reevaluate"):
        raise PolicyFailure("unsupported policy dispatch action")
    payload = record(event.get("client_payload"))
    if set(payload) != {"pull_number"}:
        raise PolicyFailure("policy dispatch must contain only a PR lookup number")
    return Trigger("dispatch", name, identity, identifier(payload["pull_number"]), None)


def parse_event(event_name: str, value: object, expected_name: str, expected_id: int) -> Trigger:
    """Event supplies identifiers only; the collector must reread native state."""
    event = record(value)
    repository(event["repository"], expected_name, expected_id)
    if same_text(event_name, "repository_dispatch"):
        return dispatch_event(event, expected_name, expected_id)
    if same_text(event_name, "issue_comment"):
        return rationale_event(event, expected_name, expected_id)
    if same_text(event_name, "workflow_run"):
        return completed_run_event(event, expected_name, expected_id)
    raise PolicyFailure("unsupported trusted issuer event")
