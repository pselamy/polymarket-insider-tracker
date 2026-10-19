"""Trusted-base metadata evaluation rejects malformed native responses."""

import pytest
from quality.trusted_policy.metadata_collector import Policy, evaluate, repository_route
from quality.trusted_policy.review_policy import PolicyFailure


def _policy() -> Policy:
    return Policy(
        repository="owner/repo",
        pull_number=1,
        head="a" * 40,
        base="b" * 40,
        workflow_id=1,
        run_id=2,
        required_names=frozenset({"Relentless CI gate"}),
    )


def test_repository_route_requires_exact_identity() -> None:
    assert repository_route("owner/repo") == "repos/owner/repo"
    with pytest.raises(PolicyFailure):
        repository_route("owner")


def test_evaluate_rejects_malformed_candidate_metadata() -> None:
    with pytest.raises(PolicyFailure):
        evaluate(lambda _endpoint: None, _policy())
