"""Per-cluster duplication files must agree with the clones a scan reports."""

from __future__ import annotations

from pathlib import Path

import pytest
from quality.duplication_clusters import (
    Clone,
    Cluster,
    clones_from_report,
    load_clusters,
    parse_cluster,
    problems,
)
from quality.duplication_clusters_report import render

CLUSTER_DIRECTORY = Path(__file__).parents[2] / "docs" / "duplication" / "clusters"
ALPHA = "tests/alpha.py"
BETA = "tests/beta.py"


def _clone(first: str = ALPHA, second: str = BETA) -> Clone:
    return Clone(tokens=60, lines=9, first=first, first_start=3, second=second, second_start=40)


def _cluster(status: str, files: str = "", watch: str = "", number: int = 3) -> Cluster:
    header = [f"cluster: {number}", f"status: {status}"]
    header += [f"files: {files}"] if files else []
    header += [f"watch: {watch}"] if watch else []
    content = "\n".join(["---", *header, "---", "", "# Cluster"])
    return parse_cluster(f"{number:02d}-example.md", content)


def test_repository_cluster_files_load_and_number_every_original_cluster() -> None:
    numbers = [cluster.number for cluster in load_clusters(CLUSTER_DIRECTORY)]
    assert numbers == list(range(1, 33))


def test_live_cluster_with_a_remaining_clone_is_current() -> None:
    clusters = [_cluster("real", files=f"{ALPHA}, {BETA}")]
    assert problems(clusters, [_clone()]) == []


def test_stale_report_live_cluster_without_clones_fails() -> None:
    clusters = [_cluster("real", files=f"{ALPHA}, {BETA}")]
    assert problems(clusters, []) == ["cluster 03 is real but no clone remains; mark it resolved"]


def test_stale_report_resolved_cluster_that_still_has_clones_fails() -> None:
    clusters = [_cluster("resolved", watch=f"{ALPHA} <-> {BETA}")]
    found = problems(clusters, [_clone(BETA, ALPHA)])
    assert found == ["cluster 03 is resolved but clones remain: " f"{BETA}:3 <-> {ALPHA}:40"]


def test_resolved_cluster_without_remaining_clones_passes() -> None:
    clusters = [_cluster("resolved", watch=f"{ALPHA} <-> {BETA}")]
    assert problems(clusters, [_clone(ALPHA, ALPHA)]) == []


def test_unowned_clones_are_reported_but_do_not_fail() -> None:
    assert problems([], [_clone()]) == []
    report = render(
        [], [_clone()], {"clones": 1, "duplicatedLines": 9, "percentage": 0.5, "sources": 2}
    )
    assert "| unassigned | 1 | 9 |" in report


@pytest.mark.parametrize(
    "content",
    [
        "# no header",
        "---\ncluster: 3\nstatus: maybe\n---\n",
        "---\ncluster: 3\nstatus: real\n---\n",
        "---\ncluster: 4\nstatus: resolved\n---\n",
    ],
)
def test_malformed_cluster_header_is_rejected(content: str) -> None:
    with pytest.raises(ValueError):
        parse_cluster("03-example.md", content)


def test_native_report_maps_back_to_repository_relative_paths() -> None:
    side = {"name": "/stage/tests/alpha.py", "start": 3}
    other = {"name": "/stage/tests/beta.py", "start": 40}
    report = {"duplicates": [{"tokens": 60, "lines": 9, "firstFile": side, "secondFile": other}]}
    assert clones_from_report(report, "/stage/") == [_clone()]
