"""Actual native object types, missing objects and aggregate resolution limits."""

from __future__ import annotations

from pathlib import Path

import pytest
from scripts import _secret_git, _secret_tree
from scripts._secret_git import GitObjects, ResolutionBudget, commit_tree_oid
from scripts._secret_models import NativeReportError
from scripts._secret_source import bind_finding
from tests.tooling.secret_source_fixtures import (
    entry,
    finding,
    repository,
    single_file,
    source_scope,
)


def test_direct_tag_does_not_peel_to_commit(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    body = f"object {head}\ntype commit\ntag fixture\ntagger Fixture <fixture@example.invalid> 1 +0000\n\ntag\n"
    tag = repo.write("tag", body.encode())
    with pytest.raises(NativeReportError, match="native direct object type"):
        bind_finding(finding(tag), source_scope(tag), GitObjects(repo.location))


def test_missing_commit_fails(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    with pytest.raises(NativeReportError):
        bind_finding(finding("1" * 40), source_scope("1" * 40), GitObjects(repo.location))


def test_unsupported_storage_format_fails(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git", "sha256")
    with pytest.raises(NativeReportError, match="native object format unsupported"):
        bind_finding(finding("1" * 40), source_scope("1" * 40), GitObjects(repo.location))


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"parent " + b"1" * 40 + b"\n",
        b"tree " + b"1" * 40 + b"x",
        b"tree " + b"\xff" * 40 + b"\n",
        b"tree " + b"g" * 40 + b"\n",
    ],
)
def test_commit_tree_header_is_exact(body: bytes) -> None:
    with pytest.raises(NativeReportError):
        commit_tree_oid(body)
    assert commit_tree_oid(b"tree " + b"1" * 40 + b"\nrest") == "1" * 40


def test_missing_promisor_blob_is_not_fetched(tmp_path: Path) -> None:
    origin = repository(tmp_path / "origin.git")
    origin_head = single_file(origin)
    origin.git("update-ref", "refs/heads/main", origin_head)
    origin.git("config", "uploadpack.allowAnySHA1InWant", "true")
    blob = origin.git("rev-parse", f"{origin_head}:note.py").decode().strip()
    repo = repository(tmp_path / "repo.git")
    repo.git("config", "remote.origin.url", str(origin.directory))
    repo.git("config", "remote.origin.promisor", "true")
    repo.git("config", "extensions.partialClone", "origin")
    head = repo.commit(repo.write("tree", entry(b"note.py", blob)))
    objects = GitObjects(repo.location)
    with pytest.raises(NativeReportError):
        bind_finding(finding(head), source_scope(head), objects)
    # The intentionally unguarded positive control fetches from an owned local bare repository.
    # No network endpoint, credentials or user repository participates.
    assert repo.git("cat-file", "blob", blob) == b"fixture\n"
    assert bind_finding(finding(head), source_scope(head), objects).source_blob_oid == blob


def test_global_command_budget_is_not_reset_per_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    monkeypatch.setattr(_secret_git, "RESOLUTION_COMMANDS", 10)
    assert bind_finding(finding(head), source_scope(head), GitObjects(repo.location))
    monkeypatch.setattr(_secret_git, "RESOLUTION_COMMANDS", 9)
    with pytest.raises(NativeReportError, match="native resolution command limit"):
        bind_finding(finding(head), source_scope(head), GitObjects(repo.location))


def test_global_raw_byte_budget_is_not_reset_per_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    commit = repo.git("cat-file", "commit", head)
    tree_oid = commit.splitlines()[0][5:].decode()
    tree = repo.git("cat-file", "tree", tree_oid)
    total = len(commit) + len(tree) + len(b"fixture\n")
    monkeypatch.setattr(_secret_git, "RESOLUTION_BYTES", total)
    assert bind_finding(finding(head), source_scope(head), GitObjects(repo.location))
    monkeypatch.setattr(_secret_git, "RESOLUTION_BYTES", total - 1)
    with pytest.raises(NativeReportError, match="native resolution byte limit"):
        bind_finding(finding(head), source_scope(head), GitObjects(repo.location))


def test_expired_resolution_budget_fails() -> None:
    with pytest.raises(NativeReportError, match="native resolution timeout"):
        ResolutionBudget(deadline=0).command()


def test_path_and_tree_entry_capacities(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    with pytest.raises(NativeReportError, match="native source path depth"):
        bind_finding(
            finding(head, path="a/" * 256 + "note.py"),
            source_scope(head),
            GitObjects(repo.location),
        )
    monkeypatch.setattr(_secret_tree, "TREE_ENTRIES", 1)
    with pytest.raises(NativeReportError, match="native tree entry limit"):
        _secret_tree.parse_tree(entry(b"a", "1" * 40) + entry(b"b", "1" * 40))
