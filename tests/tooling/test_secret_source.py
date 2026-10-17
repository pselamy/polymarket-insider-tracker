"""A finding binds only to its complete exact committed regular-file bytes."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from scripts._secret_git import GitObjects
from scripts._secret_models import NativeReportError, ScanMode
from scripts._secret_source import bind_finding
from tests.tooling.secret_source_fixtures import (
    entry,
    finding,
    repository,
    single_file,
    source_scope,
)


def test_history_and_tree_are_distinct_exact_identities(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    scope = source_scope(head)
    objects = GitObjects(repo.location)
    historical = bind_finding(finding(head), scope, objects)
    tree = bind_finding(finding(head, ScanMode.TREE), scope, objects, tree_bytes=b"fixture\n")
    assert historical.source_commit == tree.source_commit == head
    assert historical.source_blob_sha256 == hashlib.sha256(b"fixture\n").hexdigest()
    assert historical.source_blob_oid == tree.source_blob_oid
    assert historical.sha256 != tree.sha256
    assert not hasattr(tree, "accepted")


@pytest.mark.parametrize("supplied", [None, b"changed\n", b"fixture", b"fixture\n\n"])
def test_tree_requires_exact_complete_export_bytes(tmp_path: Path, supplied: bytes | None) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    with pytest.raises(NativeReportError):
        bind_finding(
            finding(head, ScanMode.TREE),
            source_scope(head),
            GitObjects(repo.location),
            tree_bytes=supplied,
        )


def test_history_never_uses_head_or_tree_bytes(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    old = single_file(repo, b"old\n")
    head = single_file(repo, b"new\n")
    result = bind_finding(finding(old), source_scope(head, old), GitObjects(repo.location))
    assert result.source_commit == old
    assert result.source_blob_sha256 == hashlib.sha256(b"old\n").hexdigest()
    with pytest.raises(NativeReportError):
        bind_finding(finding(old), source_scope(head), GitObjects(repo.location))
    with pytest.raises(NativeReportError):
        bind_finding(
            finding(old), source_scope(head, old), GitObjects(repo.location), tree_bytes=b"old\n"
        )


@pytest.mark.parametrize("path", ["-dash", ":(glob)*", "a:b [*]", "café.py"])
def test_literal_utf8_paths_are_not_pathspecs(tmp_path: Path, path: str) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo, name=path.encode())
    identity = bind_finding(finding(head, path=path), source_scope(head), GitObjects(repo.location))
    assert identity.finding.path == path


@pytest.mark.parametrize("mode", [b"120000", b"160000", b"40000"])
def test_nonregular_final_entries_fail(tmp_path: Path, mode: bytes) -> None:
    repo = repository(tmp_path / "repo.git")
    target = repo.write("blob", b"target")
    if mode == b"40000":
        target = repo.write("tree", b"")
    tree = repo.write("tree", entry(b"note.py", target, mode))
    head = repo.commit(tree)
    with pytest.raises(NativeReportError):
        bind_finding(finding(head), source_scope(head), GitObjects(repo.location))


@pytest.mark.parametrize("body", [b"", b"a", b"a\n", b"a\r\n", b"\xff\n\xfe"])
def test_lf_positions_and_opaque_columns(tmp_path: Path, body: bytes) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo, body)
    row = replace(finding(head), end_line=body.count(b"\n") + 1, end_column=1_000_000)
    assert bind_finding(row, source_scope(head), GitObjects(repo.location)).finding == row
    with pytest.raises(NativeReportError):
        bind_finding(
            replace(row, end_line=row.end_line + 1), source_scope(head), GitObjects(repo.location)
        )


def test_verified_nested_tree_and_non_utf8_sibling(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    blob = repo.write("blob", b"fixture\n")
    nested = repo.write("tree", entry(b"note.py", blob) + entry(b"\xff", blob))
    tree = repo.write("tree", entry(b"directory", nested, b"40000"))
    head = repo.commit(tree)
    row = finding(head, path="directory/note.py")
    assert bind_finding(row, source_scope(head), GitObjects(repo.location)).source_blob_oid == blob
    other = repo.write("blob", b"other\n")
    repo.corrupt(nested, "tree", entry(b"note.py", other) + entry(b"\xff", other))
    with pytest.raises(NativeReportError):
        bind_finding(row, source_scope(head), GitObjects(repo.location))


@pytest.mark.parametrize("kind", ["commit", "tree", "blob"])
def test_corrupt_object_chain_fails(tmp_path: Path, kind: str) -> None:
    repo = repository(tmp_path / "repo.git")
    blob = repo.write("blob", b"fixture\n")
    tree_body = entry(b"note.py", blob)
    tree = repo.write("tree", tree_body)
    head = repo.commit(tree)
    oid, body = {
        "blob": (blob, b"changed\n"),
        "tree": (tree, b""),
        "commit": (head, repo.git("cat-file", "commit", head) + b"changed\n"),
    }[kind]
    repo.corrupt(oid, kind, body)
    with pytest.raises(NativeReportError):
        bind_finding(finding(head), source_scope(head), GitObjects(repo.location))


def test_replacement_objects_do_not_redirect_source(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    replacement = single_file(repo, b"changed\n")
    repo.git("replace", head, replacement)
    result = bind_finding(finding(head), source_scope(head), GitObjects(repo.location))
    assert result.source_blob_sha256 == hashlib.sha256(b"fixture\n").hexdigest()


@pytest.mark.parametrize("path", ["absent", "note.py/child", "directory/note.py"])
def test_missing_path_or_nondirectory_component_fails(tmp_path: Path, path: str) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    with pytest.raises(NativeReportError):
        bind_finding(finding(head, path=path), source_scope(head), GitObjects(repo.location))


def test_mutable_tree_bytes_are_not_a_frozen_measurement(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    mutable = cast(bytes, bytearray(b"fixture\n"))
    with pytest.raises(NativeReportError):
        bind_finding(
            finding(head, ScanMode.TREE),
            source_scope(head),
            GitObjects(repo.location),
            tree_bytes=mutable,
        )
