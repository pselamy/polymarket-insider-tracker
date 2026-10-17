"""Restricted canonical identity is exact data, not an approval decision."""

from __future__ import annotations

import hashlib
import json
import traceback
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from scripts._secret_git import GitObjects
from scripts._secret_identity import (
    FindingIdentityV1,
    SourceScope,
    validate_scope,
)
from scripts._secret_models import NativeFinding, NativeReportError, ScanMode
from scripts._secret_schema import validate_native_finding
from scripts._secret_source import bind_finding
from tests.tooling.secret_source_fixtures import finding, repository, single_file, source_scope


def test_canonical_identity_has_exact_inventory_and_domain() -> None:
    row = finding("1" * 40, path="café.py")
    identity = FindingIdentityV1(17, "a" * 64, row, "1" * 40, "2" * 40, "3" * 64)
    expected = {
        "schema_version": 1,
        "repository_id": 17,
        "scanner_policy_sha256": "a" * 64,
        "mode": "git",
        "native_commit": "1" * 40,
        "source_commit": "1" * 40,
        "path": "café.py",
        "symlink_path": "",
        "rule_id": "fixture-rule",
        "start_line": 1,
        "end_line": 1,
        "start_column": 1,
        "end_column": 9,
        "native_fingerprint": f"{'1' * 40}:café.py:fixture-rule:1",
        "tags": ["fixture"],
        "source_blob_oid": "2" * 40,
        "source_blob_sha256": "3" * 64,
    }
    canonical = json.dumps(
        expected, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    assert identity.canonical_bytes() == canonical
    assert (
        identity.sha256 == hashlib.sha256(b"polymarket-native-finding-v1\0" + canonical).hexdigest()
    )
    assert "café.py" not in repr(identity)
    assert "fixture-rule" not in repr(identity)
    assert not hasattr(identity, "accepted")


def test_every_independent_identity_field_changes_digest() -> None:
    row = finding("1" * 40)
    identity = FindingIdentityV1(17, "a" * 64, row, "1" * 40, "2" * 40, "3" * 64)
    changes = (
        replace(identity, repository_id=18),
        replace(identity, scanner_policy_sha256="b" * 64),
        replace(identity, source_blob_oid="4" * 40),
        replace(identity, source_blob_sha256="5" * 64),
    )
    rows = (
        replace(row, start_column=2),
        replace(row, end_column=10),
        replace(row, end_line=2),
        replace(row, tags=("other", "fixture")),
        replace(row, tags=("fixture", "other")),
        replace(row, rule_id="other", native_fingerprint=f"{'1' * 40}:note.py:other:1"),
        replace(row, path="other", native_fingerprint=f"{'1' * 40}:other:fixture-rule:1"),
        replace(
            row, start_line=2, end_line=2, native_fingerprint=f"{'1' * 40}:note.py:fixture-rule:2"
        ),
    )
    alternatives = (*changes, *(replace(identity, finding=value) for value in rows))
    digests = {value.sha256 for value in alternatives}
    assert identity.sha256 not in digests
    assert len(digests) == len(alternatives)


@pytest.mark.parametrize(
    "field,value",
    [
        ("repository_id", True),
        ("repository_id", 0),
        ("scanner_policy_sha256", "A" * 64),
        ("head", "1" * 39),
        ("retained_commits", frozenset[str]()),
        ("retained_commits", {"1" * 40}),
        ("retained_commits", frozenset({"2" * 40})),
        ("retained_commits", frozenset({1})),
    ],
)
def test_scope_rejects_invalid_direct_constructor(field: str, value: object) -> None:
    fields: dict[str, object] = {
        "repository_id": 17,
        "scanner_policy_sha256": "a" * 64,
        "head": "1" * 40,
        "retained_commits": frozenset({"1" * 40}),
    }
    fields[field] = value
    with pytest.raises(NativeReportError):
        SourceScope(
            cast(int, fields["repository_id"]),
            cast(str, fields["scanner_policy_sha256"]),
            cast(str, fields["head"]),
            cast(frozenset[str], fields["retained_commits"]),
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("mode", "git"),
        ("native_commit", "A" * 40),
        ("path", "../private"),
        ("rule_id", ""),
        ("native_fingerprint", "private"),
        ("start_line", True),
        ("start_column", 0),
        ("end_column", -1),
        ("end_line", 0),
        ("tags", ["private"]),
        ("tags", (False,)),
    ],
)
def test_direct_findings_fail_safely(tmp_path: Path, field: str, value: object) -> None:
    repo = repository(tmp_path / "repo.git")
    head = single_file(repo)
    row = finding(head)
    invalid = replace(row)
    object.__setattr__(invalid, field, value)
    private = "caller-" + "private-sentinel"
    try:
        raise RuntimeError(private)
    except RuntimeError:
        with pytest.raises(NativeReportError) as error:
            bind_finding(invalid, source_scope(head), GitObjects(repo.location))
    assert private not in "".join(traceback.format_exception(error.value))
    assert error.value.__suppress_context__


def test_tree_identity_cannot_carry_to_another_head(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo.git")
    old = single_file(repo)
    tree = repo.git("show", "-s", "--format=%T", old).decode().strip()
    head = repo.commit(tree, old)
    row: NativeFinding = finding(old, ScanMode.TREE)
    first = bind_finding(row, source_scope(old), GitObjects(repo.location), tree_bytes=b"fixture\n")
    second = bind_finding(
        row, source_scope(head, old), GitObjects(repo.location), tree_bytes=b"fixture\n"
    )
    assert first.source_blob_oid == second.source_blob_oid
    assert first.sha256 != second.sha256


def test_nominal_types_and_history_source_mismatch_fail() -> None:
    with pytest.raises(NativeReportError):
        validate_scope("private")
    with pytest.raises(NativeReportError):
        validate_native_finding("private")
    with pytest.raises(NativeReportError, match="native identity source mismatch"):
        FindingIdentityV1(17, "a" * 64, finding("1" * 40), "2" * 40, "3" * 40, "4" * 64)


def test_unrepresentable_integer_serialization_suppresses_context() -> None:
    row = replace(finding("1" * 40), end_column=10**10000)
    identity = FindingIdentityV1(17, "a" * 64, row, "1" * 40, "2" * 40, "3" * 64)
    private = "private-" + "caller-sentinel"
    try:
        raise RuntimeError(private)
    except RuntimeError:
        with pytest.raises(NativeReportError, match="native identity serialization") as error:
            identity.canonical_bytes()
    assert private not in "".join(traceback.format_exception(error.value))
