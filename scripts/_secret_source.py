"""Join exact native metadata to complete committed bytes, without approving it."""

from __future__ import annotations

import hashlib

from scripts._secret_git import GitObjects
from scripts._secret_identity import (
    FindingIdentityV1,
    SourceScope,
    validate_scope,
)
from scripts._secret_models import NativeFinding, NativeReportError, ScanMode
from scripts._secret_schema import validate_native_finding


def _source_commit(row: NativeFinding, scope: SourceScope, tree_bytes: bytes | None) -> str:
    if row.mode is ScanMode.TREE:
        if type(tree_bytes) is not bytes:
            raise NativeReportError("native tree bytes required") from None
        return scope.head
    if tree_bytes is not None:
        raise NativeReportError("native history tree bytes forbidden") from None
    if row.native_commit not in scope.retained_commits:
        raise NativeReportError("native history commit outside scope") from None
    return row.native_commit


def _source_bytes(row: NativeFinding, body: bytes, tree_bytes: bytes | None) -> None:
    if row.mode is ScanMode.TREE and tree_bytes != body:
        raise NativeReportError("native tree export mismatch") from None
    if row.end_line > body.count(b"\n") + 1:
        raise NativeReportError("native source line bounds") from None


def bind_finding(
    finding: NativeFinding,
    scope: SourceScope,
    objects: GitObjects,
    *,
    tree_bytes: bytes | None = None,
) -> FindingIdentityV1:
    """Equality/membership are not execution, complete enrollment or human authority."""
    row = validate_native_finding(finding)
    scope = validate_scope(scope)
    commit = _source_commit(row, scope, tree_bytes)
    blob = objects.resolve(commit, row.path)
    _source_bytes(row, blob.body, tree_bytes)
    return FindingIdentityV1(
        scope.repository_id,
        scope.scanner_policy_sha256,
        row,
        commit,
        blob.oid,
        hashlib.sha256(blob.body).hexdigest(),
    )
