"""Frozen source declarations and restricted identities confer no approval authority."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Literal, cast

from scripts._secret_models import NativeFinding, NativeReportError, ScanMode
from scripts._secret_schema import validate_native_finding


def hexadecimal(value: object, length: int) -> str:
    if type(value) is not str or re.fullmatch(f"[0-9a-f]{{{length}}}", value) is None:
        raise NativeReportError("native hexadecimal identity") from None
    return value


def _repository_id(value: object) -> None:
    if type(value) is not int or value < 1:
        raise NativeReportError("native repository identity") from None


def _retained(value: object, head: str) -> None:
    if type(value) is not frozenset or not value:
        raise NativeReportError("native retained scope") from None
    for commit in cast(frozenset[object], value):
        hexadecimal(commit, 40)
    if head not in value:
        raise NativeReportError("native retained head absent") from None


@dataclass(frozen=True, slots=True)
class SourceScope:
    """Declared metadata only; neither authenticity nor history completeness."""

    repository_id: int
    scanner_policy_sha256: str
    head: str
    retained_commits: frozenset[str]

    def __post_init__(self) -> None:
        validate_scope(self)


def _scope_fields(value: SourceScope) -> None:
    _repository_id(value.repository_id)
    hexadecimal(value.scanner_policy_sha256, 64)
    hexadecimal(value.head, 40)
    _retained(value.retained_commits, value.head)


def validate_scope(value: object) -> SourceScope:
    if type(value) is not SourceScope:
        raise NativeReportError("native source scope schema") from None
    try:
        _scope_fields(value)
    except AttributeError:
        raise NativeReportError("native source scope schema") from None
    return value


def _schema_version(value: object) -> None:
    if type(value) is not int or value != 1:
        raise NativeReportError("native identity schema version") from None


@dataclass(frozen=True, slots=True)
class FindingIdentityV1:
    """Exact restricted data, never an accepted or authenticated decision."""

    repository_id: int
    scanner_policy_sha256: str
    finding: NativeFinding = field(repr=False)
    source_commit: str
    source_blob_oid: str
    source_blob_sha256: str
    schema_version: Literal[1] = field(default=1, init=False)

    def __post_init__(self) -> None:
        try:
            self._validate()
        except AttributeError:
            raise NativeReportError("native identity schema") from None

    def _validate(self) -> None:
        _schema_version(self.schema_version)
        _repository_id(self.repository_id)
        hexadecimal(self.scanner_policy_sha256, 64)
        validate_native_finding(self.finding)
        hexadecimal(self.source_commit, 40)
        hexadecimal(self.source_blob_oid, 40)
        hexadecimal(self.source_blob_sha256, 64)
        if (
            self.finding.mode is ScanMode.HISTORY
            and self.finding.native_commit != self.source_commit
        ):
            raise NativeReportError("native identity source mismatch") from None

    def canonical_bytes(self) -> bytes:
        """Restricted evidence serialization; do not log or use as an output renderer."""
        self.__post_init__()
        row = self.finding
        data = {
            "schema_version": self.schema_version,
            "repository_id": self.repository_id,
            "scanner_policy_sha256": self.scanner_policy_sha256,
            "mode": row.mode.value,
            "native_commit": row.native_commit,
            "source_commit": self.source_commit,
            "path": row.path,
            "symlink_path": "",
            "rule_id": row.rule_id,
            "start_line": row.start_line,
            "end_line": row.end_line,
            "start_column": row.start_column,
            "end_column": row.end_column,
            "native_fingerprint": row.native_fingerprint,
            "tags": row.tags,
            "source_blob_oid": self.source_blob_oid,
            "source_blob_sha256": self.source_blob_sha256,
        }
        try:
            return json.dumps(
                data, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        except (ValueError, UnicodeError, OverflowError, RecursionError):
            raise NativeReportError("native identity serialization") from None

    @property
    def sha256(self) -> str:
        return hashlib.sha256(
            b"polymarket-native-finding-v1\0" + self.canonical_bytes()
        ).hexdigest()
