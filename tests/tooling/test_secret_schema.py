"""Strict qualified Gitleaks8.30.1 field and coordinate boundary controls."""

from __future__ import annotations

import json
from typing import cast

import pytest
from scripts._secret_models import NativeReportError, ScanMode
from scripts._secret_report import parse_report
from tests.tooling.test_secret_report import native_row, report_bytes


@pytest.mark.parametrize("field", list(native_row()))
def test_every_required_native_field_is_required(field: str) -> None:
    row = native_row()
    del row[field]
    with pytest.raises(NativeReportError, match="schema"):
        parse_report(report_bytes(row), ScanMode.TREE, 42)


def test_unknown_native_schema_is_not_silently_dropped() -> None:
    with pytest.raises(NativeReportError, match="schema"):
        parse_report(report_bytes(native_row(Unknown="value")), ScanMode.TREE, 42)


@pytest.mark.parametrize(
    "field,value",
    [
        ("StartLine", True),
        ("StartLine", "1"),
        ("StartLine", 1.0),
        ("StartLine", 0),
        ("EndLine", 0),
        ("StartColumn", 0),
        ("EndColumn", -1),
        ("StartColumn", False),
        ("RuleID", ""),
        ("File", ""),
        ("Fingerprint", ""),
        ("Description", None),
        ("Match", []),
        ("Secret", "unredacted synthetic marker"),
        ("Secret", None),
        ("Tags", None),
        ("Tags", "decoded"),
        ("Tags", [1]),
        ("Entropy", True),
        ("Entropy", "3.25"),
        ("Author", 2),
        ("Email", []),
        ("Message", {}),
        ("Date", None),
        ("SymlinkFile", "target.py"),
        ("Link", None),
    ],
)
def test_unsupported_native_field_values_fail(field: str, value: object) -> None:
    row = native_row(**{field: value})
    with pytest.raises(NativeReportError):
        parse_report(report_bytes(row), ScanMode.TREE, 42)


@pytest.mark.parametrize(
    "changes",
    [
        {"StartLine": 2, "EndLine": 1},
        {"StartColumn": 50, "EndColumn": 40},
        {"Commit": "a" * 40},
        {"Fingerprint": "different:fixture-rule:1"},
    ],
)
def test_inconsistent_native_metadata_fails(changes: dict[str, object]) -> None:
    with pytest.raises(NativeReportError):
        parse_report(report_bytes(native_row(**changes)), ScanMode.TREE, 42)


@pytest.mark.parametrize("commit", ["", "a" * 39, "A" * 40, "g" * 40, "a" * 41])
def test_history_requires_qualified_full_lowercase_commit(commit: str) -> None:
    row = native_row(Commit=commit, Fingerprint=f"{commit}:tests/fixture.py:fixture-rule:1")
    with pytest.raises(NativeReportError):
        parse_report(report_bytes(row), ScanMode.HISTORY, 42)


@pytest.mark.parametrize(
    "path",
    [
        "/abs.py",
        "../escape.py",
        "a/../b.py",
        "a/./b.py",
        "a//b.py",
        "a/",
        "a\\b.py",
        "a\n.py",
        "a\x00.py",
    ],
)
def test_paths_are_rejected_without_normalization(path: str) -> None:
    row = native_row(File=path, Fingerprint=f"{path}:fixture-rule:1")
    with pytest.raises(NativeReportError, match="path"):
        parse_report(report_bytes(row), ScanMode.TREE, 42)


@pytest.mark.parametrize("field", ["File", "Match", "Tags"])
def test_unpaired_unicode_surrogates_fail_without_rendering_them(field: str) -> None:
    value: object = "\ud800"
    if field == "Tags":
        value = [value]
    row = native_row(**{field: value})
    with pytest.raises(NativeReportError):
        parse_report(report_bytes(row), ScanMode.TREE, 42)


def test_unicode_path_and_opaque_columns_are_preserved() -> None:
    path = "tests/évidence.py"
    row = native_row(File=path, Fingerprint=f"{path}:fixture-rule:1", StartColumn=17, EndColumn=500)
    payload = json.dumps([row], ensure_ascii=False).encode()
    finding = parse_report(payload, ScanMode.TREE, 42).findings[0]
    assert finding.path == path
    assert finding.start_column == 17
    assert finding.end_column == 500


@pytest.mark.parametrize("mode", [None, "dir", "git", 1])
def test_untyped_callers_cannot_substitute_a_different_scope(mode: object) -> None:
    # Deliberately hostile runtime input; production callers use the enum.
    with pytest.raises(NativeReportError, match="mode"):
        parse_report(b"[]", cast(ScanMode, mode), 0)


def test_oversized_integer_entropy_fails_as_a_safe_domain_error() -> None:
    row = native_row(Entropy=10**400)
    with pytest.raises(NativeReportError, match="entropy"):
        parse_report(report_bytes(row), ScanMode.TREE, 42)


def test_excessively_nested_json_fails_without_a_decoder_trace() -> None:
    with pytest.raises(NativeReportError, match="JSON") as failure:
        parse_report(b"[" * 2000 + b"]" * 2000, ScanMode.TREE, 0)
    assert failure.value.__suppress_context__ is True
