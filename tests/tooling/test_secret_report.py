"""Pure native evidence parsing; no finding acceptance or authenticated scan claim."""

from __future__ import annotations

import hashlib
import json
import logging
import traceback
from dataclasses import FrozenInstanceError

import pytest
from scripts._secret_models import NativeReportError, ScanMode
from scripts._secret_report import parse_report


def native_row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "RuleID": "fixture-rule",
        "Description": "native source description",
        "StartLine": 1,
        "EndLine": 1,
        "StartColumn": 1,
        "EndColumn": 40,
        "Match": "context that must not be retained",
        "Secret": "REDACTED",
        "File": "tests/fixture.py",
        "SymlinkFile": "",
        "Commit": "",
        "Entropy": 3.25,
        "Author": "source author",
        "Email": "source email",
        "Date": "source date",
        "Message": "source message",
        "Tags": [],
        "Fingerprint": "tests/fixture.py:fixture-rule:1",
    }
    row.update(changes)
    return row


def report_bytes(*rows: dict[str, object]) -> bytes:
    return json.dumps(rows).encode()


@pytest.mark.parametrize("mode", list(ScanMode))
def test_native_clean_retains_exact_report_digest(mode: ScanMode) -> None:
    payload = b" [ ]\n"
    report = parse_report(payload, mode, 0)
    assert report.findings == ()
    assert report.sha256 == hashlib.sha256(payload).hexdigest()
    assert report.mode is mode


def test_findings_remain_present_and_do_not_retain_source_context() -> None:
    row = native_row()
    report = parse_report(report_bytes(row), ScanMode.TREE, 42)
    finding = report.findings[0]
    assert finding.path == "tests/fixture.py"
    assert finding.rule_id == "fixture-rule"
    assert finding.native_commit == ""
    assert finding.tags == ()
    assert "context that must not be retained" not in repr(report)
    assert "source author" not in repr(report)


@pytest.mark.parametrize("field", ["path", "rule_id"])
def test_restricted_metadata_cannot_be_mutated(field: str) -> None:
    finding = parse_report(report_bytes(native_row()), ScanMode.TREE, 42).findings[0]
    with pytest.raises(FrozenInstanceError):
        setattr(finding, field, "changed")


def test_historical_metadata_binds_full_native_commit() -> None:
    commit = "a" * 40
    row = native_row(Commit=commit, Fingerprint=f"{commit}:tests/fixture.py:fixture-rule:1")
    row["Link"] = "opaque native link"
    report = parse_report(report_bytes(row), ScanMode.HISTORY, 42)
    assert report.findings[0].native_commit == commit
    assert report.findings[0].mode is ScanMode.HISTORY


def test_restricted_native_identity_text_is_not_a_safe_display_string(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sentinel = "private-context-marker-for-negative-control"
    path = f"tests/{sentinel}.py"
    row = native_row(
        File=path, RuleID=sentinel, Fingerprint=f"{path}:{sentinel}:1", Tags=[sentinel]
    )
    report = parse_report(report_bytes(row), ScanMode.TREE, 42)
    assert report.findings[0].path == path
    assert report.findings[0].tags == (sentinel,)
    assert sentinel not in repr(report)
    assert sentinel not in repr(report.findings[0])
    with caplog.at_level(logging.INFO):
        logging.getLogger(__name__).info("Native metadata %r %r", report, report.findings[0])
    assert sentinel not in caplog.text


def test_same_fingerprint_findings_preserve_distinct_coordinates_and_order() -> None:
    left = native_row()
    right = native_row(StartColumn=42, EndColumn=81)
    report = parse_report(report_bytes(left, right), ScanMode.TREE, 42)
    assert [row.start_column for row in report.findings] == [1, 42]
    assert report.findings[0].native_fingerprint == report.findings[1].native_fingerprint


def test_multiline_native_end_column_can_precede_start_column() -> None:
    row = native_row(StartColumn=81, EndLine=2, EndColumn=39, Tags=["decoded:base64"])
    finding = parse_report(report_bytes(row), ScanMode.TREE, 42).findings[0]
    assert (finding.start_column, finding.end_line, finding.end_column) == (81, 2, 39)
    assert finding.tags == ("decoded:base64",)


@pytest.mark.parametrize("exit_code,rows", [(0, 1), (42, 0), (1, 0), (1, 1), (-9, 0), (99, 1)])
def test_unknown_fatal_and_inconsistent_exits_fail(exit_code: int, rows: int) -> None:
    payload = report_bytes(*[native_row()] * rows)
    with pytest.raises(NativeReportError, match="exit"):
        parse_report(payload, ScanMode.TREE, exit_code)


@pytest.mark.parametrize("exit_code", [False, True])
def test_boolean_is_not_a_native_exit(exit_code: bool) -> None:
    with pytest.raises(NativeReportError, match="exit"):
        parse_report(b"[]", ScanMode.TREE, exit_code)


@pytest.mark.parametrize(
    "payload",
    [b"", b"null", b"{}", b"[1]", b"[true]", b"[] trailing", b"\xff", b"\xef\xbb\xbf[]"],
)
def test_invalid_json_and_non_array_or_non_object_roots_fail(payload: bytes) -> None:
    with pytest.raises(NativeReportError):
        parse_report(payload, ScanMode.TREE, 0)


def test_duplicate_json_keys_fail_before_the_last_value_can_win() -> None:
    payload = report_bytes(native_row()).replace(
        b'"RuleID": "fixture-rule"', b'"RuleID": "changed", "RuleID": "fixture-rule"'
    )
    with pytest.raises(NativeReportError, match="duplicate"):
        parse_report(payload, ScanMode.TREE, 42)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_nonfinite_entropy_fails(constant: str) -> None:
    payload = report_bytes(native_row()).replace(b"3.25", constant.encode())
    with pytest.raises(NativeReportError):
        parse_report(payload, ScanMode.TREE, 42)


def test_exact_duplicate_findings_fail_without_deduplication() -> None:
    payload = report_bytes(native_row(), native_row())
    with pytest.raises(NativeReportError, match="duplicate"):
        parse_report(payload, ScanMode.TREE, 42)


def test_duplicate_safe_identity_with_different_source_prose_is_ambiguous() -> None:
    payload = report_bytes(native_row(), native_row(Message="different opaque source prose"))
    with pytest.raises(NativeReportError, match="duplicate"):
        parse_report(payload, ScanMode.TREE, 42)


def test_parser_error_does_not_render_input_or_decoder_excerpt() -> None:
    sentinel = "private-context-marker-for-negative-control"
    payload = ('[{"Match": "' + sentinel + '", broken]').encode()
    with pytest.raises(NativeReportError) as failure:
        parse_report(payload, ScanMode.TREE, 42)
    assert sentinel not in str(failure.value)
    assert sentinel not in "".join(traceback.format_exception(failure.value))
    assert failure.value.__suppress_context__ is True


def test_byte_limit_fails_before_decoding_or_truncation() -> None:
    with pytest.raises(NativeReportError, match="size"):
        parse_report(b" " * (16 * 1024 * 1024 + 1), ScanMode.TREE, 0)


def test_row_limit_fails_before_processing_partial_rows() -> None:
    with pytest.raises(NativeReportError, match="row"):
        parse_report(b"[" + b"0," * 100_000 + b"0]", ScanMode.TREE, 42)
