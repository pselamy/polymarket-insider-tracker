"""Pure report validation only. No filesystem, scanner, policy or approval authority."""

from __future__ import annotations

import hashlib

from scripts._secret_json import decode_array
from scripts._secret_models import NativeFinding, NativeReport, NativeReportError, ScanMode
from scripts._secret_schema import parse_finding


def _exit_relationship(exit_code: int, count: int) -> None:
    if type(exit_code) is not int:
        raise NativeReportError("invalid native exit") from None
    expected = 42 if count else 0
    if exit_code != expected:
        raise NativeReportError("inconsistent or fatal native exit") from None


def _findings(rows: list[object], mode: ScanMode) -> tuple[NativeFinding, ...]:
    findings: list[NativeFinding] = []
    seen: set[NativeFinding] = set()
    for value in rows:
        finding = parse_finding(value, mode)
        if finding in seen:
            raise NativeReportError("duplicate native identity") from None
        seen.add(finding)
        findings.append(finding)
    return tuple(findings)


def _scan_mode(value: object) -> ScanMode:
    if not isinstance(value, ScanMode):
        raise NativeReportError("invalid native mode") from None
    return value


def parse_report(payload: bytes, mode: ScanMode, exit_code: int) -> NativeReport:
    """Parse complete bytes into restricted metadata, never accepted findings."""
    mode = _scan_mode(mode)
    rows = decode_array(payload)
    findings = _findings(rows, mode)
    _exit_relationship(exit_code, len(findings))
    return NativeReport(mode=mode, sha256=hashlib.sha256(payload).hexdigest(), findings=findings)
