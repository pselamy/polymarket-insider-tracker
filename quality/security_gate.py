"""Enforce gitleaks (tree plus post-cut-over history) and opengrep; report osv only."""

import subprocess
from pathlib import Path

from quality.deadline import read_deadline
from quality.report_data import array, record
from quality.security import REPORTS, dependencies, static_analysis, tool
from quality.security_reports import read_report, verify_empty
from quality.source_scope import discover

CUTOVER = "gitleaks-cutover.txt"
SHA_LENGTH = 40
HEX_DIGITS = frozenset("0123456789abcdef")
VULNERABILITIES_FOUND = 1


def cutover(root: Path) -> str:
    """The fixed commit after which every commit is scanned; it must be an ancestor."""
    sha = (root / "quality" / CUTOVER).read_text(encoding="utf-8").strip()
    if len(sha) != SHA_LENGTH or not set(sha) <= HEX_DIGITS:
        raise ValueError("gitleaks cut-over must be a full lowercase commit SHA")
    subprocess.run(
        ["git", "merge-base", "--is-ancestor", sha, "HEAD"], cwd=root, check=True, timeout=60
    )
    return sha


def scan_secrets(root: Path, timeout: float, mode: str, name: str, extra: list[str]) -> None:
    report = root / ".quality-results" / name
    command = ["gitleaks", mode, ".", "--no-banner", "--redact", "--ignore-gitleaks-allow"]
    command += ["--config", "quality/gitleaks.toml", "--gitleaks-ignore-path"]
    command += ["quality/gitleaks.ignore", "--report-format", "json", "--report-path", str(report)]
    tool([*command, *extra], root, timeout)
    verify_empty(read_report(report))


def secrets(root: Path, timeout: float) -> None:
    """Full working tree, then only the commits after the cut-over."""
    scan_secrets(root, timeout, "dir", "tree-secrets.json", [])
    log_range = f"--log-opts={cutover(root)}..HEAD"
    scan_secrets(root, timeout, "git", "history-secrets.json", [log_range])


def audit_summary(report: object) -> str:
    packages = [
        package
        for result in array(record(report)["results"])
        for package in array(record(result)["packages"])
    ]
    groups = [len(array(record(package).get("groups", []))) for package in packages]
    return f"{sum(1 for count in groups if count)} packages, {sum(groups)} advisory groups"


def audit_report_only(root: Path, timeout: float) -> None:
    """Advisories are reported, not enforced; a crashed or missing scan still fails."""
    try:
        dependencies(root, timeout)
    except subprocess.CalledProcessError as error:
        if error.returncode != VULNERABILITIES_FOUND:
            raise
    report = read_report(root / ".quality-results" / "dependencies.json")
    print(f"osv-scanner (report only, not enforced): {audit_summary(report)}")


def verify_security_gate(root: Path) -> None:
    """Run independently of the unenrolled source-size hook; stale reports never count."""
    timeout = read_deadline(root / "quality" / "timeout.json")
    output = root / ".quality-results"
    output.mkdir(exist_ok=True)
    for name in REPORTS:
        (output / name).unlink(missing_ok=True)
    static_analysis(root, timeout, sorted(discover(root)))
    secrets(root, timeout)
    audit_report_only(root, timeout)
