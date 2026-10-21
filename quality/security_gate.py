"""Enforce gitleaks (tree plus post-cut-over history) opengrep and osv advisories."""

import subprocess
from pathlib import Path

from quality.deadline import read_deadline
from quality.report_data import array, record, text
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


def group_line(package: object, group: object) -> str:
    ids = ", ".join(text(item) for item in array(record(group)["ids"]))
    return f"{package_name(package)}: {ids}"


def vulnerability_line(package: object, vulnerability: object) -> str:
    return f"{package_name(package)}: {text(record(vulnerability)['id'])}"


def package_lines(package: object) -> list[str]:
    fields = record(package)
    groups = [group_line(package, item) for item in array(fields.get("groups", []))]
    found = array(fields.get("vulnerabilities", []))
    return groups + [vulnerability_line(package, item) for item in found]


def advisory_lines(report: object) -> list[str]:
    """One line per osv alias group or vulnerability: package, locked version, advisory IDs."""
    packages = [
        package
        for result in array(record(report)["results"])
        for package in array(record(result)["packages"])
    ]
    return [line for package in packages for line in package_lines(package)]


def package_name(package: object) -> str:
    identity = record(record(package)["package"])
    return f"{text(identity['name'])} {text(identity['version'])}"


class AdvisoryError(RuntimeError):
    """The dependency scan found advisories, or could not prove there were none."""


def audit_enforced(root: Path, timeout: float) -> None:
    """Any advisory fails the gate; so does a scan that crashed or produced unusable output."""
    try:
        scan_dependencies(root, timeout)
    except AdvisoryError:
        raise
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as error:
        raise AdvisoryError(f"osv-scanner could not prove the lockfile clean: {error!r}") from error


def run_scan(root: Path, timeout: float) -> Exception | None:
    """The scanner's own verdict; a nonzero exit or failed report check is judged afterwards."""
    try:
        dependencies(root, timeout)
    except subprocess.CalledProcessError as error:
        if error.returncode != VULNERABILITIES_FOUND:
            raise
        return error
    except ValueError as error:
        return error
    return None


def scan_dependencies(root: Path, timeout: float) -> None:
    verdict = run_scan(root, timeout)
    lines = advisory_lines(read_report(root / ".quality-results" / "dependencies.json"))
    if lines:
        raise AdvisoryError("osv-scanner reported advisories:\n" + "\n".join(lines))
    if verdict is not None:
        raise AdvisoryError(f"osv-scanner could not prove the lockfile clean: {verdict!r}")


def verify_security_gate(root: Path) -> None:
    """Run independently of the unenrolled source-size hook; stale reports never count."""
    timeout = read_deadline(root / "quality" / "timeout.json")
    output = root / ".quality-results"
    output.mkdir(exist_ok=True)
    for name in REPORTS:
        (output / name).unlink(missing_ok=True)
    static_analysis(root, timeout, sorted(discover(root)))
    secrets(root, timeout)
    audit_enforced(root, timeout)
