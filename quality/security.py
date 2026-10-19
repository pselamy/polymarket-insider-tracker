"""Run pinned scanners and validate fresh, complete reports."""

import os
from pathlib import Path

from quality.commands import run
from quality.security_reports import (
    read_report,
    verify_audit,
    verify_empty,
    verify_sast,
)
from quality.source_scope import verify_sources

REPORTS = (
    "history-secrets.json",
    "tree-secrets.json",
    "dependencies.json",
    "static-security.json",
)


def tool(command: list[str], root: Path, timeout: float) -> None:
    """Mise uses pinned artifact URLs and hashes; tool failures propagate."""
    run(
        ["mise", "--yes", "--locked", "exec", "--", *command],
        root,
        timeout,
        dict(os.environ),
    )


def secrets(root: Path, timeout: float) -> None:
    """Inspect current files and all available history without suppressions."""
    for mode, name in (("git", "history-secrets.json"), ("dir", "tree-secrets.json")):
        report = root / ".quality-results" / name
        tool(
            [
                "gitleaks",
                mode,
                ".",
                "--no-banner",
                "--redact",
                "--ignore-gitleaks-allow",
                "--config",
                "quality/gitleaks.toml",
                "--gitleaks-ignore-path",
                "quality/gitleaks.ignore",
                "--report-format",
                "json",
                "--report-path",
                str(report),
            ],
            root,
            timeout,
        )
        verify_empty(read_report(report))


def dependencies(root: Path, timeout: float) -> None:
    """Scan every locked development, transitive and platform-specific package."""
    report = root / ".quality-results" / "dependencies.json"
    lockfile = root / "uv.lock"
    tool(
        [
            "osv-scanner",
            "scan",
            "source",
            "--lockfile",
            str(lockfile),
            "--config",
            "quality/osv-scanner.toml",
            "--all-vulns",
            "--all-packages",
            "--format",
            "json",
            "--output-file",
            str(report),
        ],
        root,
        timeout,
    )
    verify_audit(read_report(report), lockfile)


def static_analysis(root: Path, timeout: float, sources: list[Path]) -> None:
    """Disable ignore shortcuts and independently check the scanned-file inventory."""
    paths = [path.relative_to(root).as_posix() for path in sources]
    report = root / ".quality-results" / "static-security.json"
    tool(
        [
            "opengrep",
            "scan",
            "--config",
            "quality/security.yml",
            "--error",
            "--strict",
            "--disable-nosem",
            "--disable-version-check",
            "--taint-intrafile",
            "--x-ignore-semgrepignore-files",
            "--no-git-ignore",
            "--max-target-bytes",
            "0",
            "--json",
            "--output",
            str(report),
            *paths,
        ],
        root,
        timeout,
    )
    verify_sast(read_report(report), set(paths))


def verify_security(root: Path, timeout: float) -> None:
    """Old output cannot substitute for a failed or incomplete new scan."""
    sources = verify_sources(root)
    output = root / ".quality-results"
    output.mkdir(exist_ok=True)
    for name in REPORTS:
        (output / name).unlink(missing_ok=True)
    secrets(root, timeout)
    dependencies(root, timeout)
    static_analysis(root, timeout, sources)
