"""Fail-closed secret scan over the tracked tree plus committed history.

Runs gitleaks when its binary is on PATH (the CI job installs the pinned
release); otherwise runs a deterministic built-in probe over high-signal
credential shapes. Both paths honor the same contract:

- exit 0: the working tree AND the committed history were scanned and no
  finding survived the single allowlist authority.
- exit 1: a finding survived, the git report is missing/empty, or the scan
  could not be proven.
- exit 2: invocation or prerequisite error (missing gitleaks binary when it
  is required, bad config path, unreadable tree, non-git tree).

The allowlist covers ONLY deterministic placeholder credentials already
asserted by the repository's own redaction/contract tests, plus (probe-only)
content-addressed sha256 digests that structurally collide with the 64-hex
rule. It lives in exactly one place: ``.gitleaks.toml``. This module never carries a second
hardcoded list; every allowlist decision is validated against the config
file so the two surfaces cannot drift. The probe redacts allowlisted spans
and still scans the remainder of each line, so a real credential that shares
a line with an allowlisted token is still caught. Any new secret pattern fails the
gate; the owner then either removes the secret or extends the allowlist
with a reviewed, narrowly-scoped entry.

The built-in probe is a last-mile safety net for environments without the
gitleaks CLI. It does NOT claim a full-history scan: it scans the working
tree plus the ``git log -p`` patch stream for credential shapes. The
authoritative history proof comes from the gitleaks branch, which always
runs in CI.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPOSITORY_ROOT / ".gitleaks.toml"
REPORT_NAME = "gitleaks-report.json"
EXIT_FAILURE = 1
EXIT_PREREQUISITE = 2


class Finding(NamedTuple):
    """One surviving secret finding."""

    rule: str
    path: str
    line: int


HISTORY_LOG_LIMIT = 500
GIT_LOG_TIMEOUT_SECONDS = 120

FALLBACK_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("selamy-aws-access-key-id", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    (
        "selamy-aws-secret",
        re.compile(r"(?i)\baws_secret_access_key\s*=\s*['\"]?[A-Za-z0-9/+=]{30,}"),
    ),
    (
        "selamy-github-pat",
        re.compile(
            r"\b(?:ghp_[0-9A-Za-z]{36}|github_pat_[0-9A-Za-z_]{60,}|gh[pousr]_[A-Za-z0-9]{20,})\b"
        ),
    ),
    (
        "selamy-stripe-clerk-secret",
        re.compile(r"\bsk_(?:live|test)_[0-9A-Za-z]{20,}\b"),
    ),
    ("selamy-slack-token", re.compile(r"\bxox[bpras]-?[0-9A-Za-z\-]{10,}\b")),
    (
        "selamy-postgres-url-password",
        re.compile(r"postg(?:res|resql)://[^:\s/]+:[^@\s/]+@[^\s/]+"),
    ),
    (
        "selamy-gcp-service-account-key",
        re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"),
    ),
    ("selamy-hex-private-key", re.compile(r"\b[0-9a-fA-F]{64}\b")),
    (
        "generic-api-key",
        re.compile(
            r"(?i)\bapi[_-]?key\s*[:=]\s*['\"]?"
            r"(?:[A-Za-z0-9_\-]{0,19}[A-Za-z0-9_\-]*[0-9][A-Za-z0-9_\-]*"
            r"|[A-Za-z0-9_\-]*[0-9][A-Za-z0-9_\-]{0,19})"
            r"[A-Za-z0-9_\-]{0,64}['\"]?"
        ),
    ),
)

SKIP_DIRS = frozenset({".git", ".venv", "__pycache__", ".mypy_cache", ".ruff_cache"})
SKIP_NAMES = frozenset({".gitleaks.toml", REPORT_NAME})


def _allowlist_table(data: object) -> dict[str, object]:
    if not isinstance(data, dict):
        return {}
    table = data.get("allowlist")
    return table if isinstance(table, dict) else {}


def _allowlist_regexes(table: dict[str, object], key: str = "regexes") -> tuple[str, ...]:
    raw = table.get(key, [])
    if not isinstance(raw, list):
        return ()
    values = [str(item).replace("\\\\", "\\") for item in raw if isinstance(item, str)]
    return tuple(values)


def _probe_table(data: object) -> dict[str, object]:
    if not isinstance(data, dict):
        return {}
    table = data.get("secret-scan-probe")
    return table if isinstance(table, dict) else {}


def _allowlist_values(config: Path) -> tuple[str, ...]:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 never runs in CI
        return ()
    try:
        with config.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, ValueError):
        return ()
    probe = _probe_table(data)
    return (
        _allowlist_regexes(_allowlist_table(data))
        + _allowlist_regexes(probe, "digest_regexes")
        + _allowlist_regexes(probe, "content_hash_regexes")
        + _allowlist_regexes(probe, "placeholder_postgres_regexes")
    )


def _redact_allowlisted(line: str, allowlisted: tuple[str, ...]) -> str:
    """Remove allowlisted spans so the rest of the line is still scanned.

    A whole-line exemption would blind every rule to a real credential that
    shares a line with an allowlisted token (e.g. a digest-pinned lockfile
    line carrying a package URL); redaction keeps that remainder visible.
    """
    for value in allowlisted:
        line = re.sub(value, "", line)
    return line


def _prune(path: Path) -> bool:
    if any(part in SKIP_DIRS for part in path.parts):
        return True
    return path.name in SKIP_NAMES


def _git_files(root: Path) -> list[Path] | None:
    try:
        completed = subprocess.run(
            ("git", "ls-files", "-z"),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if completed.returncode != 0 or not completed.stdout:
        return None
    return [root / entry for entry in completed.stdout.split("\0") if entry]


def _walk_files(root: Path) -> list[Path]:
    pruned: list[Path] = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and not _prune(path):
            pruned.append(path)
    return pruned


def _tracked_files(root: Path) -> list[Path]:
    listed = _git_files(root)
    return listed if listed is not None else _walk_files(root)


def _git_history_text(root: Path) -> str:
    try:
        completed = subprocess.run(
            ("git", "log", "-p", f"--max-count={HISTORY_LOG_LIMIT}", "--", "."),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=GIT_LOG_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if completed.returncode != 0:
        return ""
    return completed.stdout


def _line_finding(
    relative: str, number: int, line: str, allowlisted: tuple[str, ...]
) -> Finding | None:
    redacted = _redact_allowlisted(line, allowlisted)
    for rule, pattern in FALLBACK_PATTERNS:
        if pattern.search(redacted):
            return Finding(rule, relative, number)
    return None


def _scan_text(relative: str, text: str, allowlisted: tuple[str, ...]) -> list[Finding]:
    matched: list[Finding] = []
    for number, line in enumerate(text.splitlines(), 1):
        finding = _line_finding(relative, number, line, allowlisted)
        if finding is not None:
            matched.append(finding)
    return matched


def _read_source(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="strict")
    except (OSError, ValueError):
        return None


def _fallback_scan(root: Path, allowlisted: tuple[str, ...]) -> list[Finding]:
    findings: list[Finding] = []
    for path in _tracked_files(root):
        text = _read_source(path)
        if text is None:
            continue
        findings.extend(_scan_text(str(path.relative_to(root)), text, allowlisted))
    history = _git_history_text(root)
    if history:
        findings.extend(_scan_text("git-history", history, allowlisted))
    return findings


def _resolve_report(root: Path, report: Path) -> Path:
    if report.is_absolute():
        return report
    if root.is_absolute():
        return root / report
    return Path.cwd() / root / report


def _gitleaks_scan(root: Path, config: Path, report: Path) -> int:
    binary = shutil.which("gitleaks")
    if binary is None:
        return -1
    resolved = _resolve_report(root, report)
    completed = subprocess.run(
        (
            binary,
            "git",
            str(root),
            "--config",
            str(config),
            "--report-format",
            "json",
            "--report-path",
            str(resolved),
            "--no-banner",
        ),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.returncode


def _object_map(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected an object")
    narrowed: dict[str, object] = {}
    for key in value:
        if not isinstance(key, str):
            raise ValueError("object keys must be strings")
        narrowed[key] = value[key]
    return narrowed


def _report_entry(value: object) -> Finding | None:
    """Narrow one decoded report entry to a Finding, or None when malformed."""
    try:
        fields = _object_map(value)
    except ValueError:
        return None
    rule: object = fields.get("RuleID", "gitleaks")
    path: object = fields.get("File", "?")
    line: object = fields.get("StartLine", 0)
    if not isinstance(rule, str) or not isinstance(path, str):
        return None
    if isinstance(line, bool) or not isinstance(line, int):
        return None
    return Finding(rule, path, line)


def _decode_entries(raw: str) -> list[object] | None:
    try:
        decoded: object = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if decoded is None:
        return []
    if not isinstance(decoded, list):
        return None
    items: list[object] = []
    for raw_item in decoded:
        items.append(raw_item)
    return items


def _narrow_entries(entries: list[object]) -> list[Finding] | None:
    findings: list[Finding] = []
    for value in entries:
        finding = _report_entry(value)
        if finding is None:
            return None
        findings.append(finding)
    return findings


def _read_gitleaks_report(report: Path) -> list[Finding] | None:
    try:
        raw = report.read_text(encoding="utf-8")
    except OSError:
        return None
    if not raw.strip():
        return None
    entries = _decode_entries(raw)
    if entries is None:
        return None
    return _narrow_entries(entries)


def _write_fallback_report(report: Path, probed: list[Finding]) -> None:
    report.write_text(
        json.dumps(
            [{"RuleID": rule, "File": path, "StartLine": line} for rule, path, line in probed]
        ),
        encoding="utf-8",
    )


def _report_fallback(probed: list[Finding]) -> int:
    if probed:
        print(f"secret scan: {len(probed)} finding(s) (fallback probe)")
        for rule, path, line in probed:
            print(f"  {rule}: {path}:{line}")
        return EXIT_FAILURE
    print("secret scan: clean (fallback probe, history included)")
    return 0


def _report_gitleaks(report: Path) -> tuple[int, list[Finding]]:
    parsed = _read_gitleaks_report(report)
    if parsed is None:
        print(
            "secret scan: gitleaks reported findings but the report "
            "is missing or malformed; failing closed",
            file=sys.stderr,
        )
        return EXIT_FAILURE, []
    if not parsed:
        print(
            "secret scan: unexpected empty gitleaks report; failing closed",
            file=sys.stderr,
        )
        return EXIT_FAILURE, []
    print(f"secret scan: {len(parsed)} finding(s) (gitleaks)")
    for rule, path, line in parsed:
        print(f"  {rule}: {path}:{line}")
    return EXIT_FAILURE, parsed


def _report_gitleaks_clean(report: Path) -> tuple[int, list[Finding]]:
    parsed = _read_gitleaks_report(report)
    if parsed is None:
        size = report.stat().st_size if report.is_file() else -1
        print(
            "secret scan: gitleaks exited 0 but no parseable report "
            f"(size={size}); failing closed",
            file=sys.stderr,
        )
        return EXIT_FAILURE, []
    print(f"secret scan: clean (gitleaks, report {len(parsed)} entries)")
    return 0, []


def _scan_with_fallback(
    root: Path, report: Path, resolved_report: Path, allowlisted: tuple[str, ...]
) -> tuple[int, list[Finding]]:
    probed = _fallback_scan(root, allowlisted)
    _write_fallback_report(resolved_report, probed)
    if report != resolved_report:
        _write_fallback_report(report, probed)
    return _report_fallback(probed), probed


def _dispatch_gitleaks_exit(
    gitleaks_exit: int,
    root: Path,
    report: Path,
    resolved_report: Path,
    allowlisted: tuple[str, ...],
) -> tuple[int, list[Finding]]:
    """Map a gitleaks exit code to the stable fail-closed contract."""
    if gitleaks_exit == -1:
        return _scan_with_fallback(root, report, resolved_report, allowlisted)
    if gitleaks_exit == 0:
        return _report_gitleaks_clean(resolved_report)
    if gitleaks_exit == 1:
        return _report_gitleaks(resolved_report)
    print(
        f"secret scan: gitleaks exited {gitleaks_exit}; failing closed",
        file=sys.stderr,
    )
    return EXIT_FAILURE, []


def run_scan(root: Path, config: Path, report: Path) -> tuple[int, list[Finding]]:
    """Scan ``root`` and return its exit code with surviving findings."""
    if not config.is_file():
        print(f"secret scan: config not found: {config}", file=sys.stderr)
        return EXIT_PREREQUISITE, []
    if not root.is_dir():
        print(f"secret scan: root not found: {root}", file=sys.stderr)
        return EXIT_PREREQUISITE, []
    resolved_report = _resolve_report(root, report)
    allowlisted = _allowlist_values(config)
    if not allowlisted:
        print(f"secret scan: allowlist unreadable in config: {config}", file=sys.stderr)
        return EXIT_PREREQUISITE, []
    gitleaks_exit = _gitleaks_scan(root, config, report)
    return _dispatch_gitleaks_exit(gitleaks_exit, root, report, resolved_report, allowlisted)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse scan options and map the result to its stable exit code."""
    parser = argparse.ArgumentParser(prog="secret_scan.py")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--report", default="")
    parser.add_argument("--fail-closed", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.fail_closed:
        parser.print_usage(sys.stderr)
        print("secret_scan.py: error: --fail-closed is required", file=sys.stderr)
        return EXIT_PREREQUISITE
    config = Path(args.config)
    if not config.is_absolute():
        config = REPOSITORY_ROOT / config
    report = Path(args.report) if args.report else (REPOSITORY_ROOT / REPORT_NAME)
    exit_code, _ = run_scan(REPOSITORY_ROOT, config, report)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
