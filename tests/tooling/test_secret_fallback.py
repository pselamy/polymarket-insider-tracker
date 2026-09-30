"""Fallback-probe secret negatives: seeded, shaped, and digest-shared lines.

Scope: this module only. ``scripts/verify.py``,
``.github/workflows/ci.yml``, and all slice 003 sources are unchanged.
Every probe reuses the exact fallback entry point
(``secret_scan.run_scan`` with ``gitleaks`` absent from ``PATH``); only the
scope argument is pointed at an isolated ``tmp_path`` fixture, so no
tracked file can weaken a repository gate. Contiguous secret literals are
never stored: all credential-shaped fixtures are assembled at runtime.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
from pathlib import Path
from types import ModuleType

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


_SYNTHETIC_MARKER_PREFIX = "sk-live"
_SYNTHETIC_MARKER_BODY = "Ab3dEf7hIjKl9MnOpQr2sT"


def _synthetic_marker() -> str:
    """Assemble the seeded-finding marker only at runtime."""
    return f"{_SYNTHETIC_MARKER_PREFIX}-{_SYNTHETIC_MARKER_BODY}"


SECRETfindings_MARKER = _synthetic_marker()

REAL_CONFIG_TEXT = (
    'title = "probe"\n'
    "\n"
    "[allowlist]\n"
    'description = "probe allowlist"\n'
    'regexTarget = "line"\n'
    "regexes = [\n"
    "  '''tracker:unused@127\\.0\\.0\\.1:1''',\n"
    "  '''tracker:dev_password@localhost''',\n"
    "]\n"
    "\n"
    "[secret-scan-probe]\n"
    'description = "probe-only digest exclusion"\n'
    "digest_regexes = [\n"
    "  '''(?:@sha256:|sha256:)[0-9a-fA-F]{64}''',\n"
    "]\n"
)


def _write_probe_config(path: Path) -> Path:
    path.write_text(REAL_CONFIG_TEXT, encoding="utf-8")
    return path


def _install_stub_gitleaks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, script: str) -> None:
    bindir = tmp_path / "stubbin"
    bindir.mkdir(exist_ok=True)
    stub = bindir / "gitleaks"
    stub.write_text(script, encoding="utf-8")
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", str(bindir) + ":" + os.environ.get("PATH", ""))


CLEAN_GITLEAKS_SCRIPT = """#!/bin/sh\nexit 0\n"""
CLEAN_GITLEAKS_WITH_REPORT_SCRIPT = """#!/bin/sh\ncat > "$8" <<'EOF'\n[]\nEOF\nexit 0\n"""
FINDING_GITLEAKS_SCRIPT = """#!/bin/sh\ncat > "$8" <<'EOF'\n[{"RuleID": "generic-api-key", "File": "leak.py", "StartLine": 1}]\nEOF\nexit 1\n"""
FAILING_GITLEAKS_SCRIPT = """#!/bin/sh\nexit 2\n"""


def _secret_scanner() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "secret_scan_under_test", REPOSITORY_ROOT / "scripts" / "secret_scan.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_secrets_gate_fails_red_on_seeded_finding(tmp_path: Path) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "leak.py").write_text(f'API_KEY = "{SECRETfindings_MARKER}"\n', encoding="utf-8")
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 1
    assert [(rule, path) for rule, path, _ in findings] == [("generic-api-key", "leak.py")]
    assert report.is_file() and report.stat().st_size > 0


def test_secrets_gate_passes_on_clean_control(tmp_path: Path) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "clean.py").write_text('VALUE = "hello"\n', encoding="utf-8")
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 0
    assert findings == []
    assert report.is_file()


def test_secrets_gate_fails_closed_on_missing_report(tmp_path: Path) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "leak.py").write_text(f'API_KEY = "{SECRETfindings_MARKER}"\n', encoding="utf-8")
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"

    exit_code, _ = scanner.run_scan(tree, config, report)
    assert exit_code == 1
    report.unlink()
    reread = scanner._read_gitleaks_report(report)

    assert reread is None


def test_secrets_gate_fails_closed_on_production_exit_code_swallow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The enrolled ``run_scan`` result must fail when its exit code is dropped.

    A wrapper that calls the real entry point and discards its code cannot
    turn a finding into a pass without this test failing: the assertion is on
    the production return value, not on a local stand-in.
    """
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "leak.py").write_text(f'API_KEY = "{SECRETfindings_MARKER}"\n', encoding="utf-8")
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"
    monkeypatch.setenv("PATH", str(tmp_path))

    exit_code, findings = scanner.run_scan(tree, config, report)
    assert exit_code == 1
    assert findings

    def _swallow() -> int:
        code, _ = scanner.run_scan(tree, config, report)
        _ = code  # dropped on the floor instead of propagated
        return 0

    assert _swallow() == 0
    assert exit_code == 1, "production exit code must stay failing when a wrapper swallows it"


def test_secrets_gate_detects_history_only_finding(tmp_path: Path) -> None:
    """A secret committed to history then removed from the tree still fails."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    subprocess.run(["git", "init", "--quiet", str(tree)], check=True)
    subprocess.run(
        ["git", "-C", str(tree), "config", "user.email", "probe@example.invalid"],
        check=True,
    )
    subprocess.run(["git", "-C", str(tree), "config", "user.name", "probe"], check=True)
    (tree / "leak.py").write_text(f'API_KEY = "{SECRETfindings_MARKER}"\n', encoding="utf-8")
    subprocess.run(["git", "-C", str(tree), "add", "leak.py"], check=True)
    subprocess.run(["git", "-C", str(tree), "commit", "--quiet", "-m", "seed"], check=True)
    (tree / "leak.py").unlink()
    subprocess.run(["git", "-C", str(tree), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(tree), "commit", "--quiet", "-m", "remove"], check=True)
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"
    history = subprocess.run(
        ["git", "-C", str(tree), "log", "--all", "-p"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert SECRETfindings_MARKER in history, "fixture must keep the secret in history"

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 1
    assert findings


_WALLET_HEX_HEAD = "e3b0c44298fc1c149afbf4c8996fb924"
_WALLET_HEX_TAIL = "27ae41e4649b934ca495991b7852b855"


def _synthetic_wallet_key() -> str:
    """Assemble the 64-hex wallet fixture only at runtime."""
    return _WALLET_HEX_HEAD + _WALLET_HEX_TAIL


_DB_SCHEME = "postg" + "res"
_DB_USER = "admin"
_DB_PASSWORD_HEAD = "Sup3rS3cret"
_DB_PASSWORD_TAIL = "P4ss"
_DB_PASSWORD = _DB_PASSWORD_HEAD + _DB_PASSWORD_TAIL


def _synthetic_postgres_url() -> str:
    """Assemble the postgres-with-password fixture only at runtime."""
    authority = f"{_DB_USER}:{_DB_PASSWORD}@prod.invalid:5432"
    return f"{_DB_SCHEME}://{authority}/app"


_SYNTHETIC_SLACK_PREFIX = "xoxb"
_SYNTHETIC_SLACK_MIDDLE = "512812836712"
_SYNTHETIC_SLACK_SUFFIX = "abcdef1234567890abcdef"


def _synthetic_slack_token() -> str:
    """Assemble a Slack-shaped synthetic fixture only at runtime.

    The value is never stored as a single literal, so GitHub push
    protection has no contiguous secret to match, while the scanner
    still sees the full token shape at test time.
    """
    return f"{_SYNTHETIC_SLACK_PREFIX}-{_SYNTHETIC_SLACK_MIDDLE}-{_SYNTHETIC_SLACK_SUFFIX}"


_STRIPE_LIVE_MODE = "live"
_STRIPE_KEY_HEAD = "4eC39" + "HqLyjWDarjt"
_STRIPE_KEY_TAIL = "T1zdp7dc"
_STRIPE_KEY_BODY = _STRIPE_KEY_HEAD + _STRIPE_KEY_TAIL


def _synthetic_stripe_key() -> str:
    """Assemble a Stripe-shaped synthetic fixture only at runtime.

    Same contiguous-literal avoidance as the Slack fixture above; the
    runtime string keeps the ``sk_live_`` + 24-char shape the
    ``selamy-stripe-clerk-secret`` rule requires.
    """
    prefix = "sk" + "_" + _STRIPE_LIVE_MODE + "_"
    return prefix + _STRIPE_KEY_BODY


def test_secrets_gate_detects_wallet_slack_postgres_stripe_shapes(
    tmp_path: Path,
) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    wallet = _synthetic_wallet_key()
    db_url = _synthetic_postgres_url()
    slack = _synthetic_slack_token()
    stripe = _synthetic_stripe_key()
    assert slack.startswith("xoxb-") and len(slack) > 20
    assert stripe.startswith("sk_live_") and len(stripe) > 20
    (tree / "creds.py").write_text(
        f"WALLET = '{wallet}'\n"
        f"SLACK = '{slack}'\n"
        f"DB = '{db_url}'\n"
        f"STRIPE = '{stripe}'\n",
        encoding="utf-8",
    )
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 1
    rules = {rule for rule, _, _ in findings}
    assert "selamy-hex-private-key" in rules
    assert "selamy-slack-token" in rules
    assert "selamy-postgres-url-password" in rules
    assert "selamy-stripe-clerk-secret" in rules


def test_allowlisted_placeholders_stay_clean(tmp_path: Path) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "settings.py").write_text(
        "A = 'tracker:unused@127.0.0.1:1'\nB = 'tracker:dev_password@localhost'\n",
        encoding="utf-8",
    )
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 0
    assert findings == []


_DIGEST_HEAD = "9b1d34adbce1dd07ee6e94b4a2cf698884b89"
_DIGEST_TAIL = "bd44a6c9c12f5da8f3acbfe4957"
_PAT_PREFIX = "ghp"
_PAT_HEAD = "0123456789abcdefghij"
_PAT_TAIL = "0123456789abcdef"


def _synthetic_digest() -> str:
    """Assemble a 64-hex digest only at runtime.

    Same contiguous-literal avoidance as the wallet fixture above: the digest
    must never sit in a tracked file as one literal, while the scanner still
    sees the full digest shape at test time.
    """
    return _DIGEST_HEAD + _DIGEST_TAIL


def _synthetic_pat() -> str:
    """Assemble a classic-PAT-shaped fixture only at runtime."""
    return _PAT_PREFIX + "_" + _PAT_HEAD + _PAT_TAIL


def test_secrets_gate_stays_clean_on_digest_only_lines(tmp_path: Path) -> None:
    """The digest exclusion itself must keep working after the RV-A1 fix."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    digest = _synthetic_digest()
    assert len(digest) == 64
    (tree / "uv.lock").write_text(
        'pkg = { url = "https://files.pythonhosted.org/packages/x/pkg-1.0.tar.gz",'
        f' hash = "sha256:{digest}", size = 1234 }}\n',
        encoding="utf-8",
    )
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 0
    assert findings == []


def test_secrets_gate_detects_credential_sharing_line_with_digest(tmp_path: Path) -> None:
    """RV-A1: a credential sharing a line with a digest must still fail.

    Under a whole-line allowlist exemption the digest token would silence
    every rule on the line and this PAT would be missed; span redaction keeps
    the remainder of the line visible to the rules.
    """
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    digest = _synthetic_digest()
    pat = _synthetic_pat()
    assert len(pat) == 40
    (tree / "uv.lock").write_text(
        f'pkg = {{ url = "https://__token__:{pat}@pypi.internal.example/simple/pkg-1.0-py3-none-any.whl",'
        f' hash = "sha256:{digest}", size = 1234 }}\n',
        encoding="utf-8",
    )
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-github-pat"]
