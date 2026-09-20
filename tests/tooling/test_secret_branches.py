"""Secret-scan branch contract: config authority and gitleaks dispatch.

Scope: this module only. The single allowlist authority lives in
``.gitleaks.toml`` (``[allowlist]`` plus the probe-only
``[secret-scan-probe]`` tables); this module never hardcodes allowlist
values. Gitleaks-branch tests run against stub ``gitleaks`` binaries on a
private ``PATH`` so the real gate command is never weakened.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _secret_scanner() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "secret_scan_under_test", REPOSITORY_ROOT / "scripts" / "secret_scan.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_REAL_CONFIG_TEXT = (
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
    path.write_text(_REAL_CONFIG_TEXT, encoding="utf-8")
    return path


def _install_stub_gitleaks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, script: str) -> None:
    bindir = tmp_path / "stubbin"
    bindir.mkdir(exist_ok=True)
    stub = bindir / "gitleaks"
    stub.write_text(script, encoding="utf-8")
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", str(bindir) + ":" + os.environ.get("PATH", ""))


CLEAN_GITLEAKS_SCRIPT = "#!/bin/sh\nexit 0\n"
CLEAN_GITLEAKS_WITH_REPORT_SCRIPT = "#!/bin/sh\ncat > \"$8\" <<'EOF'\n[]\nEOF\nexit 0\n"
FINDING_GITLEAKS_SCRIPT = '#!/bin/sh\ncat > "$8" <<\'EOF\'\n[{"RuleID": "generic-api-key", "File": "leak.py", "StartLine": 1}]\nEOF\nexit 1\n'
FAILING_GITLEAKS_SCRIPT = "#!/bin/sh\nexit 2\n"


def test_allowlist_values_come_from_config_not_module_constants(tmp_path: Path) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "clean.py").write_text('VALUE = "hello"\n', encoding="utf-8")
    report = tmp_path / "report.json"

    assert not hasattr(scanner, "ALLOWLIST_VALUES"), "single allowlist authority lives in config"

    missing = tmp_path / "missing.toml"
    exit_code, _ = scanner.run_scan(tree, missing, report)
    assert exit_code == scanner.EXIT_PREREQUISITE

    empty = tmp_path / "empty.toml"
    empty.write_text('title = "probe"\n', encoding="utf-8")
    exit_code, _ = scanner.run_scan(tree, empty, report)
    assert exit_code == scanner.EXIT_PREREQUISITE


def test_gitleaks_branch_reports_findings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "clean.py").write_text('VALUE = "hello"\n', encoding="utf-8")
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"
    _install_stub_gitleaks(tmp_path, monkeypatch, FINDING_GITLEAKS_SCRIPT)

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 1
    assert [(rule, path) for rule, path, _ in findings] == [("generic-api-key", "leak.py")]
    assert report.is_file() and report.stat().st_size > 0


def test_gitleaks_clean_path_requires_parseable_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exit 0 without a report is unproven and fails closed, not clean."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "clean.py").write_text('VALUE = "hello"\n', encoding="utf-8")
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"
    _install_stub_gitleaks(tmp_path, monkeypatch, CLEAN_GITLEAKS_SCRIPT)

    exit_code, _ = scanner.run_scan(tree, config, report)

    assert exit_code == 1
    assert not report.is_file() or report.stat().st_size == 0


def test_gitleaks_clean_with_report_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "clean.py").write_text('VALUE = "hello"\n', encoding="utf-8")
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"
    _install_stub_gitleaks(tmp_path, monkeypatch, CLEAN_GITLEAKS_WITH_REPORT_SCRIPT)

    exit_code, findings = scanner.run_scan(tree, config, report)

    assert exit_code == 0
    assert findings == []
    assert report.is_file()


def test_gitleaks_unexpected_exit_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "clean.py").write_text('VALUE = "hello"\n', encoding="utf-8")
    config = _write_probe_config(tmp_path / ".gitleaks.toml")
    report = tmp_path / "report.json"
    _install_stub_gitleaks(tmp_path, monkeypatch, FAILING_GITLEAKS_SCRIPT)

    exit_code, _ = scanner.run_scan(tree, config, report)

    assert exit_code == 1


def test_gitleaks_invocation_uses_git_subcommand(tmp_path: Path) -> None:
    import inspect

    scanner = _secret_scanner()
    source = inspect.getsource(scanner._gitleaks_scan)

    assert '"git"' in source
    assert '"detect"' not in source


def test_shipped_config_exposes_probe_digest_table(tmp_path: Path) -> None:
    """The shipped ``.gitleaks.toml`` must expose a probe digest regex.

    Regression guard for the deleted-``[secret-scan-probe]`` class: every
    digest/allowlist unit test builds a synthetic config, so only a test
    that loads the repository's real config can detect a deleted table.
    """
    scanner = _secret_scanner()
    config = REPOSITORY_ROOT / ".gitleaks.toml"

    assert config.is_file(), "shipped .gitleaks.toml is the scan authority"
    values = scanner._allowlist_values(config)

    assert any("(?:@sha256:|sha256:)" in value for value in values), (
        "probe digest table missing from shipped config: "
        "[secret-scan-probe] digest_regexes must stay enrolled"
    )
