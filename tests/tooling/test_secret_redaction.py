"""Probe-only redaction: content hashes and placeholder URLs stay clean.

Scope: this module only. The fallback probe flags bare 64-hex tokens and
postgres URLs with passwords; both shapes collide with deterministic
repository content (content-addressed file hashes, redaction-test
placeholder URLs). The probe-only ``[secret-scan-probe]`` tables redact
exactly those deterministic shapes as spans — the rest of each line is
still scanned, so a real credential sharing the line still fails.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _secret_scanner() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "secret_scan_under_test", REPOSITORY_ROOT / "scripts" / "secret_scan.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_shipped_config_copy(path: Path) -> Path:
    """Copy the shipped config so the test reads the real authority."""
    path.write_text((REPOSITORY_ROOT / ".gitleaks.toml").read_text(encoding="utf-8"))
    return path


def _run_tree(scanner: ModuleType, tree: Path, config: Path) -> tuple[int, list[object]]:
    report = tree.parent / "report.json"
    return scanner.run_scan(tree, config, report)


def test_content_hash_json_value_lines_stay_clean(tmp_path: Path) -> None:
    """Manifest/TRACEABILITY digest maps carry `"key": "<64hex>"`, not keys."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "codex.manifest.json").write_text(
        '{\n  "files": {\n'
        '    "a/SKILL.md": "f9828d73a545500680c4f6f1e1eb8b0f9124578430128045170a8d7e388a944b",\n'
        '    "b/SKILL.md": "d3aa120d6b5d7718c4c6d792dd41bee66254bd4bb82f88f3067f1d1c4e9d2dc2"\n'
        "  }\n}\n",
        encoding="utf-8",
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 0
    assert findings == []


def test_content_hash_tick_wrapped_lines_stay_clean(tmp_path: Path) -> None:
    """Markdown evidence tables wrap file hashes in backticks."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "feasibility.md").write_text(
        "| `probe.json` |"
        " `72556dcf585c713f1d3813ae9f7d9ad52b5a49170c3c9b9e303d4b6738f4eff0` |\n",
        encoding="utf-8",
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 0
    assert findings == []


def test_placeholder_postgres_urls_stay_clean(tmp_path: Path) -> None:
    """Redaction-test placeholder URLs carry no credential value."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "settings.py").write_text(
        'A = "postgresql://user:pass@localhost/db"\n'
        'B = "postgresql+psycopg://tracker:password@localhost:5432/research"\n'
        'C = "postgresql://tracker:***@localhost:5432"\n',
        encoding="utf-8",
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 0
    assert findings == []


def test_bare_hex_wallet_key_still_fails(tmp_path: Path) -> None:
    """A bare 64-hex value with no digest-map spelling is still a finding."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    wallet = "e3b0c44298fc1c149afbf4c8996fb924" + "27ae41e4649b934ca495991b7852b855"
    (tree / "creds.py").write_text(f"WALLET = '{wallet}'\n", encoding="utf-8")
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-hex-private-key"]


def test_realistic_postgres_password_still_fails(tmp_path: Path) -> None:
    """A non-placeholder password on any host still fails the gate."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "creds.py").write_text(
        "DB = 'postgres://admin:Sup3rS3cretP4ss@prod.invalid:5432/app'\n",
        encoding="utf-8",
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-postgres-url-password"]


def test_placeholder_password_on_prod_host_still_fails(tmp_path: Path) -> None:
    """A placeholder password outside the test hosts is not allowlisted."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "creds.py").write_text(
        "DB = 'postgresql://user:pass@prod.evil.example:5432/db'\n",
        encoding="utf-8",
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-postgres-url-password"]


def test_credential_sharing_line_with_content_hash_still_fails(tmp_path: Path) -> None:
    """Span redaction keeps the credential remainder of the line visible."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "probe.json").write_text(
        '{"url": "https://__token__:ghp_0123456789abcdefghij0123456789abcdef'
        '@pypi.internal.example/simple/pkg.whl", '
        '"sha": "9b1d34adbce1dd07ee6e94b4a2cf698884b89bd44a6c9c12f5da8f3acbfe4957"}\n',
        encoding="utf-8",
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-github-pat"]


def _synthetic_hex_credential() -> str:
    """Assemble a 64-hex credential only at runtime (never one literal)."""
    return "ab12cd34" * 8


def test_credential_in_private_key_json_value_still_fails(
    tmp_path: Path,
) -> None:
    """A'-B1: a 64-hex credential in JSON-value position must still fail."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    credential = _synthetic_hex_credential()
    (tree / "wallet_config.json").write_text(
        f'{{\n  "private_key": "{credential}"\n}}\n', encoding="utf-8"
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-hex-private-key"]


def test_credential_in_lone_backtick_position_still_fails(
    tmp_path: Path,
) -> None:
    """A'-B1: a 64-hex credential in backtick position must still fail."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    credential = _synthetic_hex_credential()
    (tree / "evidence.md").write_text(f"| conf | `{credential}` |\n", encoding="utf-8")
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-hex-private-key"]


def test_namespaced_credential_keys_still_fail(tmp_path: Path) -> None:
    """B-prime-B1: dotted/slashed credential keys must still fail.

    The prior dot/slash heuristic blinded "wallet.private_key",
    "app.secret_key", "secrets/wallet_key", "signing.key" and "hash_key":
    all five fired at head and were missed under the candidate.
    """
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    credential = _synthetic_hex_credential()
    namespaced = (
        "{\n"
        f'  "wallet.private_key": "{credential}",\n'
        f'  "app.secret_key": "{credential}",\n'
        f'  "secrets/wallet_key": "{credential}",\n'
        f'  "signing.key": "{credential}",\n'
        f'  "hash_key": "{credential}"\n'
        "}\n"
    )
    (tree / "wallet_config.json").write_text(namespaced, encoding="utf-8")
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-hex-private-key"] * 5


def test_extension_named_credential_key_still_fails(tmp_path: Path) -> None:
    """B-prime-B1 adversarial: a credential key ending in a supported
    extension is real credential context and must still fail."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    credential = _synthetic_hex_credential()
    (tree / "evil_config.json").write_text(
        f'{{\n  "evil.toml": "{credential}"\n}}\n', encoding="utf-8"
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-hex-private-key"]


def test_credential_named_evidence_row_still_fails(tmp_path: Path) -> None:
    """B-prime-B1 adversarial: an evidence row whose first cell is itself
    credential-named must still fail."""
    scanner = _secret_scanner()
    tree = tmp_path / "tree"
    tree.mkdir()
    credential = _synthetic_hex_credential()
    (tree / "evidence.md").write_text(
        f"| `wallet.private_key` | `{credential}` |\n", encoding="utf-8"
    )
    config = _write_shipped_config_copy(tmp_path / ".gitleaks.toml")

    exit_code, findings = _run_tree(scanner, tree, config)

    assert exit_code == 1
    assert [rule for rule, _, _ in findings] == ["selamy-hex-private-key"]
