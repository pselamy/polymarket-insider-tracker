"""The security gate scans the tree plus post-cut-over history and reports osv only."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from quality import security_gate

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CUTOVER_SHA = (REPOSITORY_ROOT / "quality" / security_gate.CUTOVER).read_text().strip()
NEEDS_MISE = pytest.mark.skipif(shutil.which("mise") is None, reason="mise is not provisioned")


def git(root: Path, *arguments: str) -> str:
    environment = ["-c", "user.name=t", "-c", "user.email=t@example.test"]
    done = subprocess.run(
        ["git", *environment, *arguments], cwd=root, check=True, capture_output=True, text=True
    )
    return done.stdout.strip()


def commit(root: Path, name: str, content: str) -> str:
    (root / name).write_text(content)
    git(root, "add", name)
    git(root, "commit", "-q", "-m", name)
    return git(root, "rev-parse", "HEAD")


def scratch_repository(root: Path) -> str:
    """A repository carrying the real scanner configuration and one clean commit."""
    git(root, "init", "-q")
    (root / "quality").mkdir()
    for name in ("gitleaks.toml", "gitleaks.ignore"):
        shutil.copy(REPOSITORY_ROOT / "quality" / name, root / "quality" / name)
    for name in ("mise.toml", "mise.lock"):
        shutil.copy(REPOSITORY_ROOT / name, root / name)
    (root / ".quality-results").mkdir()
    return commit(root, "clean.txt", "nothing to see\n")


def planted_secret() -> str:
    """Assembled at run time so this source file never contains a scannable credential."""
    return "ghp_" + "".join(chr(ord("a") + (index * 7) % 26) for index in range(36))


def declare_cutover(root: Path, sha: str) -> None:
    (root / "quality" / security_gate.CUTOVER).write_text(sha + "\n")


def test_recorded_cutover_is_an_ancestor_of_the_checkout() -> None:
    assert security_gate.cutover(REPOSITORY_ROOT) == CUTOVER_SHA


@pytest.mark.parametrize("text", ["", "abc123", "G" * 40, CUTOVER_SHA.upper(), CUTOVER_SHA[:39]])
def test_cutover_must_be_a_full_lowercase_sha(tmp_path: Path, text: str) -> None:
    (tmp_path / "quality").mkdir()
    declare_cutover(tmp_path, text)
    with pytest.raises(ValueError, match="full lowercase commit SHA"):
        security_gate.cutover(tmp_path)


def test_cutover_outside_the_history_fails_closed(tmp_path: Path) -> None:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    git(scratch, "init", "-q")
    commit(scratch, "a.txt", "a\n")
    (scratch / "quality").mkdir()
    declare_cutover(scratch, "0" * 40)
    with pytest.raises(subprocess.CalledProcessError):
        security_gate.cutover(scratch)


def test_audit_summary_counts_packages_and_groups() -> None:
    report: object = json.loads(
        '{"results": [{"packages": [{"groups": [{}, {}]}, {}, {"groups": [{}]}]},'
        ' {"packages": [{"groups": []}]}]}'
    )
    assert security_gate.audit_summary(report) == "2 packages, 3 advisory groups"


def write_audit(root: Path, report: object) -> None:
    (root / ".quality-results").mkdir(exist_ok=True)
    (root / ".quality-results" / "dependencies.json").write_text(json.dumps(report))


def test_advisories_are_reported_without_failing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def found(_root: Path, _timeout: float) -> None:
        write_audit(tmp_path, json.loads('{"results": [{"packages": [{"groups": [{}]}]}]}'))
        raise subprocess.CalledProcessError(security_gate.VULNERABILITIES_FOUND, ["osv-scanner"])

    monkeypatch.setattr(security_gate, "dependencies", found)
    security_gate.audit_report_only(tmp_path, 1.0)
    assert "report only, not enforced): 1 packages, 1 advisory groups" in capsys.readouterr().out


def test_a_crashed_dependency_scan_still_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def crashed(_root: Path, _timeout: float) -> None:
        raise subprocess.CalledProcessError(127, ["osv-scanner"])

    monkeypatch.setattr(security_gate, "dependencies", crashed)
    with pytest.raises(subprocess.CalledProcessError):
        security_gate.audit_report_only(tmp_path, 1.0)


def test_a_missing_dependency_report_still_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def silent(_root: Path, _timeout: float) -> None:
        return None

    monkeypatch.setattr(security_gate, "dependencies", silent)
    with pytest.raises(FileNotFoundError):
        security_gate.audit_report_only(tmp_path, 1.0)


@NEEDS_MISE
def test_clean_tree_and_history_pass(tmp_path: Path) -> None:
    declare_cutover(tmp_path, scratch_repository(tmp_path))
    commit(tmp_path, "later.txt", "still nothing\n")
    security_gate.secrets(tmp_path, 120.0)


@NEEDS_MISE
def test_secret_committed_after_the_cutover_fails_the_gate(tmp_path: Path) -> None:
    declare_cutover(tmp_path, scratch_repository(tmp_path))
    commit(tmp_path, "config.txt", f"api_token = {planted_secret()}\n")
    git(tmp_path, "rm", "-q", "config.txt")
    git(tmp_path, "commit", "-q", "-m", "remove it again")
    with pytest.raises(subprocess.CalledProcessError):
        security_gate.secrets(tmp_path, 120.0)


@NEEDS_MISE
def test_secret_in_the_working_tree_fails_the_gate(tmp_path: Path) -> None:
    declare_cutover(tmp_path, scratch_repository(tmp_path))
    (tmp_path / "untracked.txt").write_text(f"api_token = {planted_secret()}\n")
    with pytest.raises(subprocess.CalledProcessError):
        security_gate.secrets(tmp_path, 120.0)


@NEEDS_MISE
def test_secret_committed_before_the_cutover_is_not_scanned(tmp_path: Path) -> None:
    scratch_repository(tmp_path)
    old = commit(tmp_path, "old.txt", f"api_token = {planted_secret()}\n")
    git(tmp_path, "rm", "-q", "old.txt")
    declare_cutover(tmp_path, old)
    git(tmp_path, "add", "quality")
    git(tmp_path, "commit", "-q", "-m", "cut over after the old secret")
    security_gate.secrets(tmp_path, 120.0)
