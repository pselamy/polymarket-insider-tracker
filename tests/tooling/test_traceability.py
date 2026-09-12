"""T5 receipt provenance, exact source binding, and immutable history contracts."""

from __future__ import annotations

import copy
import importlib.util
import json
import shlex
import subprocess
from hashlib import sha256
from pathlib import Path
from types import ModuleType

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
LEDGERS = tuple(
    REPOSITORY_ROOT / "specs" / name / "evidence/TRACEABILITY.json"
    for name in ("002-reproducible-runtime", "003-safe-observable-operation")
)
NODE = "tests/tooling/test_verify.py::test_profile_membership_and_ordering_are_exact"
SKIPPED = (
    "tests/integration/test_runtime_services.py::"
    "test_real_postgres_redis_and_disposable_migration_cycle"
)


def _validator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "traceability_under_test", REPOSITORY_ROOT / "scripts/traceability.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _head(root: Path = REPOSITORY_ROOT) -> str:
    return subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _document(
    ledger: Path = LEDGERS[0], node: str = NODE, root: Path = REPOSITORY_ROOT
) -> dict[str, object]:
    ledger = root / ledger.relative_to(REPOSITORY_ROOT)
    validator = _validator()
    context = validator.LedgerContext(root, ledger, expected_revision=_head(root))
    old = validator.object_map(json.loads(ledger.read_text()))
    identity = validator.source_identity(context)
    rows = validator.object_list(old["rows"])
    row = {
        "requirement": "T5-R1-R2-R3",
        "scenario": "Real harness execution",
        "negative_scenario": "Forged or skipped outcomes cannot earn credit",
        "test": [node],
        "run": shlex.join(validator.execution_command([node])),
        "source": identity,
        "config": {
            path: sha256((root / path).read_bytes()).hexdigest()
            for path in ("pyproject.toml", "uv.lock")
        },
        "result": "passed",
        "artifact_sha256": {
            "scripts/traceability.py": sha256(
                (root / "scripts/traceability.py").read_bytes()
            ).hexdigest()
        },
        "state": "exact-head-passed",
    }
    return {
        "historical_manifest": old["manifest"],
        "manifest": {
            **identity,
            "slice": ledger.parents[1].name,
            "history_revision": validator.BASELINE,
        },
        "rows": [*rows, row],
    }


def _check(
    document: object,
    output: Path | None = None,
    ledger: Path = LEDGERS[0],
    root: Path = REPOSITORY_ROOT,
):
    validator = _validator()
    ledger = root / ledger.relative_to(REPOSITORY_ROOT)
    context = validator.LedgerContext(root, ledger, frozenset({NODE}), frozenset(), _head(root))
    return validator.validate_ledger_document(document, context, execution_output=output)


@pytest.fixture
def committed_workspace(tmp_path: Path) -> Path:
    clone = tmp_path / "source"
    subprocess.run(
        ["git", "clone", "--quiet", "--no-hardlinks", str(REPOSITORY_ROOT), str(clone)], check=True
    )
    return clone


def _last(document: dict[str, object]) -> dict[str, object]:
    rows = document["rows"]
    assert isinstance(rows, list)
    row = rows[-1]
    assert isinstance(row, dict)
    return row


def test_ledgers_exist_and_validate_at_exact_head(
    tmp_path: Path, committed_workspace: Path
) -> None:
    for index, ledger in enumerate(LEDGERS):
        document = _document(ledger, root=committed_workspace)
        output = tmp_path / str(index)
        result = _check(document, output, ledger, committed_workspace)
        assert result.ok, result.errors
        receipt = json.loads((output / "receipt.json").read_text())
        assert receipt["collected"] == [NODE]
        assert receipt["reports"][NODE] == {
            "setup": "passed",
            "call": "passed",
            "teardown": "passed",
        }
        assert "1 passed" in (output / "stdout").read_text()


def test_ledger_states_are_enumerated() -> None:
    validator = _validator()
    for ledger in LEDGERS:
        document = validator.object_map(json.loads(ledger.read_text()))
        for row in validator.object_list(document["rows"]):
            assert validator.object_map(row)["state"] in validator.STATES


def test_ledger_has_no_dangling_test_or_artifact_paths() -> None:
    validator = _validator()
    for ledger in LEDGERS:
        document = validator.object_map(json.loads(ledger.read_text()))
        for row in validator.object_list(document["rows"]):
            _assert_paths(validator.object_map(row))


def _assert_paths(row: dict[str, object]) -> None:
    validator = _validator()
    paths = [
        entry.split("::")[0]
        for entry in validator.strings(row["test"])
        if not entry.startswith("gate:")
    ]
    paths.extend(validator.object_map(row["artifact_sha256"]))
    assert all((REPOSITORY_ROOT / path).is_file() for path in paths)


def test_skipped_test_cited_as_exact_head_passed_fails(
    tmp_path: Path, committed_workspace: Path
) -> None:
    output = tmp_path / "skip"
    result = _check(
        _document(node=SKIPPED, root=committed_workspace), output, root=committed_workspace
    )
    assert not result.ok
    assert "skipped/not-run" in str(result.errors)
    receipt = json.loads((output / "receipt.json").read_text())
    assert receipt["reports"][SKIPPED]["call"] == "skipped"


def test_missing_test_cited_as_exact_head_passed_fails(
    tmp_path: Path, committed_workspace: Path
) -> None:
    result = _check(
        _document(node="tests/tooling/test_verify.py::test_no_such_case", root=committed_workspace),
        tmp_path / "missing",
        root=committed_workspace,
    )
    assert not result.ok
    assert "collected test IDs differ" in str(result.errors)


def test_dangling_test_path_fails() -> None:
    result = _check(_document(node="tests/tooling/test_no_such_file.py::test_absent"))
    assert not result.ok
    assert "dangling test path" in str(result.errors)


def test_self_referential_artifact_hash_fails() -> None:
    document = _document()
    _last(document)["artifact_sha256"] = {
        LEDGERS[0].relative_to(REPOSITORY_ROOT).as_posix(): "0" * 64
    }
    result = _check(document)
    assert not result.ok
    assert "self-referential" in str(result.errors)


def test_unknown_state_fails() -> None:
    document = _document()
    _last(document)["state"] = "passed"
    result = _check(document)
    assert not result.ok
    assert "unknown state" in str(result.errors)


def test_wrong_head_artifact_digest_fails() -> None:
    document = _document()
    _last(document)["artifact_sha256"] = {"scripts/traceability.py": "0" * 64}
    result = _check(document)
    assert not result.ok
    assert "digest mismatch" in str(result.errors)


@pytest.mark.parametrize("field", ["head", "tree", "inputs_sha256"])
def test_actual_source_binding_rejects_wrong_identity(field: str) -> None:
    document = _document()
    manifest = document["manifest"]
    assert isinstance(manifest, dict)
    manifest[field] = "0" * 40
    result = _check(document)
    assert not result.ok
    assert "manifest source/head/tree" in str(result.errors)


def test_configured_expected_revision_is_independently_checked() -> None:
    validator = _validator()
    context = validator.LedgerContext(REPOSITORY_ROOT, LEDGERS[0], expected_revision="0" * 40)
    result = validator.validate_ledger_document(_document(), context)
    assert not result.ok
    assert "configured expected revision" in str(result.errors)


@pytest.mark.parametrize("field", ["run", "source", "config", "result"])
def test_caller_metadata_cannot_replace_execution(field: str) -> None:
    document = _document()
    _last(document)[field] = "caller-forged"
    result = _check(document)
    assert not result.ok
    assert "validator-owned execution" in str(result.errors)


@pytest.mark.parametrize("node", [NODE, "tests/tooling/test_verify.py", "gate:tests"])
def test_caller_sets_never_grant_credit(node: str) -> None:
    result = _check(_document(node=node))
    assert not result.ok


def test_skipped_file_reference_never_grants_credit() -> None:
    result = _check(_document(node=SKIPPED.split("::")[0]))
    assert not result.ok
    assert "exact collected node IDs" in str(result.errors)


@pytest.mark.parametrize("mutation", ["delete", "rewrite", "reorder", "manifest"])
def test_history_is_checked_against_git_not_caller_hash(mutation: str) -> None:
    document = _document()
    rows = document["rows"]
    assert isinstance(rows, list)
    _mutate_history(document, rows, mutation)
    result = _check(document)
    assert not result.ok
    assert "history" in str(result.errors) or "historical manifest" in str(result.errors)


def _mutate_history(document: dict[str, object], rows: list[object], mutation: str) -> None:
    if mutation == "delete":
        del rows[0]
        return
    if mutation == "reorder":
        rows[0], rows[1] = rows[1], rows[0]
        return
    target = rows[0] if mutation == "rewrite" else document["historical_manifest"]
    assert isinstance(target, dict)
    target["scenario"] = "rewritten historical scenario"


def test_receipt_file_and_self_consistent_hash_cannot_grant_credit(
    tmp_path: Path, committed_workspace: Path
) -> None:
    document = _document(root=committed_workspace)
    output = tmp_path / "forged"
    output.mkdir()
    (output / "receipt.json").write_text(
        json.dumps(
            {
                "collected": [NODE],
                "reports": {NODE: {"setup": "passed", "call": "passed", "teardown": "passed"}},
            }
        )
    )
    result = _check(copy.deepcopy(document), output, root=committed_workspace)
    assert not result.ok
    assert "File exists" in str(result.errors)


def test_legacy_file_api_cannot_credit_shipped_claims() -> None:
    validator = _validator()
    result = validator.validate_ledger_file(
        LEDGERS[0], REPOSITORY_ROOT, frozenset({NODE}), frozenset()
    )
    assert not result.ok


def test_recorder_retains_actual_ids_and_duplicate_phase_failure(
    request: pytest.FixtureRequest,
) -> None:
    spec = importlib.util.spec_from_file_location(
        "receipt_harness", REPOSITORY_ROOT / "scripts/traceability_harness.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    recorder = module.Recorder()
    recorder.pytest_collection_finish(request.session)
    assert request.node.nodeid in recorder.collected
    report = pytest.TestReport(
        NODE, ("tests/tooling/test_verify.py", 0, "test"), {}, "passed", None, "call"
    )
    recorder.pytest_runtest_logreport(report)
    recorder.pytest_runtest_logreport(report)
    assert recorder.reports[NODE]["call"] == "duplicate"


@pytest.mark.parametrize(
    "body",
    [
        "assert False",
        "pytest.skip('not executed')",
        "pytest.xfail('not passed')",
        "Path(__file__).write_text('# changed during execution')",
    ],
)
def test_real_harness_failures_and_source_mutation_are_rejected(
    body: str, tmp_path: Path, committed_workspace: Path
) -> None:
    from uuid import uuid4

    path = committed_workspace / "tests/tooling" / f"test_t5_receipt_{uuid4().hex}.py"
    path.write_text(
        "import pytest\nfrom pathlib import Path\ndef test_receipt():\n    " + body + "\n"
    )
    try:
        _commit_history(committed_workspace, path)
        node = path.relative_to(committed_workspace).as_posix() + "::test_receipt"
        result = _check(
            _document(node=node, root=committed_workspace),
            tmp_path / "run",
            root=committed_workspace,
        )
        assert not result.ok
        assert (tmp_path / "run/receipt.json").is_file()
        reason = "source changed" if body.startswith("Path") else "failed/skipped/not-run"
        assert reason in str(result.errors)
    finally:
        path.unlink()


def test_duplicate_new_requirement_test_pairs_fail() -> None:
    document = _document()
    rows = document["rows"]
    assert isinstance(rows, list)
    rows.append(copy.deepcopy(rows[-1]))
    result = _check(document)
    assert not result.ok
    assert "duplicate requirement/test pair" in str(result.errors)


def test_committed_additions_become_immutable_history(tmp_path: Path) -> None:
    validator = _validator()
    clone = tmp_path / "history"
    subprocess.run(
        ["git", "clone", "--quiet", "--no-hardlinks", str(REPOSITORY_ROOT), str(clone)], check=True
    )
    ledger = clone / LEDGERS[0].relative_to(REPOSITORY_ROOT)
    document = _document()
    ledger.write_text(json.dumps(document))
    _commit_history(clone, ledger)
    context = validator.LedgerContext(clone, ledger)
    rows = document["rows"]
    assert isinstance(rows, list)
    count = len(rows)
    rows.append(copy.deepcopy(rows[-1]))
    assert validator._history(document, context) == count
    previous = rows[-2]
    assert isinstance(previous, dict)
    previous["scenario"] = "rewritten prior committed addition"
    with pytest.raises(ValueError, match="append-only history"):
        validator._history(document, context)


def _commit_history(clone: Path, ledger: Path) -> None:
    import os

    environment = {
        **os.environ,
        "GIT_AUTHOR_NAME": "pselamy",
        "GIT_AUTHOR_EMAIL": "pselamy@gmail.com",
        "GIT_COMMITTER_NAME": "pselamy",
        "GIT_COMMITTER_EMAIL": "pselamy@gmail.com",
    }
    subprocess.run(["git", "-C", str(clone), "add", str(ledger)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(clone),
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "-m",
            "Record immutable test history",
        ],
        env=environment,
        check=True,
    )


def test_self_consistent_dirty_source_cannot_claim_committed_head(
    tmp_path: Path,
    committed_workspace: Path,
) -> None:
    path = committed_workspace / "scripts/traceability_harness.py"
    path.write_text(path.read_text() + "\n# modified after configured revision\n")
    document = _document(root=committed_workspace)
    result = _check(document, tmp_path / "dirty", root=committed_workspace)
    assert not result.ok
    assert "uncommitted inputs" in str(result.errors)


def test_assume_unchanged_dirty_test_cannot_claim_committed_head(
    tmp_path: Path,
    committed_workspace: Path,
) -> None:
    from uuid import uuid4

    path = committed_workspace / "tests/tooling" / f"test_t5_index_{uuid4().hex}.py"
    path.write_text("def test_index_binding():\n    assert True\n")
    _commit_history(committed_workspace, path)
    node = path.relative_to(committed_workspace).as_posix() + "::test_index_binding"
    path.write_text("def test_index_binding():\n    assert False, 'dirty but index-hidden'\n")
    subprocess.run(
        ["git", "-C", str(committed_workspace), "update-index", "--assume-unchanged", str(path)],
        check=True,
    )
    try:
        assert (
            subprocess.run(
                ["git", "-C", str(committed_workspace), "status", "--porcelain"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            == ""
        )
        result = _check(
            _document(node=node, root=committed_workspace),
            tmp_path / "index",
            root=committed_workspace,
        )
        assert not result.ok
        assert "differs from committed bytes" in str(result.errors)
    finally:
        subprocess.run(
            [
                "git",
                "-C",
                str(committed_workspace),
                "update-index",
                "--no-assume-unchanged",
                str(path),
            ],
            check=True,
        )
        path.unlink()


def test_skip_worktree_dirty_test_cannot_claim_committed_head(
    tmp_path: Path,
    committed_workspace: Path,
) -> None:
    from uuid import uuid4

    path = committed_workspace / "tests/tooling" / f"test_t5_worktree_{uuid4().hex}.py"
    path.write_text("def test_worktree_binding():\n    assert True\n")
    _commit_history(committed_workspace, path)
    node = path.relative_to(committed_workspace).as_posix() + "::test_worktree_binding"
    path.write_text("def test_worktree_binding():\n    assert False, 'dirty but index-hidden'\n")
    subprocess.run(
        ["git", "-C", str(committed_workspace), "update-index", "--skip-worktree", str(path)],
        check=True,
    )
    try:
        result = _check(
            _document(node=node, root=committed_workspace),
            tmp_path / "worktree",
            root=committed_workspace,
        )
        assert not result.ok
        assert "differs from committed bytes" in str(result.errors)
    finally:
        subprocess.run(
            [
                "git",
                "-C",
                str(committed_workspace),
                "update-index",
                "--no-skip-worktree",
                str(path),
            ],
            check=True,
        )
        path.unlink()


def test_ignored_untracked_test_cannot_claim_committed_head(
    tmp_path: Path,
    committed_workspace: Path,
) -> None:
    path = committed_workspace / "tests" / ".cache" / "test_t5_ignored.py"
    path.parent.mkdir(exist_ok=True)
    path.write_text("def test_ignored_binding():\n    assert True\n")
    try:
        node = "tests/.cache/test_t5_ignored.py::test_ignored_binding"
        result = _check(
            _document(node=node, root=committed_workspace),
            tmp_path / "ignored",
            root=committed_workspace,
        )
        assert not result.ok
        assert "not a committed tree entry" in str(result.errors)
    finally:
        path.unlink()
