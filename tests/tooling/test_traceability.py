"""Contract tests for the T5 machine-readable traceability ledger (slices 002/003).

Every row binds ``requirement -> scenario -> negative_scenario -> test ->
run -> source -> config -> result -> artifact_sha256 -> state``. The ledger
files live at ``specs/<slice>/evidence/TRACEABILITY.json`` and are validated
through the shared stdlib-only ``scripts/traceability.py`` validator: unknown
states, dangling test/artifact paths, duplicate rows, digest mismatches, and
self-referential ledger hashes all fail. A row citing a skipped/not-run test
as ``exact-head-passed`` fails validation (positive real-harness receipt plus
forgery/missing/dangling/wrong-head negative controls).
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VALIDATOR_PATH = REPOSITORY_ROOT / "scripts" / "traceability.py"
LEDGERS = (
    REPOSITORY_ROOT / "specs" / "002-reproducible-runtime" / "evidence" / "TRACEABILITY.json",
    REPOSITORY_ROOT / "specs" / "003-safe-observable-operation" / "evidence" / "TRACEABILITY.json",
)

SKIPPED_NODE_IDS = frozenset(
    {
        "tests/integration/test_migration_backfill.py::"
        "test_migration_backfills_unrecorded_disposition_for_legacy_rows",
        "tests/integration/test_runtime_services.py::"
        "test_real_postgres_redis_and_disposable_migration_cycle",
        "tests/test_shutdown.py::"
        "TestWindowsSignalHandlers::test_windows_signal_handler_installed",
    }
)


def _validator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("traceability_under_test", VALIDATOR_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_ledger(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _file_test_entries(document: dict[str, object]) -> list[str]:
    entries: list[str] = []
    rows = document["rows"]
    assert isinstance(rows, list)
    for row in rows:
        entries.extend(_row_file_entries(row))
    return entries


def _row_file_entries(row: object) -> list[str]:
    assert isinstance(row, dict)
    tests = row["test"]
    assert isinstance(tests, list)
    return _listed_file_entries(tests)


def _listed_file_entries(tests: list[object]) -> list[str]:
    entries: list[str] = []
    for entry in tests:
        assert isinstance(entry, str)
        if not entry.startswith("gate:"):
            entries.append(entry.split("::")[0])
    return entries


def test_ledgers_exist_and_validate_at_exact_head() -> None:
    validator = _validator()
    for ledger in LEDGERS:
        assert ledger.is_file(), f"missing ledger {ledger}"
        result = validator.validate_ledger_file(
            ledger, REPOSITORY_ROOT, frozenset(), SKIPPED_NODE_IDS
        )
        assert result.ok, "\n".join(result.errors)


def test_ledger_states_are_enumerated() -> None:
    allowed = {
        "specified",
        "implemented",
        "executed",
        "exact-head-passed",
        "merge-required",
        "post-merge-verified",
    }
    for ledger in LEDGERS:
        rows = _load_ledger(ledger)["rows"]
        assert isinstance(rows, list)
        assert rows
        for row in rows:
            assert isinstance(row, dict)
            assert row["state"] in allowed


def test_ledger_has_no_dangling_test_or_artifact_paths() -> None:
    for ledger in LEDGERS:
        document = _load_ledger(ledger)
        for entry in _file_test_entries(document):
            assert (REPOSITORY_ROOT / entry).is_file(), f"dangling test path {entry}"
        _assert_artifacts_exist(document)


def _assert_artifacts_exist(document: dict[str, object]) -> None:
    rows = document["rows"]
    assert isinstance(rows, list)
    for row in rows:
        _assert_row_artifacts_exist(row)


def _assert_row_artifacts_exist(row: object) -> None:
    assert isinstance(row, dict)
    digest = row["artifact_sha256"]
    assert isinstance(digest, dict)
    for path in digest:
        assert (REPOSITORY_ROOT / path).is_file(), f"dangling artifact {path}"


def test_skipped_test_cited_as_exact_head_passed_fails() -> None:
    validator = _validator()
    ledger = LEDGERS[0]
    document = _load_ledger(ledger)
    rows = document["rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    forged = json.loads(json.dumps(first))
    forged["test"] = [
        "tests/integration/test_migration_backfill.py::"
        "test_migration_backfills_unrecorded_disposition_for_legacy_rows"
    ]
    forged["state"] = "exact-head-passed"
    context = validator.LedgerContext(
        repository_root=REPOSITORY_ROOT,
        ledger_path=ledger,
        executed_tests=frozenset(),
        skipped_tests=SKIPPED_NODE_IDS,
    )
    result = validator.validate_ledger_document(
        {"manifest": document["manifest"], "rows": [forged]}, context
    )
    assert not result.ok
    assert any("skipped/not-run" in error for error in result.errors)


def test_missing_test_cited_as_exact_head_passed_fails() -> None:
    validator = _validator()
    ledger = LEDGERS[0]
    document = _load_ledger(ledger)
    rows = document["rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    forged = json.loads(json.dumps(first))
    forged["test"] = ["tests/tooling/test_verify.py::test_no_such_case"]
    forged["state"] = "exact-head-passed"
    context = validator.LedgerContext(
        repository_root=REPOSITORY_ROOT,
        ledger_path=ledger,
        executed_tests=frozenset(),
        skipped_tests=SKIPPED_NODE_IDS,
    )
    result = validator.validate_ledger_document(
        {"manifest": document["manifest"], "rows": [forged]}, context
    )
    assert not result.ok
    assert any("no executed-run receipt" in error for error in result.errors)


def test_dangling_test_path_fails() -> None:
    validator = _validator()
    ledger = LEDGERS[0]
    document = _load_ledger(ledger)
    rows = document["rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    forged = json.loads(json.dumps(first))
    forged["test"] = ["tests/tooling/test_no_such_file.py"]
    context = validator.LedgerContext(
        repository_root=REPOSITORY_ROOT,
        ledger_path=ledger,
        executed_tests=frozenset({"tests/tooling/test_no_such_file.py"}),
        skipped_tests=SKIPPED_NODE_IDS,
    )
    result = validator.validate_ledger_document(
        {"manifest": document["manifest"], "rows": [forged]}, context
    )
    assert not result.ok
    assert any("dangling test path" in error for error in result.errors)


def test_self_referential_artifact_hash_fails() -> None:
    validator = _validator()
    ledger = LEDGERS[0]
    document = _load_ledger(ledger)
    rows = document["rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    forged = json.loads(json.dumps(first))
    forged["artifact_sha256"] = {
        "specs/002-reproducible-runtime/evidence/TRACEABILITY.json": "0" * 64
    }
    context = validator.LedgerContext(
        repository_root=REPOSITORY_ROOT,
        ledger_path=ledger,
        executed_tests=frozenset(),
        skipped_tests=SKIPPED_NODE_IDS,
    )
    result = validator.validate_ledger_document(
        {"manifest": document["manifest"], "rows": [forged]}, context
    )
    assert not result.ok
    assert any("self-referential" in error for error in result.errors)


def test_unknown_state_fails() -> None:
    validator = _validator()
    ledger = LEDGERS[0]
    document = _load_ledger(ledger)
    rows = document["rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    forged = json.loads(json.dumps(first))
    forged["state"] = "passed"
    context = validator.LedgerContext(
        repository_root=REPOSITORY_ROOT,
        ledger_path=ledger,
        executed_tests=frozenset(),
        skipped_tests=SKIPPED_NODE_IDS,
    )
    result = validator.validate_ledger_document(
        {"manifest": document["manifest"], "rows": [forged]}, context
    )
    assert not result.ok
    assert any("unknown state" in error for error in result.errors)


def test_wrong_head_artifact_digest_fails() -> None:
    validator = _validator()
    ledger = LEDGERS[0]
    document = _load_ledger(ledger)
    rows = document["rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    forged = json.loads(json.dumps(first))
    digest = forged["artifact_sha256"]
    assert isinstance(digest, dict)
    first_key = next(iter(digest))
    digest[first_key] = "0" * 64
    context = validator.LedgerContext(
        repository_root=REPOSITORY_ROOT,
        ledger_path=ledger,
        executed_tests=frozenset(),
        skipped_tests=SKIPPED_NODE_IDS,
    )
    result = validator.validate_ledger_document(
        {"manifest": document["manifest"], "rows": [forged]}, context
    )
    assert not result.ok
    assert any("digest mismatch" in error for error in result.errors)
