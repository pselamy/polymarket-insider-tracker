"""Validate T5 history and fresh, validator-owned execution (see traceability contract)."""

from __future__ import annotations

import json
import shlex
import subprocess
from hashlib import sha256
from pathlib import Path

from scripts._traceability_execution import (
    _committed_test as _committed_test,
)
from scripts._traceability_execution import (
    _execute as _execute,
)
from scripts._traceability_execution import (
    _new_rows as _new_rows,
)
from scripts._traceability_execution import (
    _require_committed_source as _require_committed_source,
)
from scripts._traceability_primitives import (
    BASELINE as BASELINE,
)
from scripts._traceability_primitives import (
    REQUIRED_KEYS as REQUIRED_KEYS,
)
from scripts._traceability_primitives import (
    STATES as STATES,
)
from scripts._traceability_primitives import (
    LedgerContext as LedgerContext,
)
from scripts._traceability_primitives import (
    LedgerResult as LedgerResult,
)
from scripts._traceability_primitives import (
    _artifact as _artifact,
)
from scripts._traceability_primitives import (
    _baseline as _baseline,
)
from scripts._traceability_primitives import (
    _committed_bytes as _committed_bytes,
)
from scripts._traceability_primitives import (
    _git as _git,
)
from scripts._traceability_primitives import (
    _history as _history,
)
from scripts._traceability_primitives import (
    _manifest as _manifest,
)
from scripts._traceability_primitives import (
    execution_command as execution_command,
)
from scripts._traceability_primitives import (
    object_list as object_list,
)
from scripts._traceability_primitives import (
    object_map as object_map,
)
from scripts._traceability_primitives import (
    source_identity as source_identity,
)
from scripts._traceability_primitives import (
    strings as strings,
)


def _claim_nodes(rows: list[dict[str, object]]) -> list[str]:
    nodes: set[str] = set()
    for row in rows:
        nodes.update(strings(row.get("test")))
    if any("::" not in node for node in nodes):
        raise ValueError("execution credit requires exact collected node IDs, never files/gates")
    return sorted(nodes)


def _configuration(root: Path) -> dict[str, str]:
    return {
        path: sha256((root / path).read_bytes()).hexdigest()
        for path in ("pyproject.toml", "uv.lock")
    }


def _claims(rows: list[dict[str, object]], identity: dict[str, object], root: Path) -> list[str]:
    nodes = _claim_nodes(rows)
    expected: dict[str, object] = {
        "run": shlex.join(execution_command(nodes)),
        "source": identity,
        "config": _configuration(root),
        "result": "passed",
        "state": "exact-head-passed",
    }
    for row in rows:
        _execution_metadata(row, expected)
    return nodes


def _execution_metadata(row: dict[str, object], expected: dict[str, object]) -> None:
    if any(row.get(key) != value for key, value in expected.items()):
        raise ValueError("run/source/config/result/state differs from validator-owned execution")


def validate_ledger_document(
    document: object, context: LedgerContext, *, execution_output: Path | None = None
) -> LedgerResult:
    """Only a newly executed child harness can grant credit; files/sets are not receipts.

    execution_output must not exist. Each validation runs again and retains both streams.
    Omitting it is read-only and always rejects execution claims.
    """
    try:
        parsed = object_map(document)
        identity = _manifest(parsed, context)
        rows = _new_rows(parsed, context)
        nodes = _claims(rows, identity, context.repository_root)
        if execution_output is None:
            raise ValueError("no executed-run receipt: validator-owned execution required")
        _require_committed_source(context)
        _execute(nodes, context, execution_output)
        _require_committed_source(context)
        if source_identity(context) != identity:
            raise ValueError("source changed during execution")
        (execution_output / "source.json").write_text(json.dumps(identity, sort_keys=True))
        (execution_output / "validated-document.json").write_text(
            json.dumps(parsed, indent=2) + "\n"
        )
        return LedgerResult(True, ())
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        return LedgerResult(False, (str(exc),))


def validate_ledger_file(
    ledger_path: Path,
    repository_root: Path,
    executed_tests: frozenset[str],
    skipped_tests: frozenset[str],
    *,
    expected_revision: str = "",
    execution_output: Path | None = None,
) -> LedgerResult:
    context = LedgerContext(
        repository_root, ledger_path, executed_tests, skipped_tests, expected_revision
    )
    try:
        document: object = json.loads(ledger_path.read_text())
    except (ValueError, OSError) as exc:
        return LedgerResult(False, (str(exc),))
    return validate_ledger_document(document, context, execution_output=execution_output)
