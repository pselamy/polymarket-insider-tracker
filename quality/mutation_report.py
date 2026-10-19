"""Require complete mutation records and actual pytest failures."""

import re
import shutil
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from quality.trial_report import trial_stdout

NORMAL = "NORMAL"
KILLED = "KILLED"


@dataclass
class Result:
    """The raw fields required to classify one Cosmic Ray trial."""

    job_id: str
    worker: str
    outcome: str
    output: str


def verify_trial(result: Result) -> None:
    """Never credit timeouts, worker exceptions or arbitrary nonzero exits."""
    if result.worker != NORMAL or result.outcome != KILLED:
        raise ValueError("mutation worker must finish normally with a killed result")
    output = trial_stdout(result.output)
    if re.search(r"(?m)^FAILED tests/\S+.*$", output) is None:
        raise ValueError("mutation must produce a pytest failure record")
    if re.search(r"(?m)^1 failed(?:, \d+ passed)? in \d+\.\d+s\n?\Z", output) is None:
        raise ValueError("mutation must finish with one pytest failure and no errors")


def unique_ids(values: list[str]) -> set[str]:
    """Reject corrupt inventories rather than collapsing duplicate jobs."""
    unique: set[str] = set()
    for value in values:
        if value in unique:
            raise ValueError("duplicate mutation job identifiers")
        unique.add(value)
    return unique


def verify_results(planned: list[str], results: list[Result]) -> int:
    """Every planned job must have exactly one conclusive test failure."""
    if not planned:
        raise ValueError("mutation inventory is empty")
    expected = unique_ids(planned)
    actual = unique_ids([result.job_id for result in results])
    if expected != actual:
        raise ValueError("mutation results do not match the planned inventory")
    for result in results:
        verify_trial(result)
    return len(results)


def text(value: object) -> str:
    """SQLite has dynamic column types; reject malformed report values."""
    if not isinstance(value, str):
        raise ValueError("mutation report fields must be strings")
    return value


def verify_session(path: Path) -> int:
    """Read a private report copy without opening the original for writes."""
    with tempfile.TemporaryDirectory() as directory:
        copy = Path(directory) / "mutation.sqlite"
        shutil.copyfile(path, copy)
        with closing(sqlite3.connect(copy)) as db:
            planned_rows: list[tuple[object, ...]] = db.execute(
                "SELECT job_id FROM work_items"
            ).fetchall()
            result_rows: list[tuple[object, ...]] = db.execute(
                "SELECT job_id, worker_outcome, test_outcome, output FROM work_results"
            ).fetchall()
    planned = [text(job_id) for (job_id,) in planned_rows]
    results = [Result(*(text(value) for value in row)) for row in result_rows]
    return verify_results(planned, results)
