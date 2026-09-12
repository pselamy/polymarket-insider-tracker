"""Child pytest harness. Receipts are consumed only by the process that launched it."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


class Recorder:
    def __init__(self) -> None:
        self.collected: list[str] = []
        self.reports: dict[str, dict[str, str]] = {}

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self.collected = sorted(item.nodeid for item in session.items)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        phases = self.reports.setdefault(report.nodeid, {})
        if report.when in phases:
            phases[report.when] = "duplicate"
            return
        phases[report.when] = report.outcome


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    receipt = Path.cwd() / "receipt.json"
    recorder = Recorder()
    nodes = [str(root / node) for node in sys.argv[1:]]
    result = pytest.main(
        [
            "-c",
            str(root / "pyproject.toml"),
            "--rootdir",
            str(root),
            "-o",
            "addopts=",
            "-vv",
            *nodes,
        ],
        plugins=[recorder],
    )
    receipt.write_text(json.dumps({"collected": recorder.collected, "reports": recorder.reports}))
    return int(result)


if __name__ == "__main__":
    raise SystemExit(main())
