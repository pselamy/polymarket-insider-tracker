"""Independently bind every completed HTTP request to the entire native plan."""

import json
import sqlite3
import tomllib
from contextlib import closing
from pathlib import Path

from quality.mutation_services import Worker
from quality.owned_receipt import integer
from quality.report_data import array, record, text

Signature = tuple[str, str, int]
HTTP_SUCCESS = 200


def signature(value: object) -> Signature:
    item = record(value)
    return (
        text(item["module_path"]),
        text(item["operator"]),
        integer(item["occurrence"]),
    )


def worker_requests(worker: Worker, command: str, timeout: object) -> list[Signature]:
    path = worker.root / ".quality-results" / "worker-jobs.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError("mutation worker journal is empty")
    return [
        item for line in lines for item in request_signatures(json.loads(line), command, timeout)
    ]


def request_signatures(value: object, command: str, timeout: object) -> list[Signature]:
    row = record(value)
    if integer(row["status"]) != HTTP_SUCCESS:
        raise ValueError("mutation worker response was not successful")
    request = record(row["request"])
    if text(request["test_command"]) != command or request["timeout"] != timeout:
        raise ValueError("mutation worker trial policy differs")
    mutations = array(request["mutations"])
    if not mutations:
        raise ValueError("mutation worker request has no mutations")
    return [signature(item) for item in mutations]


def verify_journals(root: Path, workers: list[Worker]) -> None:
    config = record(tomllib.loads((root / "cosmic-ray.toml").read_text())["cosmic-ray"])
    command = text(config["test-command"])
    requests = [
        item for worker in workers for item in worker_requests(worker, command, config["timeout"])
    ]
    with closing(sqlite3.connect(root / "mutation.sqlite")) as db:
        rows = db.execute(
            "SELECT module_path, operator_name, occurrence FROM mutation_specs"
        ).fetchall()
    planned = [(text(row[0]), text(row[1]), integer(row[2])) for row in rows]
    require_unique_plan(planned)
    require_unique_plan(requests)
    if sorted(planned) != sorted(requests):
        raise ValueError("mutation worker journals differ from complete native plan")


def require_unique_plan(signatures: list[Signature]) -> None:
    """A complete plan is nonempty and identifies each native mutation once."""
    if not signatures:
        raise ValueError("complete native plan is empty")
    seen: set[Signature] = set()
    for item in signatures:
        if item in seen:
            raise ValueError("complete native plan contains duplicate signatures")
        seen.add(item)
