"""Bound worker startup and require every owned shutdown receipt."""

import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from quality.mutation_inputs import copy_inputs, environment
from quality.owned_commands import (
    OwnedCommand,
    SupervisionUnproven,
    finish_owned,
    start_owned,
)
from quality.report_data import record, text

MAXIMUM_WORKERS = 8
PRIVATE_ADDRESS = ("http", "127.0.0.1", None, None)


@dataclass(frozen=True)
class Worker:
    """A fresh private source copy and its actual dedicated supervisor."""

    root: Path
    nonce: str
    owned: OwnedCommand


def worker_count() -> int:
    """Bound resource use while retaining the complete native mutation inventory."""
    return min(MAXIMUM_WORKERS, os.cpu_count() or 1)


def launch(root: Path, index: int, timeout: float, env: dict[str, str]) -> Worker:
    output = root / ".quality-results"
    output.mkdir(exist_ok=True)
    target = output / f"mutation-worker-{index}"
    target.mkdir()
    copy_inputs(root, target)
    nonce = uuid4().hex
    owned = start_owned(
        [sys.executable, "-m", "quality.mutation_worker_main", nonce],
        target,
        timeout + 180,
        environment(target, env),
    )
    return Worker(target, nonce, owned)


def require_live(worker: Worker) -> None:
    if worker.owned.process.poll() is not None:
        raise SupervisionUnproven(
            worker.owned.process.pid,
            worker.owned.directory,
            "worker supervisor exited before readiness",
        )


def ready(worker: Worker) -> str:
    path = worker.root / ".quality-results" / "worker-ready.json"
    deadline = time.monotonic() + 10
    while not path.exists():
        require_live(worker)
        if time.monotonic() >= deadline:
            raise TimeoutError("mutation worker readiness deadline exceeded")
        time.sleep(0.01)
    require_live(worker)
    value: object = json.loads(path.read_text(encoding="utf-8"))
    fields = record(value)
    if set(fields) != {"nonce", "url"} or text(fields["nonce"]) != worker.nonce:
        raise ValueError("mutation worker readiness binding differs")
    return loopback_url(text(fields["url"]))


def loopback_url(value: str) -> str:
    url = urlsplit(value)
    if (url.scheme, url.hostname, url.username, url.password) != PRIVATE_ADDRESS:
        raise ValueError("mutation worker must use a private loopback URL")
    if url.path or url.query or url.fragment or not url.port:
        raise ValueError("mutation worker URL must contain only a positive port")
    return value


def stop(worker: Worker) -> None:
    path = worker.root / ".quality-results"
    temporary = path / "worker-stop.tmp"
    temporary.write_text(json.dumps({"nonce": worker.nonce}), encoding="utf-8")
    temporary.replace(path / "worker-stop.json")


def shutdown(workers: list[Worker]) -> list[BaseException]:
    """Request every stop before waiting; retain every failure and live handle."""
    errors: list[BaseException] = []
    for worker in workers:
        capture_stop(worker, errors)
    for worker in workers:
        capture_finish(worker, errors)
    return errors


def capture_stop(worker: Worker, errors: list[BaseException]) -> None:
    try:
        stop(worker)
    except BaseException as error:
        errors.append(error)


def capture_finish(worker: Worker, errors: list[BaseException]) -> None:
    try:
        # Allow an existing sixty-second native trial to finish before cleanup.
        finish_owned(worker.owned, 69)
    except BaseException as error:
        errors.append(error)
