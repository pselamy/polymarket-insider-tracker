"""Execute the complete native plan on isolated, owned HTTP worker copies."""

import sys
from pathlib import Path

from quality.mutation_config import execution_config
from quality.mutation_inputs import snapshot
from quality.mutation_services import Worker, launch, ready, shutdown, worker_count
from quality.owned_commands import run_owned


def execute_pool(root: Path, timeout: float, env: dict[str, str]) -> list[Worker]:
    """Do not preserve/delete inputs until every worker stop has been accounted for."""
    before = snapshot(root)
    workers: list[Worker] = []
    errors: list[BaseException] = []
    try:
        urls = start_pool(root, timeout, env, workers)
        config = execution_config(root / "cosmic-ray.toml", urls)
        run_owned(
            [
                sys.executable,
                "-m",
                "quality.mutation_coordinator_main",
                "exec",
                config.name,
                "mutation.sqlite",
            ],
            root,
            timeout,
            env,
        )
    except BaseException as error:
        errors.append(error)
    finally:
        errors.extend(shutdown(workers))
    if errors:
        raise BaseExceptionGroup("mutation pool failed", errors)
    require_restored(root, workers, before)
    return workers


def start_pool(root: Path, timeout: float, env: dict[str, str], workers: list[Worker]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for index in range(worker_count()):
        worker = launch(root, index, timeout, env)
        workers.append(worker)
        url = ready(worker)
        if url in seen:
            raise ValueError("mutation worker URLs are duplicated")
        seen.add(url)
        urls.append(url)
    return urls


def require_restored(root: Path, workers: list[Worker], before: dict[str, bytes]) -> None:
    if snapshot(root) != before:
        raise ValueError("coordinator inputs changed or were not restored")
    for worker in workers:
        if snapshot(worker.root) != before:
            raise ValueError("worker inputs changed or were not restored")
