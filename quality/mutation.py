"""Always execute mutations in a fresh copy of source and tests."""

import os
import shutil
from pathlib import Path

from quality.deadline import read_deadline
from quality.mutation_inputs import copy_inputs as copy_inputs
from quality.mutation_inputs import environment
from quality.mutation_inputs import snapshot as snapshot
from quality.mutation_journal import verify_journals
from quality.mutation_pool import execute_pool
from quality.mutation_report import verify_session
from quality.mutation_workspace import workspace
from quality.owned_commands import run_owned
from quality.trial_launcher import prepare_launcher

run = run_owned


def mutate(root: Path, timeout: float) -> int:
    """Fresh initialization and baseline are required; cached outcomes are unused."""
    before = snapshot(root)
    execution_timeout = read_deadline(root / "quality" / "mutation-timeout.json")
    with workspace(root) as target:
        copy_inputs(root, target)
        prepare_launcher(target)
        env = environment(target, dict(os.environ))
        commands = (
            ["cosmic-ray", "init", "cosmic-ray.toml", "mutation.sqlite"],
            ["cosmic-ray", "baseline", "cosmic-ray.toml"],
        )
        for command in commands:
            run(command, target, timeout, env)
        workers = execute_pool(target, execution_timeout, env)
        verify_journals(target, workers)
        output = root / ".quality-results"
        output.mkdir(exist_ok=True)
        latest = output / "mutation-latest.sqlite"
        shutil.copy2(target / "mutation.sqlite", latest)
        count = verify_session(latest)
        if snapshot(target) != before or snapshot(root) != before:
            raise ValueError("mutation inputs changed or were not restored")
        shutil.copy2(latest, output / "mutation.sqlite")
    return count
