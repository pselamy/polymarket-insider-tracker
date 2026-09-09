"""Run the repository's locked Complexipy policy without cwd configuration discovery."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_SCOPE = ("src", "tests", "scripts", "alembic", "conftest.py")
POLICY_FLAGS = (
    "--max-complexity-allowed",
    "5",
    "--no-ignore",
    "--ignore-complexity=false",
    "--snapshot-ignore=true",
    "--snapshot-create=false",
    "--exclude=.",
    "--check-script=true",
)
EXPECTED_ARGUMENTS = (*CANONICAL_SCOPE, *POLICY_FLAGS)


def _analyzer_command(repository_root: Path) -> tuple[str, ...]:
    absolute_scope = tuple(str((repository_root / entry).resolve()) for entry in CANONICAL_SCOPE)
    return (
        sys.executable,
        "-c",
        "from complexipy.cli import main; main()",
        *absolute_scope,
        *POLICY_FLAGS,
    )


def run_complexipy(repository_root: Path = REPOSITORY_ROOT) -> int:
    """Run Complexipy from a fresh cwd so repository-local config cannot alter semantics."""
    with tempfile.TemporaryDirectory(prefix="complexipy-gate-") as clean_cwd:
        completed = subprocess.run(
            _analyzer_command(repository_root),
            cwd=clean_cwd,
            check=False,
        )
    return completed.returncode


def main(argv: Sequence[str] | None = None) -> int:
    """Reject any invocation that does not visibly carry the exact scope and policy."""
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    if arguments != EXPECTED_ARGUMENTS:
        print(
            "Complexipy gate requires the exact documented scope and policy arguments.",
            file=sys.stderr,
        )
        return 2
    return run_complexipy()


if __name__ == "__main__":
    raise SystemExit(main())
