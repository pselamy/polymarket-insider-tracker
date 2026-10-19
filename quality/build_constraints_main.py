"""Check the current repository's build hash export."""

from pathlib import Path

from quality.build_constraints import verify_build_constraints

verify_build_constraints(Path.cwd())
