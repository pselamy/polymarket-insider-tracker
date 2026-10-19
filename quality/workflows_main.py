"""Validate every authored workflow using the same local and hosted registry."""

from pathlib import Path

from quality.workflows import verify_workflows

verify_workflows(Path.cwd())
