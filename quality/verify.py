"""Entry point: uv run --locked --group dev python -m quality.verify."""

from pathlib import Path

from quality.pipeline import verify

verify(Path.cwd())
