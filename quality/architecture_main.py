"""Run architecture verification without executing repository imports."""

from pathlib import Path

from quality.architecture import verify_architecture

verify_architecture(Path.cwd())
