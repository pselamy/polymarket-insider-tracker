"""Execute the stability gate without running it when its helpers are imported."""

from pathlib import Path

from quality.stability import run_all

run_all(Path.cwd())
