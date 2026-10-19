"""Check declared Python support without credentials or live network requests."""

from datetime import UTC, datetime
from pathlib import Path

from quality.runtime_support import verify_runtimes

verify_runtimes(Path.cwd(), datetime.now(UTC).date())
