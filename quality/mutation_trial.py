"""Retain actual pytest streams and signed status inside the native trial output."""

import json
import subprocess
import sys


def run_trial() -> int:
    """The native Cosmic Ray deadline still bounds this whole process group."""
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-x", "-q", "--color=no"],
        capture_output=True,
        text=True,
        check=False,
    )
    print(
        json.dumps(
            {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )
    )
    if result.stderr:
        return 2
    return result.returncode
