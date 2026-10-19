"""Reject native kills without complete signed pytest execution evidence."""

import json

from quality.owned_receipt import integer, no_duplicate_keys
from quality.report_data import record


def stream(value: object) -> str:
    """An empty stream is valid; non-string diagnostic data is not."""
    if not isinstance(value, str):
        raise ValueError("mutation trial streams must be strings")
    return value


def trial_stdout(output: str) -> str:
    """Only an ordinary pytest failure with no stderr can earn mutation credit."""
    value: object = json.loads(output, object_pairs_hook=no_duplicate_keys)
    data = record(value)
    if set(data) != {"returncode", "stdout", "stderr"}:
        raise ValueError("mutation trial diagnostic fields are incomplete")
    if integer(data["returncode"]) != 1:
        raise ValueError("mutation trial must exit with pytest failure status 1")
    if stream(data["stderr"]):
        raise ValueError("mutation trial stderr contains runtime diagnostics")
    return stream(data["stdout"])
