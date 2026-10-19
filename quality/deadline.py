"""Reject unbounded or malformed protected command deadlines."""

import json
import math
from pathlib import Path

MINIMUM_TIMEOUT = 0


def read_deadline(path: Path) -> float:
    policy: object = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(policy, bool) or not isinstance(policy, (int, float)):
        raise ValueError("command timeout must be a positive number")
    if policy <= MINIMUM_TIMEOUT or not math.isfinite(policy):
        raise ValueError("command timeout must be a positive finite number")
    return policy
