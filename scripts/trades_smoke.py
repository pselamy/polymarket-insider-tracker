#!/usr/bin/env python3
"""Live-safe smoke check for the documented public trades query (FR-013, SC-005).

The deterministic core, ``run_smoke``, takes an injected ``httpx.AsyncClient`` and classifies each
response into exactly one of seven named cases. ``--live`` performs one bounded anonymous request per
selected coverage mode against the real endpoint. Records are aggregate-only: no wallet, name,
pseudonym, profile value, or raw row is ever retained; the body is hashed and discarded. The command
never opens Redis or PostgreSQL, never constructs an alert channel, never sends a credential, and is
not part of any verifier profile or CI job. See ``contracts/live-smoke.md``.

Exit status: ``0`` when every record passed and was reachable (an HTTP 200 list); ``1`` when a record
failed (incompatible schema, a terminal status, a share of invalid rows above 5%) or every attempt was
transient (throttled or timed out); ``2`` for an invocation error.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from scripts._smoke_cli import (
        COVERAGE_MODES,
        DEFAULT_TRADES_URL,
        EXIT_FAILED,
        EXIT_PASSED,
        EXIT_USAGE,
        ClientFactory,
        exit_code_for,
        main,
        resolve_trades_url,
    )
    from scripts._smoke_records import SmokeCase, SmokeRecord
    from scripts._smoke_runner import SMOKE_MAX_RETRIES, run_smoke
finally:
    _LOADED = (
        DEFAULT_TRADES_URL,
        EXIT_FAILED,
        EXIT_PASSED,
        EXIT_USAGE,
        ClientFactory,
        COVERAGE_MODES,
        SmokeCase,
        SmokeRecord,
        SMOKE_MAX_RETRIES,
        exit_code_for,
        main,
        resolve_trades_url,
        run_smoke,
    )

__all__ = [
    "DEFAULT_TRADES_URL",
    "EXIT_FAILED",
    "EXIT_PASSED",
    "EXIT_USAGE",
    "ClientFactory",
    "COVERAGE_MODES",
    "SMOKE_MAX_RETRIES",
    "SmokeCase",
    "SmokeRecord",
    "exit_code_for",
    "main",
    "resolve_trades_url",
    "run_smoke",
]

if __name__ == "__main__":
    sys.exit(main())
