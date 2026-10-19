"""Explicit staged enrollment of the reference verification hooks.

``quality/checks.json`` owns the enrolled command controls. This module owns the
enrolled pipeline hooks. Every reference hook stays referenced here so the vendored
package is not silently deleted, while only the hooks that pass on the current tree
are executed. The full ledger, including measured violation counts and diagnostics,
lives in ``docs/relentless-ci-adoption.md``.
"""

from collections.abc import Callable
from pathlib import Path

from quality.mutation import mutate
from quality.security import verify_security
from quality.source_scope import verify_sources, verify_tracked
from quality.test_report import prepare_tests, verify_tests

Hook = Callable[[Path], None]

ENROLLED_HOOKS: dict[str, Hook] = {
    "verify_tracked": verify_tracked,
}

UNENROLLED_HOOKS: dict[str, Callable[..., object]] = {
    "verify_sources": verify_sources,
    "prepare_tests": prepare_tests,
    "verify_tests": verify_tests,
    "verify_security": verify_security,
    "mutate": mutate,
}

UNENROLLED_REASONS: dict[str, str] = {
    "verify_sources": (
        "REF-HOOK-01: 50 authored files exceed 399 lines; .pyi stubs and "
        "non-snake_case alembic revisions are outside the .py-only scope"
    ),
    "prepare_tests": "REF-HOOK-03: only meaningful together with verify_tests",
    "verify_tests": (
        "REF-HOOK-03: the test report carries 55 warnings and the hook is gated "
        "by the unenrolled verify_sources"
    ),
    "verify_security": "REF-HOOK-04: pinned Gitleaks/OSV/Opengrep are not provisioned",
    "mutate": "REF-HOOK-05: the native Cosmic Ray mutation service is not provisioned",
}
