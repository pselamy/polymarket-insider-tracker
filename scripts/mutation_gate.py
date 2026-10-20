"""Fail the mutation job when any enrolled module has a mutant that no test caught.

Reads ``mutmut results --all true`` on stdin. Every mutant of an enrolled module must be
``killed`` or ``timeout`` (a mutant that hangs the suite is detected); ``survived``,
``no tests``, ``suspicious``, ``segfault`` and anything else fail the gate. An enrolled
module with no mutants at all also fails, so a misconfigured run cannot pass silently.
The module list only ever grows; ``redaction`` is mutated by the same job but is not yet
enrolled because it still has survivors (see ``docs/relentless-ci-adoption.md``).
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from collections.abc import Iterable

ENROLLED_MODULES = (
    "polymarket_insider_tracker.endpoints",
    "polymarket_insider_tracker.ingestor._retry",
    "polymarket_insider_tracker.ingestor._attempt_budget",
    "polymarket_insider_tracker.ingestor._cycle_cutoff",
)
DETECTED_STATUSES = frozenset({"killed", "timeout"})
_MUTANT_LINE = re.compile(r"^\s*(?P<module>[\w.]+)\.x[^.\s:]*__mutmut_\d+: (?P<status>.+?)\s*$")


def _statuses_by_module(lines: Iterable[str]) -> dict[str, Counter[str]]:
    counts: dict[str, Counter[str]] = {module: Counter() for module in ENROLLED_MODULES}
    for line in lines:
        matched = _MUTANT_LINE.match(line)
        if matched and matched["module"] in counts:
            counts[matched["module"]][matched["status"]] += 1
    return counts


def _problems(module: str, statuses: Counter[str]) -> list[str]:
    if not statuses:
        return [f"{module}: no mutants were generated"]
    return [
        f"{module}: {count} mutant(s) {status}"
        for status, count in sorted(statuses.items())
        if status not in DETECTED_STATUSES
    ]


def evaluate(lines: Iterable[str]) -> list[str]:
    """Every reason the enrolled modules fail the gate; empty when all mutants are detected."""
    counts = _statuses_by_module(lines)
    return [
        problem for module, statuses in counts.items() for problem in _problems(module, statuses)
    ]


def main() -> int:
    problems = evaluate(sys.stdin)
    for problem in problems:
        print(f"mutation gate: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
