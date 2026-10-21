"""Shared print-or-check entry point for the spec 004 evidence generators.

``[]`` prints the generated text (exit 0). ``["--check", FILE]`` exits 0 only when FILE exists and
equals the text byte for byte, else 1. The text always ends in exactly one newline, matching
end-of-file-fixer. Any other argument list is a usage error (exit 2), so a mistyped or truncated
``--check`` can never pass silently.
"""

from __future__ import annotations

import sys
from pathlib import Path


def committed_text(path: Path) -> str | None:
    try:
        return path.read_text()
    except OSError:
        return None


def check(path: Path, text: str) -> int:
    if committed_text(path) == text:
        return 0
    print(f"stale or missing: {path} differs from the generated output", file=sys.stderr)
    return 1


def print_or_check(argv: list[str], text: str) -> int:
    text = text.rstrip("\n") + "\n"
    args = argv[1:]
    if not args:
        sys.stdout.write(text)
        return 0
    if len(args) == 2 and args[0] == "--check":
        return check(Path(args[1]), text)
    print(f"usage: {Path(argv[0]).name} [--check FILE]", file=sys.stderr)
    return 2
