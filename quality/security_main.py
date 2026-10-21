"""Entrypoint for the independently wired security gate."""

from pathlib import Path

from quality.security_gate import verify_security_gate


def main() -> None:
    verify_security_gate(Path.cwd())


if __name__ in {"__main__"}:
    main()
