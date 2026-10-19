"""Entrypoint for protected support-file syntax and shell checks."""

from pathlib import Path

from quality.support_files import verify_support


def main() -> None:
    verify_support(Path.cwd())


if __name__ in {"__main__"}:
    main()
