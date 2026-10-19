"""Entrypoint for the protected Python duplication gate."""

from pathlib import Path

from quality.duplication import verify_duplication


def main() -> None:
    verify_duplication(Path.cwd())


if __name__ in {"__main__"}:
    main()
