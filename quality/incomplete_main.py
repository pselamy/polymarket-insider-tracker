"""Entrypoint for the protected incomplete-source gate."""

from pathlib import Path

from quality.incomplete import verify_incomplete


def main() -> None:
    verify_incomplete(Path.cwd())


if __name__ in {"__main__"}:
    main()
