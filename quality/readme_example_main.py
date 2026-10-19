"""Entrypoint for the protected README example execution gate."""

from pathlib import Path

from quality.readme_example import verify_readme_example


def main() -> None:
    verify_readme_example(Path.cwd())


if __name__ in {"__main__"}:
    main()
