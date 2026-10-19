"""Run the registered repository hygiene gate against the current checkout."""

from pathlib import Path

from quality.repository_hygiene import verify_repository


def main() -> None:
    verify_repository(Path.cwd())


if __name__ in {"__main__"}:
    main()
