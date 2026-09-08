"""Guard the deterministic-suite isolation contract documented in the runtime contract."""

from __future__ import annotations

from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_deterministic_suite_runs_outside_the_repository_checkout(
    isolate_repository_dotenv: Path,
) -> None:
    """Pydantic must not be able to rediscover the contributor's repository `.env`."""
    working_directory = Path.cwd().resolve()

    assert working_directory == isolate_repository_dotenv.resolve()
    assert working_directory != REPOSITORY_ROOT
    assert REPOSITORY_ROOT not in working_directory.parents
    assert not (working_directory / ".env").exists()
