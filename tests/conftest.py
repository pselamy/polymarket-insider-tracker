"""Pytest configuration and fixtures."""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def isolate_repository_dotenv(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """Keep unit tests from implicitly loading the contributor's repository `.env`.

    Yields the isolated working directory so a test can prove the harness contract holds.
    """
    original_directory = Path.cwd()
    isolated_directory = tmp_path_factory.mktemp("test-cwd")
    os.chdir(isolated_directory)
    try:
        yield isolated_directory
    finally:
        os.chdir(original_directory)
