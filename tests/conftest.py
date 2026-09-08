"""Pytest configuration and fixtures."""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def isolate_repository_dotenv(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Keep unit tests from implicitly loading the contributor's repository `.env`."""
    original_directory = Path.cwd()
    os.chdir(tmp_path_factory.mktemp("test-cwd"))
    try:
        yield
    finally:
        os.chdir(original_directory)


@pytest.fixture
def sample_market_id() -> str:
    """Sample market ID for testing."""
    return "0x1234567890abcdef1234567890abcdef12345678"
