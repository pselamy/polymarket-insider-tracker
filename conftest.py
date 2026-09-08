"""Pytest configuration for the polymarket-insider-tracker tests."""

import pytest

__all__ = ["event_loop_policy", "pytest_plugins"]

# Configure pytest-asyncio
pytest_plugins = ["pytest_asyncio"]


@pytest.fixture(scope="session")
def event_loop_policy():
    """Use default event loop policy."""
    import asyncio

    return asyncio.DefaultEventLoopPolicy()
