"""Pytest configuration and fixtures."""

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import Base


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


@pytest.fixture
async def fake_redis() -> AsyncIterator[FakeAsyncRedis]:
    """Isolated in-memory Redis with redis-py semantics.

    ``fakeredis`` is a ``redis.asyncio.Redis`` subclass backed by a private in-memory server, so
    every product Redis call runs unchanged. Its fidelity for the operations this product uses is
    proven by the shared contract suite in ``tests/integration/test_redis_contract.py``, which runs
    the same scenarios against a real loopback Redis in the services profile.
    """
    client = FakeAsyncRedis()
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def async_engine() -> AsyncIterator[AsyncEngine]:
    """Real in-memory SQLite engine with the product schema created."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def db_manager(async_engine: AsyncEngine) -> DatabaseManager:
    """A real DatabaseManager whose async engine is the in-memory SQLite engine.

    ``DatabaseManager`` only accepts PostgreSQL URLs, so the SQLite engine is attached after
    construction; every session, repository, and transaction path is the production code.
    """
    manager = DatabaseManager(
        "postgresql+psycopg://tracker:unused@127.0.0.1:1/unit-tests", async_mode=True
    )
    manager._async_engine = async_engine
    manager._async_session_factory = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    return manager
