"""Tests for supported sync and async database engine behavior."""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.storage.database import (
    DatabaseManager,
    create_async_db_engine,
    create_sync_engine,
)
from polymarket_insider_tracker.storage.database_url import DatabaseUrlMigrationWarning

CANONICAL_URL = "postgresql+psycopg://tracker:password@localhost:5432/research"


def test_canonical_url_selects_sync_psycopg_engine() -> None:
    engine = create_sync_engine(CANONICAL_URL)
    try:
        assert engine.dialect.name == "postgresql"
        assert engine.dialect.driver == "psycopg"
        assert not engine.dialect.is_async
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_canonical_url_selects_async_psycopg_engine() -> None:
    engine = create_async_db_engine(CANONICAL_URL)
    try:
        assert engine.dialect.name == "postgresql"
        assert engine.dialect.driver == "psycopg"
        assert engine.dialect.is_async
    finally:
        await engine.dispose()


def test_manager_normalizes_legacy_url_once_at_boundary() -> None:
    with pytest.warns(DatabaseUrlMigrationWarning):
        manager = DatabaseManager("postgresql://tracker:password@localhost:5432/research")

    assert manager.database_url == CANONICAL_URL


def test_sync_dispose_clears_engine_and_factory() -> None:
    manager = DatabaseManager(CANONICAL_URL, async_mode=False)
    session = manager.get_sync_session()
    session.close()
    assert manager._sync_engine is not None
    assert manager._sync_session_factory is not None

    manager.dispose()

    assert manager._sync_engine is None
    assert manager._sync_session_factory is None


@pytest.mark.asyncio
async def test_async_dispose_clears_engine_and_factory() -> None:
    manager = DatabaseManager(CANONICAL_URL)
    async with manager.get_async_session():
        pass
    assert manager._async_engine is not None
    assert manager._async_session_factory is not None

    await manager.dispose_async()

    assert manager._async_engine is None
    assert manager._async_session_factory is None
