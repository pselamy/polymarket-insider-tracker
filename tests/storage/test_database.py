"""Tests for supported sync and async database engine behavior."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.asyncio import create_async_engine

from polymarket_insider_tracker.storage.database import (
    DatabaseManager,
    create_async_db_engine,
    create_sync_engine,
    init_async_db,
    init_db,
)
from polymarket_insider_tracker.storage.database_url import DatabaseUrlMigrationWarning
from polymarket_insider_tracker.storage.models import Base

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
    assert manager.is_sync_open

    manager.dispose()

    assert not manager.is_sync_open


@pytest.mark.asyncio
async def test_async_dispose_clears_engine_and_factory() -> None:
    manager = DatabaseManager(CANONICAL_URL)
    async with manager.get_async_session():
        pass
    assert manager.is_async_open

    await manager.dispose_async()

    assert not manager.is_async_open


def test_dispose_before_any_session_is_a_noop() -> None:
    manager = DatabaseManager(CANONICAL_URL, async_mode=False)

    manager.dispose()

    assert not manager.is_sync_open


@pytest.mark.asyncio
async def test_async_dispose_before_any_session_is_a_noop() -> None:
    manager = DatabaseManager(CANONICAL_URL)

    await manager.dispose_async()

    assert not manager.is_async_open


def test_init_db_creates_every_model_table() -> None:
    engine = create_engine("sqlite:///:memory:")
    try:
        init_db(engine)
        assert set(Base.metadata.tables) <= set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_init_async_db_creates_every_model_table() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        await init_async_db(engine)
        async with engine.connect() as conn:
            tables = await conn.run_sync(lambda sync: set(inspect(sync).get_table_names()))
        assert set(Base.metadata.tables) <= tables
    finally:
        await engine.dispose()


def test_sync_sessions_share_one_engine_until_disposed() -> None:
    manager = DatabaseManager(CANONICAL_URL, async_mode=False)
    first = manager.get_sync_session()
    second = manager.get_sync_session()
    try:
        assert first.get_bind() is second.get_bind()
    finally:
        first.close()
        second.close()
        manager.dispose()
