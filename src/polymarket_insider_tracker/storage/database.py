"""Database connection and session management.

This module provides the database engine, session factory, and
async session support for the storage layer.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from polymarket_insider_tracker.storage.models import Base

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)


def create_sync_engine(database_url: str, **kwargs: Any) -> Engine:
    """Create a synchronous SQLAlchemy engine.

    Args:
        database_url: Database connection URL (e.g., postgresql://...).
        **kwargs: Additional engine options.

    Returns:
        SQLAlchemy Engine instance.
    """
    return create_engine(database_url, **kwargs)


def _ensure_async_driver(database_url: str) -> str:
    """Coerce a generic postgresql:// URL to postgresql+asyncpg://.

    SQLAlchemy's create_async_engine refuses URLs whose dialect resolves to a
    sync driver. The same DATABASE_URL is consumed by both alembic (sync) and
    the application (async), so a single canonical postgresql:// value must
    work for both. Rewrite to the asyncpg driver here so callers can pass the
    canonical URL unchanged.
    """
    if database_url.startswith("postgresql://"):
        return "postgresql+asyncpg://" + database_url[len("postgresql://") :]
    return database_url


def create_async_db_engine(database_url: str, **kwargs: Any) -> AsyncEngine:
    """Create an asynchronous SQLAlchemy engine.

    Args:
        database_url: Database connection URL. Either ``postgresql://...`` or
            ``postgresql+asyncpg://...`` is accepted; the former is rewritten
            to use the asyncpg driver.
        **kwargs: Additional engine options.

    Returns:
        SQLAlchemy AsyncEngine instance.
    """
    return create_async_engine(_ensure_async_driver(database_url), **kwargs)


def create_sync_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create a synchronous session factory.

    Args:
        engine: SQLAlchemy Engine instance.

    Returns:
        Session factory.
    """
    return sessionmaker(bind=engine, expire_on_commit=False)


def create_async_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create an asynchronous session factory.

    Args:
        engine: SQLAlchemy AsyncEngine instance.

    Returns:
        Async session factory.
    """
    return async_sessionmaker(bind=engine, expire_on_commit=False)


def init_db(engine: Engine) -> None:
    """Initialize the database schema.

    Creates all tables defined in the models.

    Args:
        engine: SQLAlchemy Engine instance.
    """
    Base.metadata.create_all(engine)
    logger.info("Database schema initialized")


async def init_async_db(engine: AsyncEngine) -> None:
    """Initialize the database schema asynchronously.

    Creates all tables defined in the models.

    Args:
        engine: SQLAlchemy AsyncEngine instance.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database schema initialized (async)")


class DatabaseManager:
    """Manages database connections and sessions.

    Provides a unified interface for both sync and async database operations.
    """

    def __init__(
        self,
        database_url: str,
        *,
        async_mode: bool = True,
        pool_size: int = 5,
        max_overflow: int = 10,
        echo: bool = False,
    ) -> None:
        """Initialize database manager.

        Args:
            database_url: Database connection URL.
            async_mode: Use async engine/sessions if True.
            pool_size: Connection pool size.
            max_overflow: Maximum overflow connections.
            echo: Echo SQL statements for debugging.
        """
        self.database_url = database_url
        self.async_mode = async_mode
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._echo = echo

        self._sync_engine: Engine | None = None
        self._async_engine: AsyncEngine | None = None
        self._sync_session_factory: sessionmaker[Session] | None = None
        self._async_session_factory: async_sessionmaker[AsyncSession] | None = None

    def _get_sync_engine(self) -> Engine:
        """Get or create the synchronous engine."""
        if self._sync_engine is None:
            self._sync_engine = create_sync_engine(
                self.database_url,
                pool_size=self._pool_size,
                max_overflow=self._max_overflow,
                echo=self._echo,
            )
        return self._sync_engine

    def _get_async_engine(self) -> AsyncEngine:
        """Get or create the asynchronous engine."""
        if self._async_engine is None:
            self._async_engine = create_async_db_engine(
                self.database_url,
                pool_size=self._pool_size,
                max_overflow=self._max_overflow,
                echo=self._echo,
            )
        return self._async_engine

    def get_sync_session(self) -> Session:
        """Get a new synchronous session.

        Returns:
            SQLAlchemy Session instance.
        """
        if self._sync_session_factory is None:
            self._sync_session_factory = create_sync_session_factory(self._get_sync_engine())
        return self._sync_session_factory()

    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an asynchronous session as a context manager.

        Yields:
            SQLAlchemy AsyncSession instance.
        """
        if self._async_session_factory is None:
            self._async_session_factory = create_async_session_factory(self._get_async_engine())

        session = self._async_session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    def init_schema(self) -> None:
        """Initialize database schema synchronously."""
        init_db(self._get_sync_engine())

    async def init_schema_async(self) -> None:
        """Initialize database schema asynchronously."""
        await init_async_db(self._get_async_engine())

    def dispose(self) -> None:
        """Dispose of all database connections."""
        if self._sync_engine is not None:
            self._sync_engine.dispose()
            self._sync_engine = None
        logger.info("Database connections disposed")

    async def dispose_async(self) -> None:
        """Dispose of all async database connections."""
        if self._async_engine is not None:
            await self._async_engine.dispose()
            self._async_engine = None
        logger.info("Async database connections disposed")
