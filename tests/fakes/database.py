"""A real ``DatabaseManager`` whose sessions fail with a chosen error."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession

from polymarket_insider_tracker.storage.database import DatabaseManager

UNUSED_DATABASE_URL = "postgresql+psycopg://tracker@127.0.0.1:1/unused"


class FailingSessionDatabase(DatabaseManager):
    """``DatabaseManager`` that raises ``failure`` when a session is opened."""

    failure: Exception | None = None

    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        if self.failure is not None:
            raise self.failure
        async with super().get_async_session() as session:
            yield session


def failing_database(failure: Exception) -> FailingSessionDatabase:
    """A database manager whose every session attempt raises ``failure``."""
    manager = FailingSessionDatabase(UNUSED_DATABASE_URL)
    manager.failure = failure
    return manager
