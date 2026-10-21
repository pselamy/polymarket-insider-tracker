"""The shared ``async_engine`` fixture keeps its schema when its connection is replaced."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


@pytest.mark.asyncio
async def test_schema_survives_connection_invalidation(async_engine: AsyncEngine) -> None:
    """A cancelled statement invalidates the pooled connection; the next one must see the tables.

    An in-memory database would reopen empty here ("no such table: risk_assessments").
    """
    async with async_engine.connect() as connection:
        await connection.invalidate()
    async with async_engine.connect() as connection:
        result = await connection.execute(text("SELECT count(*) FROM risk_assessments"))
        assert result.scalar_one() == 0
