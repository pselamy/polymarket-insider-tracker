"""Value boundaries and failures for disposable service verification."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol


class ServicePrerequisiteError(RuntimeError):
    """Raised before a destructive service-verification operation begins."""


class ServiceVerificationError(RuntimeError):
    """Raised when a real service or migration verification step fails."""


class MigrationBackend(Protocol):
    """Operations needed to prove a disposable migration lifecycle."""

    def expected_revisions(self) -> tuple[str, str]: ...

    def create_database(self, name: str) -> None: ...

    def run_alembic(self, database_url: str, command: str, target: str) -> None: ...

    def current_revision(self, database_url: str) -> str: ...

    async def async_query(self, database_url: str) -> None: ...

    def drop_database(self, name: str) -> None: ...


ProbeRunner = Callable[[str, str], Awaitable[dict[str, object]]]


MigrationRunner = Callable[[str], Awaitable[dict[str, object]]]


BackendFactory = Callable[[str], MigrationBackend]
