"""Service and disposable-migration verification tests."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

MODULE_PATH = Path(__file__).parents[2] / "scripts" / "runtime_services.py"
LOCAL_DATABASE_URL = "postgresql+psycopg://tracker:dev_password@localhost:5432/polymarket_tracker"
LOCAL_REDIS_URL = "redis://localhost:6379"


def _load_module() -> ModuleType:
    assert MODULE_PATH.exists(), "runtime service verifier is not implemented"
    spec = importlib.util.spec_from_file_location("runtime_services_under_test", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeMigrationBackend:
    """Stateful fake that models the required migration transitions."""

    def __init__(self, _base_url: str, *, fail_at: str | None = None) -> None:
        self.fail_at = fail_at
        self.operations: list[str] = []
        self.current: str | None = None

    def expected_revisions(self) -> tuple[str, str]:
        return "002_risk_assessments", "001_initial"

    def create_database(self, name: str) -> None:
        self.operations.append(f"create:{name}")

    def run_alembic(self, database_url: str, command: str, target: str) -> None:
        assert "pit_verify_" in database_url
        self.operations.append(f"alembic:{command}:{target}")
        if self.fail_at == f"{command}:{target}":
            raise RuntimeError("deliberate primary failure")
        if command == "upgrade":
            self.current = "002_risk_assessments"
        else:
            self.current = "001_initial"

    def current_revision(self, database_url: str) -> str:
        assert "pit_verify_" in database_url
        assert self.current is not None
        self.operations.append(f"current:{self.current}")
        return self.current

    async def async_query(self, database_url: str) -> None:
        assert "pit_verify_" in database_url
        self.operations.append("async-query")

    def drop_database(self, name: str) -> None:
        self.operations.append(f"drop:{name}")
        if self.fail_at == "cleanup":
            raise RuntimeError("deliberate cleanup failure")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "expected"),
    [
        ("probe", ["probe"]),
        ("migrations", ["migrations"]),
        ("all", ["probe", "migrations"]),
    ],
)
async def test_phase_selection_is_explicit_and_ordered(phase: str, expected: list[str]) -> None:
    module = _load_module()
    calls: list[str] = []

    async def probe(_database_url: str, _redis_url: str) -> dict[str, bool]:
        calls.append("probe")
        return {"postgres_reachable": True, "redis_reachable": True}

    async def migrations(_database_url: str) -> dict[str, bool]:
        calls.append("migrations")
        return {"cleanup_succeeded": True}

    await module.execute_phase(
        phase,
        LOCAL_DATABASE_URL,
        LOCAL_REDIS_URL,
        probe_runner=probe,
        migration_runner=migrations,
    )

    assert calls == expected


def test_non_loopback_database_is_rejected_before_backend_creation() -> None:
    module = _load_module()

    with pytest.raises(module.ServicePrerequisiteError, match="loopback"):
        module.validate_loopback_database_url(
            "postgresql+psycopg://tracker:secret@192.0.2.10:5432/research"
        )


@pytest.mark.asyncio
async def test_migration_cycle_proves_each_revision_and_cleans_up() -> None:
    module = _load_module()
    backend: FakeMigrationBackend | None = None

    def factory(base_url: str) -> FakeMigrationBackend:
        nonlocal backend
        backend = FakeMigrationBackend(base_url)
        return backend

    evidence = await module.run_migration_cycle(LOCAL_DATABASE_URL, backend_factory=factory)

    assert evidence["migration_states"] == [
        "002_risk_assessments",
        "001_initial",
        "002_risk_assessments",
    ]
    assert evidence["async_query_succeeded"] is True
    assert evidence["cleanup_succeeded"] is True
    assert backend is not None
    assert backend.operations[0].startswith("create:pit_verify_")
    assert backend.operations[-1].startswith("drop:pit_verify_")


@pytest.mark.asyncio
async def test_primary_migration_failure_still_cleans_up() -> None:
    module = _load_module()
    backend: FakeMigrationBackend | None = None

    def factory(base_url: str) -> FakeMigrationBackend:
        nonlocal backend
        backend = FakeMigrationBackend(base_url, fail_at="downgrade:-1")
        return backend

    with pytest.raises(module.ServiceVerificationError, match="migration verification failed"):
        await module.run_migration_cycle(LOCAL_DATABASE_URL, backend_factory=factory)

    assert backend is not None
    assert backend.operations[-1].startswith("drop:pit_verify_")


@pytest.mark.asyncio
async def test_cleanup_failure_fails_the_gate() -> None:
    module = _load_module()

    def factory(base_url: str) -> FakeMigrationBackend:
        return FakeMigrationBackend(base_url, fail_at="cleanup")

    with pytest.raises(module.ServiceVerificationError, match="cleanup failed"):
        await module.run_migration_cycle(LOCAL_DATABASE_URL, backend_factory=factory)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_postgres_redis_and_disposable_migration_cycle() -> None:
    if os.environ.get("RUN_SERVICE_TESTS") != "1":
        pytest.skip("set RUN_SERVICE_TESTS=1 with the documented local services")
    module = _load_module()
    database_url = os.environ.get("DATABASE_URL", LOCAL_DATABASE_URL)
    redis_url = os.environ.get("REDIS_URL", LOCAL_REDIS_URL)

    evidence: dict[str, Any] = await module.execute_phase("all", database_url, redis_url)

    assert evidence["postgres_reachable"] is True
    assert evidence["redis_reachable"] is True
    assert evidence["async_query_succeeded"] is True
    assert evidence["cleanup_succeeded"] is True
