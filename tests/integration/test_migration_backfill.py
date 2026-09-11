"""Backfill semantics of migration ``003_safe_observable_operation`` on real PostgreSQL.

Rows that already exist when the migration runs have no recorded delivery outcome, so the
migration must label them ``unrecorded`` rather than the contradictory pair
``delivery_disposition='dry_run'`` with ``dry_run=false``.
"""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path
from types import ModuleType

import psycopg
import pytest
from sqlalchemy.engine import make_url

MODULE_PATH = Path(__file__).parents[2] / "scripts" / "runtime_services.py"
LOCAL_DATABASE_URL = "postgresql+psycopg://tracker:dev_password@localhost:5432/polymarket_tracker"

LEGACY_ROW_INSERT = """
INSERT INTO risk_assessments (
    assessment_id, trade_id, wallet_address, market_id, side, price, size,
    notional_usdc, trade_timestamp, weighted_score, signals_triggered,
    should_alert, threshold_at_eval, created_at
) VALUES (
    %(assessment_id)s, 'legacy-trade-1', '0x1234567890abcdef1234567890abcdef12345678',
    'legacy-market-1', 'BUY', 0.5, 100, 50, now(), 0.850, 2, true, 0.800, now()
)
"""


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("runtime_services_backfill", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _psycopg_dsn(database_url: str) -> str:
    return make_url(database_url).set(drivername="postgresql").render_as_string(hide_password=False)


def _insert_legacy_row(database_url: str, assessment_id: str) -> None:
    with psycopg.connect(_psycopg_dsn(database_url), autocommit=True) as connection:
        connection.execute(LEGACY_ROW_INSERT, {"assessment_id": assessment_id})


def _read_legacy_disposition(database_url: str, assessment_id: str) -> tuple[str, bool]:
    query = "SELECT delivery_disposition, dry_run FROM risk_assessments WHERE assessment_id = %s"
    with psycopg.connect(_psycopg_dsn(database_url)) as connection:
        row = connection.execute(query, (assessment_id,)).fetchone()
    assert row is not None
    return str(row[0]), bool(row[1])


def _read_legacy_scoring_identity(database_url: str, assessment_id: str) -> tuple[str, str | None]:
    query = (
        "SELECT scoring_algorithm_version, scoring_config"
        " FROM risk_assessments WHERE assessment_id = %s"
    )
    with psycopg.connect(_psycopg_dsn(database_url)) as connection:
        row = connection.execute(query, (assessment_id,)).fetchone()
    assert row is not None
    return str(row[0]), row[1]


def _scoring_identity_column_shapes(database_url: str) -> dict[str, tuple[str, str]]:
    query = (
        "SELECT column_name, is_nullable, COALESCE(column_default, '') FROM"
        " information_schema.columns WHERE table_name = 'risk_assessments'"
        " AND column_name IN ('scoring_algorithm_version', 'scoring_config')"
    )
    with psycopg.connect(_psycopg_dsn(database_url)) as connection:
        rows = connection.execute(query).fetchall()
    return {str(name): (str(nullable), str(default)) for name, nullable, default in rows}


@pytest.mark.integration
def test_migration_backfills_unrecorded_disposition_for_legacy_rows() -> None:
    """A pre-migration row must read back as ``unrecorded``/``false``, never ``dry_run``/``false``,
    and as ``legacy-unversioned`` with NULL scoring_config (Patrick's 2026-09-11 decision).

    The cycle also proves determinism: downgrade drops the scoring-identity columns
    and a re-upgrade re-applies the identical backfill.
    """
    if os.environ.get("RUN_SERVICE_TESTS") != "1":
        pytest.skip("set RUN_SERVICE_TESTS=1 with the documented local services")
    module = _load_module()
    database_url = module.validate_loopback_database_url(
        os.environ.get("DATABASE_URL", LOCAL_DATABASE_URL)
    )
    backend = module.RealMigrationBackend(database_url)
    database_name = f"pit_backfill_{uuid.uuid4().hex}"
    temporary_url = (
        make_url(database_url).set(database=database_name).render_as_string(hide_password=False)
    )
    assessment_id = str(uuid.uuid4())

    backend.create_database(database_name)
    try:
        backend.run_alembic(temporary_url, "upgrade", "002_risk_assessments")
        _insert_legacy_row(temporary_url, assessment_id)
        backend.run_alembic(temporary_url, "upgrade", "head")
        disposition, dry_run = _read_legacy_disposition(temporary_url, assessment_id)
        version, config = _read_legacy_scoring_identity(temporary_url, assessment_id)
        shapes = _scoring_identity_column_shapes(temporary_url)

        backend.run_alembic(temporary_url, "downgrade", "002_risk_assessments")
        downgraded_shapes = _scoring_identity_column_shapes(temporary_url)

        backend.run_alembic(temporary_url, "upgrade", "head")
        re_version, re_config = _read_legacy_scoring_identity(temporary_url, assessment_id)
    finally:
        backend.drop_database(database_name)

    assert disposition == "unrecorded"
    assert dry_run is False
    assert version == "legacy-unversioned"
    assert config is None
    # NOT NULL without an insert default: only the backfill may produce the label.
    assert shapes["scoring_algorithm_version"] == ("NO", "")
    assert shapes["scoring_config"][0] == "YES"
    assert downgraded_shapes == {}
    assert (re_version, re_config) == ("legacy-unversioned", None)
