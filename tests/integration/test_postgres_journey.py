"""Dry-run journey persisted through migrated disposable PostgreSQL.

This is the single-boundary variant of the deterministic end-to-end journey: every
product component, the production repositories, and the PostgreSQL driver path are
real, while Redis remains ``fakeredis`` and market/chain/notification data remain the
existing controlled fakes. The test owns a uniquely named database, applies the
repository's Alembic migrations, and drops the database in teardown.

Selection mirrors the service-profile gates: the test runs only when ``DATABASE_URL``
is present (the ``services`` gates inherit it) and skips cleanly when the deterministic
compatibility suite scrubs service configuration.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from fakeredis import FakeAsyncRedis
from psycopg import sql
from sqlalchemy import select
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from tests.fakes import FakeAlertChannel, FakeClock
from tests.integration._journey_harness import (
    POLL_START,
    journey_settings,
    make_sample_trade_event,
    pipeline_row,
    run_until,
    running_pipeline,
    seeded_server,
    stamped_volume_market,
    wire_journey_pipeline,
)

from polymarket_insider_tracker.detector.scoring_config import (
    VERSION_004_1,
    Document004,
    read_scoring_config,
)
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DATABASE_ENV = "DATABASE_URL"
TX_NUMBER = 9

# Pinned scoring expectations for this deterministic fixture, derived from the fixture
# inputs and the scoring configuration the assessment must record, not copied from an
# observed run. The trade is $5000 x $0.65 = $3250 notional from a nonce-0 wallet, so the
# fresh-wallet detector records base 0.5 + brand-new bonus 0.2 (age unknown, trade below the
# $10,000 large-trade bonus) as NON-SCORING evidence (G1, FR-007c). The "science" market
# carries a fresh stamped $10,000 Gamma volume (T021): the ratio 0.325 saturates the 3x cap, so
# the volume term is 0.5 and the niche multiplier 1.5 makes the size confidence 0.750. Applying
# the recorded weights (size_anomaly 0.35, niche_market 0.25) with one scoring group and no
# bonus gives:
#     0.750*0.35 + 0.750*0.25 = 0.450
# A change to any weight, bonus, or base confidence makes these literals stop matching the
# stored row, so a scoring change fails this test instead of silently passing.
_EXPECTED_FRESH_WALLET_CONFIDENCE = Decimal("0.700")
_EXPECTED_SIZE_ANOMALY_CONFIDENCE = Decimal("0.750")
_EXPECTED_WEIGHTED_SCORE = (
    _EXPECTED_SIZE_ANOMALY_CONFIDENCE * Decimal("0.35")
    + _EXPECTED_SIZE_ANOMALY_CONFIDENCE * Decimal("0.25")
).quantize(Decimal("0.001"))
_EXPECTED_SCORING_CONFIG: dict[str, object] = {
    "alert_threshold": "0.400",
    "multi_signal_bonus_2": "1.2",
    "multi_signal_bonus_3": "1",
    "score_quantum": "0.001",
    "weights": {
        "fresh_wallet": "0.4",
        "niche_market": "0.25",
        "size_anomaly": "0.35",
    },
}


def _required_database_url() -> str:
    """Return the service URL or skip when the deterministic suite scrubbed it."""
    value = os.environ.get(DATABASE_ENV)
    if not value:
        pytest.skip(f"{DATABASE_ENV} is required for the disposable PostgreSQL journey")
    return value


def _admin_url(base_url: str) -> URL:
    return make_url(base_url).set(database="postgres")


def _dsn(url: URL) -> str:
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def _create_database(admin_url: URL, name: str) -> None:
    with psycopg.connect(_dsn(admin_url), autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def _drop_database(admin_url: URL, name: str) -> None:
    with psycopg.connect(_dsn(admin_url), autocommit=True) as connection:
        connection.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def _database_exists(admin_url: URL, name: str) -> bool:
    with psycopg.connect(_dsn(admin_url)) as connection:
        row = connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
    return row is not None


def _migrate(database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Apply the repository's own migration path to the disposable database."""
    config = AlembicConfig(str(REPOSITORY_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(REPOSITORY_ROOT / "alembic"))
    monkeypatch.setenv(DATABASE_ENV, database_url)
    command.upgrade(config, "head")


@contextmanager
def _disposable_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Create, migrate, and always drop a uniquely named PostgreSQL database."""
    base_url = _required_database_url()
    admin_url = _admin_url(base_url)
    name = f"pit_journey_{uuid.uuid4().hex}"
    _create_database(admin_url, name)
    database_url = make_url(base_url).set(database=name).render_as_string(hide_password=False)
    try:
        _migrate(database_url, monkeypatch)
        yield database_url
    finally:
        _drop_database(admin_url, name)


async def _assessment_rows(database_url: str) -> list[RiskAssessmentModel]:
    """Read through a fresh engine and session, independent of the pipeline's."""
    engine = create_async_engine(database_url)
    try:
        async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
            result = await session.execute(select(RiskAssessmentModel))
            return list(result.scalars().all())
    finally:
        await engine.dispose()


def _assert_trade_identity(row: RiskAssessmentModel, trade: TradeEvent) -> None:
    assert row.trade_id == "0x" + f"{TX_NUMBER:064x}"
    assert row.wallet_address == trade.wallet_address.lower()
    assert row.market_id == trade.market_id
    assert row.asset_id == trade.asset_id
    assert row.side == trade.side
    assert row.outcome == trade.outcome
    assert row.outcome_index == trade.outcome_index
    assert row.price == trade.price
    assert row.size == trade.size


def _assert_score_inputs(row: RiskAssessmentModel) -> None:
    assert row.signals_triggered == 1
    assert row.is_niche_market is True
    assert row.fresh_wallet_confidence == _EXPECTED_FRESH_WALLET_CONFIDENCE
    assert row.size_anomaly_confidence == _EXPECTED_SIZE_ANOMALY_CONFIDENCE
    assert row.wallet_tx_count == 0
    assert row.wallet_age_known is False
    assert row.weighted_score == _EXPECTED_WEIGHTED_SCORE
    assert row.threshold_at_eval == Decimal("0.400")
    assert row.scoring_algorithm_version == VERSION_004_1


def _assert_scoring_config(row: RiskAssessmentModel) -> None:
    assert row.scoring_config is not None
    document = read_scoring_config(VERSION_004_1, row.scoring_config)
    assert isinstance(document, Document004)
    stored = json.loads(row.scoring_config)
    assert {key: stored[key] for key in _EXPECTED_SCORING_CONFIG} == _EXPECTED_SCORING_CONFIG
    assert document.evaluation_evidence is not None
    assert document.evaluation_evidence["result"] == {
        "weighted_score": str(_EXPECTED_WEIGHTED_SCORE),
        "signals_triggered": 1,
        "should_alert": True,
    }


def _assert_dry_run_disposition(row: RiskAssessmentModel) -> None:
    assert row.should_alert is True
    assert row.dry_run is True
    assert row.delivery_disposition == "dry_run"
    assert json.loads(row.delivery_channels or "{}") == {
        "discord": "dry_run",
        "telegram": "dry_run",
    }


@pytest.mark.asyncio
async def test_dry_run_journey_persists_in_disposable_postgres(
    fake_redis: FakeAsyncRedis,
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One explainable assessment lands in migrated PostgreSQL; nothing is delivered."""
    with _disposable_database(monkeypatch) as database_url:
        clock = FakeClock(POLL_START)
        server = seeded_server(clock)
        trade = make_sample_trade_event()
        discord = FakeAlertChannel("discord")
        telegram = FakeAlertChannel("telegram")
        settings = journey_settings(dry_run=True, health_port=unused_tcp_port)
        manager = DatabaseManager(database_url, async_mode=True)
        pipeline = await wire_journey_pipeline(
            settings,
            redis=fake_redis,
            market=stamped_volume_market(trade),
            db_manager=manager,
            channels=[discord, telegram],
            trades=server,
            poll_clock=clock,
        )

        try:
            async with running_pipeline(pipeline):
                await run_until(lambda: len(server.requests) >= 1)
                server.publish(pipeline_row(trade, clock, TX_NUMBER))
                await run_until(lambda: pipeline.stats.trades_processed >= 1)
                await run_until(lambda: len(server.requests) >= 4)
        finally:
            await manager.dispose_async()

        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.alerts_sent == 0
        assert discord.deliveries == []
        assert telegram.deliveries == []
        assert await fake_redis.keys("alert:dedup:*") == []
        assert await fake_redis.keys("alert:ambiguous:*") == []

        rows = await _assessment_rows(database_url)
        assert len(rows) == 1
        row = rows[0]
        _assert_trade_identity(row, trade)
        _assert_score_inputs(row)
        _assert_scoring_config(row)
        _assert_dry_run_disposition(row)


def test_disposable_database_cleanup_runs_when_journey_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Teardown drops the database even when the journey fails mid-flight."""
    base_url = _required_database_url()
    admin_url = _admin_url(base_url)
    name = ""

    with (
        pytest.raises(RuntimeError, match="injected journey failure"),
        _disposable_database(monkeypatch) as database_url,
    ):
        name = make_url(database_url).database or ""
        assert name.startswith("pit_journey_")
        assert _database_exists(admin_url, name) is True
        raise RuntimeError("injected journey failure")

    assert name
    assert _database_exists(admin_url, name) is False
