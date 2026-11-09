"""T017: every row the pipeline writes is a 004.1 document the reader accepts.

Persisted scoring_config is validated against JSON Schema and reconstruction rules,
and read back through version dispatch. Old 003.1 rows read back unchanged.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis
from scripts.spec004_evidence.check_envelope import reconstruct, schema_for, validate
from scripts.spec004_evidence.replay_envelopes import PRE_T027
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from tests.detector.test_scoring_config import GOLDEN_PURE
from tests.fakes import (
    FailingDetector,
    FakeAlertChannel,
    FakeEth,
    Substitutes,
    make_test_settings,
)
from tests.fakes import wire_pipeline as wire
from tests.integration._journey_harness import (
    make_sample_trade_event,
    market_for_trade,
    persisted_assessments,
    stamped_volume_market,
)

from polymarket_insider_tracker.detector import scoring_config as sc
from polymarket_insider_tracker.ingestor.models import MarketMetadata
from polymarket_insider_tracker.ingestor.volume_observation import GAMMA_SOURCE, VolumeObservation
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import Base, RiskAssessmentModel
from polymarket_insider_tracker.storage.repos import RiskAssessmentDTO, RiskAssessmentRepository

# A 003.1 row as slice 003 wrote it (3+ bonus 1.3); it must never be rewritten or reinterpreted.
ROW_003_1 = (
    '{"alert_threshold":"0.800","multi_signal_bonus_2":"1.2","multi_signal_bonus_3":"1.3",'
    '"score_quantum":"0.001","weights":{"fresh_wallet":"0.4","niche_market":"0.25",'
    '"size_anomaly":"0.35"}}'
)


def contract_errors(text: str | None) -> list[str]:
    """The schema violations and failed reconstruction rules of a stored document."""
    assert text is not None
    document = json.loads(text)
    schema = schema_for(PRE_T027)
    return validate(document, schema, schema) + reconstruct(document)


def read_row(row: RiskAssessmentModel) -> sc.Document004:
    assert row.scoring_algorithm_version == sc.VERSION_004_1
    assert contract_errors(row.scoring_config) == []
    document = sc.read_scoring_config(row.scoring_algorithm_version, row.scoring_config)
    assert isinstance(document, sc.Document004)
    return document


def evidence_of(document: sc.Document004) -> dict[str, sc.JsonValue]:
    assert document.evaluation_evidence is not None
    return dict(document.evaluation_evidence)


def member(evidence: dict[str, sc.JsonValue], name: str) -> dict[str, sc.JsonValue]:
    value = evidence[name]
    assert isinstance(value, dict)
    return value


async def single_row(engine: AsyncEngine, pipeline: Pipeline) -> RiskAssessmentModel:
    await pipeline.process_trade(make_sample_trade_event())
    rows = await persisted_assessments(engine)
    assert len(rows) == 1
    return rows[0]


async def wired(
    redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    market: MarketMetadata | None,
    threshold: float = 0.8,
    substitutes: Substitutes | None = None,
) -> Pipeline:
    return await wire(
        make_test_settings(persist_assessments=True, alert_threshold=threshold),
        redis=redis,
        eth=FakeEth(transaction_count=0),
        market=market,
        db_manager=db_manager,
        channels=[FakeAlertChannel("discord")],
        substitutes=substitutes,
    )


@pytest.mark.parametrize(
    ("threshold", "stored", "alerts"), [(0.8, "0.800", False), (0.4, "0.400", True)]
)
async def test_a_scored_row_is_a_004_1_document_whose_threshold_reconstructs(
    fake_redis: FakeAsyncRedis,
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    threshold: float,
    stored: str,
    alerts: bool,
) -> None:
    """Default (0.80, R0) and operator-override thresholds both reconstruct from the row."""
    market = stamped_volume_market(make_sample_trade_event())
    pipeline = await wired(fake_redis, db_manager, market, threshold)

    row = await single_row(async_engine, pipeline)
    document = read_row(row)

    assert document.pure.alert_threshold == Decimal(stored) == row.threshold_at_eval
    evidence = evidence_of(document)
    result = member(evidence, "result")
    assert result == {"weighted_score": "0.450", "signals_triggered": 1, "should_alert": alerts}
    assert row.should_alert is alerts
    assert Decimal(str(result["weighted_score"])) == row.weighted_score
    assert member(evidence, "contributions") == {
        "fresh_wallet": "0",
        "size_anomaly": "0.26250",
        "niche_market": "0.18750",
        "bonus": "1",
        "capped_score": "0.45000",
    }


async def test_the_row_records_the_wallet_as_non_scoring_evidence_and_the_volume_state(
    fake_redis: FakeAsyncRedis, db_manager: DatabaseManager, async_engine: AsyncEngine
) -> None:
    market = stamped_volume_market(make_sample_trade_event())
    pipeline = await wired(fake_redis, db_manager, market)

    document = read_row(await single_row(async_engine, pipeline))

    assert document.wallet_group_status is sc.WalletGroupStatus.UNQUALIFIED_PRE_T027
    assert document.detector_failures == ()
    # The pipeline's default detectors: exclusive boundary 5.
    assert document.detector_config.fresh_transaction_boundary == 5
    evidence = evidence_of(document)
    wallet = member(evidence, "wallet")
    assert wallet["classification"] == "low_activity_unknown_age"
    assert (wallet["tx_count"], wallet["confidence"], wallet["factors"]) == (0, None, {})
    assert (wallet["age_state"], wallet["age_basis"], wallet["age_hours"]) == (
        "unknown",
        "none",
        None,
    )
    size = member(evidence, "size")
    assert (size["volume_state"], size["daily_volume"], size["volume_impact"]) == (
        "available",
        "10000",
        "0.3250",
    )
    assert (size["signal_emitted"], size["confidence"], size["niche_basis"]) == (
        True,
        "0.750",
        "numeric",
    )
    assert evidence["notional"] == "3250.00"


async def test_a_verbatim_numeric_raw_volume_is_stored_and_read_back(
    fake_redis: FakeAsyncRedis, db_manager: DatabaseManager, async_engine: AsyncEngine
) -> None:
    """Gamma's volume24hr is a JSON number; its decoded float is stored verbatim (section 1.1a)."""
    trade = make_sample_trade_event()
    stamp = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    observation = VolumeObservation(True, 10000.5, stamp, GAMMA_SOURCE)
    market = replace(market_for_trade(trade, "science"), volume=observation)
    pipeline = await wired(fake_redis, db_manager, market)

    document = read_row(await single_row(async_engine, pipeline))

    size = member(evidence_of(document), "size")
    assert (size["volume24hr_raw"], size["daily_volume"]) == (10000.5, "10000.5")


async def test_a_wallet_retrieval_failure_is_recorded_beside_the_size_evidence(
    fake_redis: FakeAsyncRedis, db_manager: DatabaseManager, async_engine: AsyncEngine
) -> None:
    pipeline = await wired(
        fake_redis,
        db_manager,
        stamped_volume_market(make_sample_trade_event()),
        substitutes=Substitutes(fresh_detector=FailingDetector(RuntimeError("polygon rpc down"))),
    )

    document = read_row(await single_row(async_engine, pipeline))

    (failure,) = document.detector_failures
    assert (failure.detector, failure.error_class) == ("fresh_wallet", "RuntimeError")
    assert "polygon rpc down" in failure.message
    wallet = member(evidence_of(document), "wallet")
    assert (wallet["classification"], wallet["retrieval_error"]) == (
        "retrieval_failed",
        "RuntimeError",
    )
    assert (wallet["tx_count"], wallet["observation_time"]) == (None, None)


async def test_a_detector_failure_row_carries_its_failures_and_no_evaluation_evidence(
    fake_redis: FakeAsyncRedis, db_manager: DatabaseManager, async_engine: AsyncEngine
) -> None:
    """Section 4: the skip row is self-describing without inventing observations."""
    pipeline = await wired(
        fake_redis,
        db_manager,
        None,
        substitutes=Substitutes(
            fresh_detector=FailingDetector(RuntimeError("polygon rpc down")),
            size_detector=FailingDetector(ValueError("metadata down")),
        ),
    )

    row = await single_row(async_engine, pipeline)
    document = read_row(row)

    assert row.delivery_disposition == "detector_failure"
    assert (document.evaluation_evidence, document.wallet_group_status) == (None, None)
    assert [(f.detector, f.error_class) for f in document.detector_failures] == [
        ("fresh_wallet", "RuntimeError"),
        ("size_anomaly", "ValueError"),
    ]


def seeded_row(
    assessment_id: str,
    version: str,
    config: str | None,
    fresh: Decimal | None = Decimal("0.700"),
    volume: Decimal | None = None,
    book: Decimal | None = None,
    trade_id: str | None = None,
) -> RiskAssessmentDTO:
    """A row as an earlier slice wrote it: ``version`` with its verbatim ``config``."""
    return RiskAssessmentDTO(
        assessment_id=assessment_id,
        trade_id=trade_id or f"trade-{assessment_id}",
        wallet_address="0x" + "b" * 40,
        market_id="0x" + "c" * 64,
        asset_id=None,
        side="BUY",
        outcome=None,
        outcome_index=None,
        price=Decimal("0.65"),
        size=Decimal("5000"),
        notional_usdc=Decimal("3250"),
        trade_timestamp=datetime(2026, 9, 11, tzinfo=UTC),
        weighted_score=Decimal("0.600"),
        signals_triggered=2,
        fresh_wallet_confidence=fresh,
        size_anomaly_confidence=Decimal("0.500"),
        is_niche_market=False,
        volume_impact=volume,
        book_impact=book,
        wallet_age_hours=None,
        should_alert=False,
        threshold_at_eval=Decimal("0.800"),
        scoring_algorithm_version=version,
        scoring_config=config,
    )


def assert_003_1_row_unchanged(row: RiskAssessmentModel) -> None:
    """The 003.1 row reads back through the dispatch as the exact document it stored."""
    config = sc.read_scoring_config(row.scoring_algorithm_version, row.scoring_config)
    assert isinstance(config, sc.PureConfig)
    assert config.multi_signal_bonus_3 == Decimal("1.3")
    assert config.alert_threshold == row.threshold_at_eval
    assert sc.serialize_pure_config(config) == row.scoring_config == ROW_003_1


async def test_a_003_1_row_reads_back_unchanged(
    db_manager: DatabaseManager, async_engine: AsyncEngine
) -> None:
    async with db_manager.get_async_session() as session:
        await RiskAssessmentRepository(session).insert(
            seeded_row("row-003-1", sc.VERSION_003, ROW_003_1)
        )

    (row,) = await persisted_assessments(async_engine)
    assert_003_1_row_unchanged(row)


def _make_manager(engine: AsyncEngine) -> DatabaseManager:
    manager = DatabaseManager(
        "postgresql+psycopg://tracker:unused@127.0.0.1:1/unit-tests", async_mode=True
    )
    manager.attach_async_engine(engine)
    return manager


def _compat_row(
    aid: str, ver: str, cfg: str | None, fresh: str, vol: str, book: str, tid: str
) -> RiskAssessmentDTO:
    return seeded_row(
        aid, ver, cfg, fresh=Decimal(fresh), volume=Decimal(vol), book=Decimal(book), trade_id=tid
    )


async def _seed_compat_rows(db: str) -> None:
    engine = create_async_engine(db, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    manager = _make_manager(engine)
    async with manager.get_async_session() as session:
        repo = RiskAssessmentRepository(session)
        await repo.insert(
            _compat_row("legacy", sc.LEGACY_VERSION, None, "0.65", "0.0300", "0.0500", "0xlegacy")
        )
        await repo.insert(
            _compat_row("003", sc.VERSION_003, GOLDEN_PURE, "0.70", "0.0400", "0.0800", "0x003")
        )
    await engine.dispose()


async def _score_once(db: str, threshold: float = 0.8) -> None:
    engine = create_async_engine(db, echo=False)
    trade = replace(make_sample_trade_event(), trade_id="0x004")
    pipeline = await wire(
        make_test_settings(persist_assessments=True, alert_threshold=threshold),
        redis=FakeAsyncRedis(),
        eth=FakeEth(transaction_count=10),
        market=stamped_volume_market(trade),
        db_manager=_make_manager(engine),
        channels=[FakeAlertChannel("discord")],
    )
    await pipeline.process_trade(trade)
    await engine.dispose()


async def _rows_by_trade(db: str) -> dict[str, RiskAssessmentModel]:
    engine = create_async_engine(db, echo=False)
    rows = await persisted_assessments(engine)
    await engine.dispose()
    return {str(row.trade_id): row for row in rows}


async def test_restart_and_legacy_003_004_row_compatibility_preserve_version_semantics(
    tmp_path: Path,
) -> None:
    """SOL-001: restart and legacy/003/004 row compatibility preserve version semantics."""
    db = f"sqlite+aiosqlite:///{tmp_path / 'compat.db'}"
    await _seed_compat_rows(db)
    await _score_once(db, threshold=0.8)

    by_id = await _rows_by_trade(db)
    assert set(by_id) == {"0xlegacy", "0x003", "0x004"}

    assert (
        sc.read_scoring_config(
            by_id["0xlegacy"].scoring_algorithm_version, by_id["0xlegacy"].scoring_config
        )
        is None
    )
    assert isinstance(
        sc.read_scoring_config(
            by_id["0x003"].scoring_algorithm_version, by_id["0x003"].scoring_config
        ),
        sc.PureConfig,
    )
    assert isinstance(
        sc.read_scoring_config(
            by_id["0x004"].scoring_algorithm_version, by_id["0x004"].scoring_config
        ),
        sc.Document004,
    )

    raw_003 = json.loads(GOLDEN_PURE)
    raw_004 = json.loads(by_id["0x004"].scoring_config or "{}")
    assert set(raw_003).issubset(raw_004)
    assert raw_004["weights"] == raw_003["weights"]
    assert raw_004["alert_threshold"] == raw_003["alert_threshold"]

    leg = by_id["0xlegacy"]
    assert (leg.fresh_wallet_confidence, leg.volume_impact, leg.book_impact) == (
        Decimal("0.65"),
        Decimal("0.0300"),
        Decimal("0.0500"),
    )
    r003 = by_id["0x003"]
    assert (r003.fresh_wallet_confidence, r003.volume_impact, r003.book_impact) == (
        Decimal("0.70"),
        Decimal("0.0400"),
        Decimal("0.0800"),
    )
    r004 = by_id["0x004"]
    assert (r004.fresh_wallet_confidence, r004.book_impact) == (None, None)
