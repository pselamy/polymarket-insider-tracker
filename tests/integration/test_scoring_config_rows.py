"""T017: every row the layer-3 pipeline writes is a 004.1 document the layer-1 reader accepts.

The persisted ``scoring_config`` of real pipeline rows (real scorer, real repository, in-memory
SQLite) is validated against the committed JSON Schema and the contract's cross-field
reconstruction rules (``scripts.spec004_evidence.check_envelope``), and read back through the
version-dispatched reader. Old 003.1 rows still read back unchanged.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from scripts.spec004_evidence.check_envelope import reconstruct, schema_for, validate
from scripts.spec004_evidence.replay_envelopes import PRE_T027
from sqlalchemy.ext.asyncio import AsyncEngine
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

from polymarket_insider_tracker.detector.scoring_config import (
    VERSION_003,
    VERSION_004_1,
    Document004,
    JsonValue,
    PureConfig,
    WalletGroupStatus,
    read_scoring_config,
    serialize_pure_config,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata
from polymarket_insider_tracker.ingestor.volume_observation import GAMMA_SOURCE, VolumeObservation
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import RiskAssessmentModel
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


def read_row(row: RiskAssessmentModel) -> Document004:
    assert row.scoring_algorithm_version == VERSION_004_1
    assert contract_errors(row.scoring_config) == []
    document = read_scoring_config(row.scoring_algorithm_version, row.scoring_config)
    assert isinstance(document, Document004)
    return document


def evidence_of(document: Document004) -> dict[str, JsonValue]:
    assert document.evaluation_evidence is not None
    return dict(document.evaluation_evidence)


def member(evidence: dict[str, JsonValue], name: str) -> dict[str, JsonValue]:
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

    assert document.wallet_group_status is WalletGroupStatus.UNQUALIFIED_PRE_T027
    assert document.detector_failures == ()
    # The pipeline's default detectors: max_nonce 5 inclusive is the exclusive boundary 6.
    assert document.detector_config.fresh_transaction_boundary == 6
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


def seeded_row(assessment_id: str, version: str, config: str | None) -> RiskAssessmentDTO:
    """A row as an earlier slice wrote it: ``version`` with its verbatim ``config``."""
    return RiskAssessmentDTO(
        assessment_id=assessment_id,
        trade_id=f"trade-{assessment_id}",
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
        fresh_wallet_confidence=Decimal("0.700"),
        size_anomaly_confidence=Decimal("0.500"),
        is_niche_market=False,
        volume_impact=None,
        book_impact=None,
        wallet_age_hours=None,
        should_alert=False,
        threshold_at_eval=Decimal("0.800"),
        scoring_algorithm_version=version,
        scoring_config=config,
    )


def assert_003_1_row_unchanged(row: RiskAssessmentModel) -> None:
    """The 003.1 row reads back through the dispatch as the exact document it stored."""
    config = read_scoring_config(row.scoring_algorithm_version, row.scoring_config)

    assert isinstance(config, PureConfig)
    assert config.multi_signal_bonus_3 == Decimal("1.3")
    assert config.alert_threshold == row.threshold_at_eval
    assert serialize_pure_config(config) == row.scoring_config == ROW_003_1


async def test_a_003_1_row_reads_back_unchanged(
    db_manager: DatabaseManager, async_engine: AsyncEngine
) -> None:
    async with db_manager.get_async_session() as session:
        await RiskAssessmentRepository(session).insert(
            seeded_row("row-003-1", VERSION_003, ROW_003_1)
        )

    (row,) = await persisted_assessments(async_engine)
    assert_003_1_row_unchanged(row)
