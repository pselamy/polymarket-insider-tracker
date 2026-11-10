"""Capability-consistency tests covering T013 obligations.

Contract: FR-001, FR-006, FR-012–014, FR-018, SC-004, SC-007.
Asserts operational core only, book-depth non-operational, sniper module experimental and
non-operational in live pipeline, funding tracing best-effort enrichment only, and research-only
alert wording with no executable backtest CLI options.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import logging
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from polymarket_insider_tracker.__main__ import create_parser
from polymarket_insider_tracker.alerter.formatter import AlertFormatter, get_triggered_signals
from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.assessment_row import assessment_dto
from polymarket_insider_tracker.detector.detector_config import default_detector_config
from polymarket_insider_tracker.detector.evaluation import SizeEvaluation, TradeEvidence
from polymarket_insider_tracker.detector.evaluation_clock import Clock
from polymarket_insider_tracker.detector.evidence_document import compose_document
from polymarket_insider_tracker.detector.models import (
    RiskAssessment,
    SizeAnomalySignal,
    SniperClusterSignal,
)
from polymarket_insider_tracker.detector.scorer import DEFAULT_WEIGHTS, RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.scoring_config import (
    DEFAULT_WALLET_GROUP_STATUS,
    WalletGroupStatus,
)
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.detector.sniper import SniperDetector
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.pipeline_components import PipelineComponents, build_components
from polymarket_insider_tracker.profiler.funding import FundingTracer
from polymarket_insider_tracker.profiler.models import FundingChain, FundingTransfer
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import FundingTransferModel
from polymarket_insider_tracker.storage.repos import RiskAssessmentDTO
from tests.detector.test_fresh_wallet import create_trade_event
from tests.fakes import FakeEth, WiredPipeline, make_test_settings, wire_pipeline
from tests.fakes.alerts import FakeAlertChannel
from tests.fakes.metadata import FakeMetadataSync
from tests.fakes.redis_await import scanned_keys
from tests.integration._journey_harness import persisted_assessments, stamped_volume_market

PROHIBITED_TERMS: tuple[str, ...] = (
    "insider trading confirmed",
    "illegal",
    "guaranteed profit",
    "financial advice",
    "buy now",
)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class SuccessfulFundingTracer(FundingTracer):
    """Working fake funding tracer returning a one-hop CEX funding transfer."""

    def __init__(self) -> None:
        pass

    async def trace(self, address: str, max_hops: int | None = None) -> FundingChain:
        _ = max_hops
        transfer = FundingTransfer(
            from_address="0x1111111111111111111111111111111111111111",
            to_address=address.lower(),
            amount=Decimal("1000"),
            token="USDC",
            tx_hash="0x" + "2" * 64,
            block_number=12345,
            timestamp=datetime.now(UTC),
        )
        return FundingChain(
            target_address=address.lower(),
            chain=[transfer],
            origin_address="0x1111111111111111111111111111111111111111",
            origin_type="cex",
            hop_count=1,
            traced_at=datetime.now(UTC),
        )


class FailingFundingTracer(FundingTracer):
    """Failing fake funding tracer simulating Polygon RPC timeout."""

    def __init__(self) -> None:
        pass

    async def trace(self, address: str, max_hops: int | None = None) -> FundingChain:
        _ = (address, max_hops)
        raise RuntimeError("polygon funding rpc timeout")


async def _ignore_trade(_trade: TradeEvent) -> None:
    return None


def _ignore_state(_state: IngestionState) -> None:
    return None


@dataclass(frozen=True)
class CoreObservation:
    """What the operational-core scenarios produced. The record adds no asserts; its observe helpers assert only input preconditions."""

    bundle_fields: tuple[str, ...]
    assessment_sniper_or_funding_fields: list[str]
    weight_keys: set[str]
    group_status: WalletGroupStatus
    book_measured_with_depth: object
    book_measured_without_depth: object
    doc_book_state_with_depth: object
    doc_book_state_without_depth: object
    doc_book_impact_with_depth: object
    doc_book_impact_without_depth: object
    dto_book_impacts: list[object]
    empty_triggered: list[str]
    large_triggered: list[str]
    niche_triggered: list[str]
    prohibited_triggered: list[str]


async def _one_book_view(
    trade: TradeEvent, now: datetime, evaluation: SizeEvaluation
) -> tuple[object, Mapping[str, object], object]:
    """Book impact, evidence document and persisted book impact for one size evaluation."""
    measured = evaluation.signal.book_impact_measured if evaluation.signal else "signal"
    evidence = TradeEvidence(now, default_detector_config(), size=evaluation)
    assessment = await RiskScorer(FakeAsyncRedis()).assess(
        SignalBundle(trade, None, evaluation.signal, evidence)
    )
    doc = compose_document(assessment, assessment.scoring_config or "{}", evidence)
    size_doc = doc.evaluation_evidence.get("size") if doc.evaluation_evidence else None
    dto = assessment_dto(assessment, alert_threshold=0.8, detector_config=default_detector_config())
    return measured, size_doc if isinstance(size_doc, dict) else {}, dto.book_impact


async def _book_depth_views(
    trade: TradeEvent, now: datetime
) -> tuple[list[object], list[Mapping[str, object]], list[object]]:
    """Book impact, evidence document and persisted record with and without book depth."""
    metadata = FakeMetadataSync()
    metadata.fallback = stamped_volume_market(trade, volume="10000")
    detector = SizeAnomalyDetector(metadata)
    eval_with = await detector.evaluate(trade, evaluated_at=now, book_depth=Decimal("50000"))
    eval_without = await detector.evaluate(trade, evaluated_at=now)
    views = [await _one_book_view(trade, now, ev) for ev in (eval_with, eval_without)]
    return [v[0] for v in views], [v[1] for v in views], [v[2] for v in views]


async def _trigger_views(
    trade: TradeEvent, now: datetime
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Triggered signal names for an empty bundle, a large non-niche trade and a niche trade."""
    empty = await RiskScorer(FakeAsyncRedis()).assess(SignalBundle(trade, None, None, None))
    non_niche_trade = create_trade_event(size=Decimal("5000"), price=Decimal("1"))
    meta_large = FakeMetadataSync()
    meta_large.fallback = stamped_volume_market(non_niche_trade, volume="100000")
    eval_large = await SizeAnomalyDetector(meta_large).evaluate(non_niche_trade, evaluated_at=now)
    large = await RiskScorer(FakeAsyncRedis()).assess(
        SignalBundle(non_niche_trade, None, eval_large.signal, None)
    )
    meta_niche = FakeMetadataSync()
    meta_niche.fallback = stamped_volume_market(trade, volume="10000")
    eval_niche = await SizeAnomalyDetector(meta_niche).evaluate(trade, evaluated_at=now)
    niche = await RiskScorer(FakeAsyncRedis()).assess(
        SignalBundle(trade, None, eval_niche.signal, None)
    )
    prohibited = [
        name
        for assessed in (empty, large, niche)
        for name in get_triggered_signals(assessed)
        if name in ("Fresh Wallet", "Sniper", "Funding")
    ]
    return (
        get_triggered_signals(empty),
        get_triggered_signals(large),
        get_triggered_signals(niche),
        prohibited,
    )


async def _observe_core() -> CoreObservation:
    bundle_fields = tuple(f.name for f in dataclasses.fields(SignalBundle))
    assessment_names = [f.name for f in dataclasses.fields(RiskAssessment)]
    trade = create_trade_event(size=Decimal("3250"), price=Decimal("1"))
    now = datetime.now(UTC)
    measured, documents, dto_impacts = await _book_depth_views(trade, now)
    empty_t, large_t, niche_t, prohibited = await _trigger_views(trade, now)
    return CoreObservation(
        bundle_fields=bundle_fields,
        assessment_sniper_or_funding_fields=[
            name
            for name in assessment_names
            if "sniper" in name.lower() or "funding" in name.lower()
        ],
        weight_keys=set(DEFAULT_WEIGHTS.keys()),
        group_status=DEFAULT_WALLET_GROUP_STATUS,
        book_measured_with_depth=measured[0],
        book_measured_without_depth=measured[1],
        doc_book_state_with_depth=documents[0].get("book_state"),
        doc_book_state_without_depth=documents[1].get("book_state"),
        doc_book_impact_with_depth=documents[0].get("book_impact"),
        doc_book_impact_without_depth=documents[1].get("book_impact"),
        dto_book_impacts=dto_impacts,
        empty_triggered=empty_t,
        large_triggered=large_t,
        niche_triggered=niche_t,
        prohibited_triggered=prohibited,
    )


@pytest.fixture
async def core() -> CoreObservation:
    return await _observe_core()


async def test_core_bundle_has_exactly_the_core_signal_fields(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.bundle_fields

    # Assert
    assert actual == (
        "trade_event",
        "fresh_wallet_signal",
        "size_anomaly_signal",
        "evidence",
    )


async def test_guard_assessment_has_no_sniper_or_funding_field(core: CoreObservation) -> None:
    """Guard, not evidence: a field name that never existed cannot fail on pre-#241 code."""
    # Arrange
    # Act
    actual = core.assessment_sniper_or_funding_fields

    # Assert
    assert actual == []


async def test_default_weights_cover_only_the_three_core_groups(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.weight_keys

    # Assert
    assert actual == {"fresh_wallet", "size_anomaly", "niche_market"}


async def test_wallet_group_is_unqualified_pre_t027(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.group_status

    # Assert
    assert actual is WalletGroupStatus.UNQUALIFIED_PRE_T027


async def test_book_depth_with_depth_measures_no_book_impact(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.book_measured_with_depth

    # Assert
    assert actual is None


async def test_book_depth_without_depth_measures_no_book_impact(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.book_measured_without_depth

    # Assert
    assert actual is None


async def test_evidence_document_with_depth_records_the_book_as_unavailable(
    core: CoreObservation,
) -> None:
    # Arrange
    # Act
    actual = core.doc_book_state_with_depth

    # Assert
    assert actual == "unavailable"


async def test_evidence_document_without_depth_records_the_book_as_unavailable(
    core: CoreObservation,
) -> None:
    # Arrange
    # Act
    actual = core.doc_book_state_without_depth

    # Assert
    assert actual == "unavailable"


async def test_evidence_document_with_depth_records_no_book_impact(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.doc_book_impact_with_depth

    # Assert
    assert actual is None


async def test_evidence_document_without_depth_records_no_book_impact(
    core: CoreObservation,
) -> None:
    # Arrange
    # Act
    actual = core.doc_book_impact_without_depth

    # Assert
    assert actual is None


async def test_persisted_record_carries_no_book_impact(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.dto_book_impacts

    # Assert
    assert actual == [None, None]


async def test_empty_bundle_triggers_no_signal(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.empty_triggered

    # Assert
    assert actual == []


async def test_large_non_niche_trade_triggers_only_large_position(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.large_triggered

    # Assert
    assert actual == ["Large Position"]


async def test_niche_trade_triggers_large_position_and_niche_market(core: CoreObservation) -> None:
    # Arrange
    # Act
    actual = core.niche_triggered

    # Assert
    assert actual == ["Large Position", "Niche Market"]


async def test_no_triggered_signal_is_a_prohibited_group(core: CoreObservation) -> None:
    """Guard, not evidence: a triggered signal is never a prohibited group."""
    # Arrange
    # Act
    actual = core.prohibited_triggered

    # Assert
    assert actual == []


async def _built_pipeline_components() -> PipelineComponents:
    settings = make_test_settings(alert_threshold=0.4)
    return await build_components(
        settings, dry_run=True, on_trade=_ignore_trade, on_ingestion_state=_ignore_state
    )


def _sniper_clustering_signals() -> list[SniperClusterSignal]:
    detector = SniperDetector(min_cluster_size=2, min_entries_per_wallet=1, eps=60.0)
    now = datetime.now(UTC)
    for wallet in ["0x" + "1" * 40, "0x" + "2" * 40]:
        trade = create_trade_event(wallet_address=wallet, market_id="mkt1")
        detector.record_entry(trade, now - timedelta(seconds=10))
    return detector.run_clustering()


def test_sniper_is_not_a_pipeline_component_field() -> None:
    """Guard, not evidence: T013: no sniper field on the pipeline components (FR-012, SC-004)."""
    # Arrange
    names = [f.name for f in dataclasses.fields(PipelineComponents)]

    # Act
    sniper_fields = [name for name in names if "sniper" in name.lower()]

    # Assert
    assert sniper_fields == []


async def test_built_pipeline_has_no_sniper_attribute() -> None:
    """Guard, not evidence: T013: the built components carry no sniper attribute (FR-012)."""
    # Arrange
    built = await _built_pipeline_components()
    try:
        # Act
        attributes = set(vars(built))
    finally:
        if built.db_manager is not None:
            await built.db_manager.dispose_async()
        await built.redis.aclose()

    # Assert
    assert not {"sniper_detector", "sniper"} & attributes


def test_sniper_clustering_signals_each_wallet_once() -> None:
    """The experimental sniper library still clusters the two wallets it is given."""
    # Arrange
    wallets = sorted(["0x" + "1" * 40, "0x" + "2" * 40])

    # Act
    signals = _sniper_clustering_signals()

    # Assert
    assert sorted(signal.wallet_address for signal in signals) == wallets


def test_risk_scorer_signature_has_no_sniper_parameter() -> None:
    """Guard, not evidence: The scorer's weighted-score method takes no sniper input (FR-018)."""
    # Arrange
    parameters = inspect.signature(RiskScorer.calculate_weighted_score).parameters

    # Act
    sniper_parameters = [name for name in parameters if "sniper" in name.lower()]

    # Assert
    assert sniper_parameters == []


def test_alert_formatter_signature_has_no_sniper_parameter() -> None:
    """Guard, not evidence: The alert formatter takes no sniper input (FR-018)."""
    # Arrange
    parameters = inspect.signature(AlertFormatter.format).parameters

    # Act
    sniper_parameters = [name for name in parameters if "sniper" in name.lower()]

    # Assert
    assert sniper_parameters == []


def test_assessment_dto_signature_has_no_sniper_parameter() -> None:
    """Guard, not evidence: The persistence mapping takes no sniper input (FR-018)."""
    # Arrange
    parameters = inspect.signature(assessment_dto).parameters

    # Act
    sniper_parameters = [name for name in parameters if "sniper" in name.lower()]

    # Assert
    assert sniper_parameters == []


def test_risk_assessment_dto_has_no_sniper_field() -> None:
    """Guard, not evidence: The persisted record has no sniper column (FR-018)."""
    # Arrange
    names = [f.name for f in dataclasses.fields(RiskAssessmentDTO)]

    # Act
    sniper_fields = [name for name in names if "sniper" in name.lower()]

    # Assert
    assert sniper_fields == []


class FixedClockPipeline(WiredPipeline):
    """Pipeline subclass with a controlled clock for deterministic evaluation timestamps."""

    def set_clock(self, clock: Clock) -> None:
        self._clock = clock


async def _run_funding_pipeline(
    tracer: FundingTracer,
    trade: TradeEvent,
    market: MarketMetadata,
    db_manager: DatabaseManager,
    redis: FakeAsyncRedis,
    channel: FakeAlertChannel,
    fixed_instant: datetime,
) -> tuple[Pipeline, list[FormattedAlert]]:
    settings = make_test_settings(alert_threshold=0.4)
    pipeline = await wire_pipeline(
        settings,
        redis=redis,
        eth=FakeEth(transaction_count=0),
        market=market,
        db_manager=db_manager,
        channels=[channel],
        pipeline_type=FixedClockPipeline,
    )
    assert isinstance(pipeline, FixedClockPipeline)
    pipeline.set_clock(lambda: fixed_instant)
    assert pipeline.components is not None
    pipeline.install_components(dataclasses.replace(pipeline.components, funding_tracer=tracer))
    await pipeline.process_trade(trade)
    return pipeline, list(channel.deliveries)


FUNDING_PIPELINE_IDS = ["successful_tracer", "failing_tracer"]
# Stands in for a pipeline that delivered no alert; its comparisons are false, so only the named fact fails.
UNDELIVERED_ALERT = FormattedAlert(
    title="", body="", discord_embed={}, telegram_markdown="", plain_text=""
)


@dataclass(frozen=True)
class FundingObservation:
    """What the two funding pipelines produced. The record asserts nothing; the one-delivery precondition is the named fact `delivery_counts`. The only asserts reached are setup type narrowing in the pipeline wiring."""

    errors: tuple[int, int]
    delivery_counts: tuple[int, int]
    warning_logged: bool
    row_count: int
    weighted_scores: tuple[object, object]
    signals: tuple[object, object]
    should_alert: tuple[object, object]
    section_equal: dict[str, bool]
    alert_field_equal: dict[str, bool]
    plain_mentions_funding: tuple[bool, bool]
    persisted_transfers: int


async def _observe_funding(
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    caplog: pytest.LogCaptureFixture,
) -> FundingObservation:
    fixed_instant = datetime.now(UTC)
    redis = FakeAsyncRedis()
    trade = create_trade_event(
        wallet_address="0x4444444444444444444444444444444444444444",
        size=Decimal("3250"),
        price=Decimal("1"),
    )
    market = stamped_volume_market(trade)
    channel_a = FakeAlertChannel("discord")
    pipe_a, deliveries_a = await _run_funding_pipeline(
        SuccessfulFundingTracer(), trade, market, db_manager, redis, channel_a, fixed_instant
    )
    for key in await scanned_keys(redis, "alert:*"):
        await redis.delete(key)

    channel_b = FakeAlertChannel("discord")
    with caplog.at_level(logging.WARNING):
        pipe_b, deliveries_b = await _run_funding_pipeline(
            FailingFundingTracer(), trade, market, db_manager, redis, channel_b, fixed_instant
        )

    alert_a = deliveries_a[0] if deliveries_a else UNDELIVERED_ALERT
    alert_b = deliveries_b[0] if deliveries_b else UNDELIVERED_ALERT
    rows = await persisted_assessments(async_engine)
    row_a, row_b = rows[-2], rows[-1]
    doc_a = json.loads(row_a.scoring_config or "{}")["evaluation_evidence"]
    doc_b = json.loads(row_b.scoring_config or "{}")["evaluation_evidence"]
    async with async_sessionmaker(bind=async_engine, expire_on_commit=False)() as session:
        result = await session.execute(
            select(FundingTransferModel).where(
                FundingTransferModel.to_address == trade.wallet_address.lower()
            )
        )
        transfers = list(result.scalars().all())
    return FundingObservation(
        errors=(pipe_a.stats.errors, pipe_b.stats.errors),
        delivery_counts=(len(deliveries_a), len(deliveries_b)),
        warning_logged=any(
            "Failed to persist wallet/funding data" in r.message for r in caplog.records
        ),
        row_count=len(rows),
        weighted_scores=(row_a.weighted_score, row_b.weighted_score),
        signals=(row_a.signals_triggered, row_b.signals_triggered),
        should_alert=(row_a.should_alert, row_b.should_alert),
        section_equal={
            k: doc_a[k] == doc_b[k] for k in ("contributions", "wallet", "size", "result")
        },
        alert_field_equal={
            "title": alert_a.title == alert_b.title,
            "body": alert_a.body == alert_b.body,
            "plain_text": alert_a.plain_text == alert_b.plain_text,
            "telegram_markdown": alert_a.telegram_markdown == alert_b.telegram_markdown,
            "discord_embed": alert_a.discord_embed == alert_b.discord_embed,
        },
        plain_mentions_funding=(
            "funding" in alert_a.plain_text.lower(),
            "funding" in alert_b.plain_text.lower(),
        ),
        persisted_transfers=len(transfers),
    )


@pytest.fixture
async def funding(
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    caplog: pytest.LogCaptureFixture,
) -> FundingObservation:
    return await _observe_funding(db_manager, async_engine, caplog)


@pytest.mark.parametrize("pipeline", [0, 1], ids=FUNDING_PIPELINE_IDS)
async def test_each_funding_pipeline_delivers_one_alert(
    funding: FundingObservation, pipeline: int
) -> None:
    assert funding.delivery_counts[pipeline] == 1


@pytest.mark.parametrize("pipeline", [0, 1], ids=FUNDING_PIPELINE_IDS)
async def test_each_funding_pipeline_runs_without_errors(
    funding: FundingObservation, pipeline: int
) -> None:
    # Arrange
    # Act
    actual = funding.errors[pipeline]

    # Assert
    assert actual == 0


async def test_a_failing_tracer_is_logged_as_a_persistence_warning(
    funding: FundingObservation,
) -> None:
    # Arrange
    # Act
    actual = funding.warning_logged

    # Assert
    assert actual is True


async def test_both_funding_pipelines_persist_one_row_each(funding: FundingObservation) -> None:
    # Arrange
    # Act
    actual = funding.row_count

    # Assert
    assert actual == 2


@pytest.mark.parametrize("pipeline", [0, 1], ids=FUNDING_PIPELINE_IDS)
async def test_each_funding_pipeline_records_the_score_0_450(
    funding: FundingObservation, pipeline: int
) -> None:
    # Arrange
    # Act
    actual = funding.weighted_scores[pipeline]

    # Assert
    assert actual == Decimal("0.450")


@pytest.mark.parametrize("pipeline", [0, 1], ids=FUNDING_PIPELINE_IDS)
async def test_each_funding_pipeline_records_one_triggered_signal(
    funding: FundingObservation, pipeline: int
) -> None:
    # Arrange
    # Act
    actual = funding.signals[pipeline]

    # Assert
    assert actual == 1


@pytest.mark.parametrize("pipeline", [0, 1], ids=FUNDING_PIPELINE_IDS)
async def test_each_funding_pipeline_decides_to_alert(
    funding: FundingObservation, pipeline: int
) -> None:
    # Arrange
    # Act
    actual = funding.should_alert[pipeline]

    # Assert
    assert actual is True


@pytest.mark.parametrize("section", ["contributions", "wallet", "size", "result"])
async def test_evidence_section_is_identical_with_and_without_tracing(
    funding: FundingObservation, section: str
) -> None:
    # Arrange
    # Act
    actual = funding.section_equal[section]

    # Assert
    assert actual is True


@pytest.mark.parametrize(
    "field",
    ["title", "body", "plain_text", "telegram_markdown", "discord_embed"],
)
async def test_alert_field_is_identical_with_and_without_tracing(
    funding: FundingObservation, field: str
) -> None:
    # Arrange
    # Act
    actual = funding.alert_field_equal[field]

    # Assert
    assert actual is True


async def test_plain_alert_with_tracing_does_not_mention_funding(
    funding: FundingObservation,
) -> None:
    # Arrange
    # Act
    actual = funding.plain_mentions_funding[0]

    # Assert
    assert actual is False


async def test_plain_alert_without_tracing_does_not_mention_funding(
    funding: FundingObservation,
) -> None:
    # Arrange
    # Act
    actual = funding.plain_mentions_funding[1]

    # Assert
    assert actual is False


async def test_only_the_successful_tracing_run_persists_a_transfer(
    funding: FundingObservation,
) -> None:
    # Arrange
    # Act
    actual = funding.persisted_transfers

    # Assert
    assert actual == 1


def test_backtest_flag_is_not_a_cli_option() -> None:
    """Guard, not evidence (T013, FR-014): no executable backtest CLI option."""
    # Arrange
    parser = create_parser()

    # Act
    action_flags = {opt for action in parser._actions for opt in action.option_strings}

    # Assert
    assert "--backtest" not in action_flags


def test_calibrate_flag_is_not_a_cli_option() -> None:
    """Guard, not evidence (T013, FR-014): no executable calibration CLI option."""
    # Arrange
    parser = create_parser()

    # Act
    action_flags = {opt for action in parser._actions for opt in action.option_strings}

    # Assert
    assert "--calibrate" not in action_flags


def _research_alert() -> FormattedAlert:
    trade = create_trade_event(size=Decimal("3250"), price=Decimal("1"))
    sig = SizeAnomalySignal(
        trade_event=trade,
        market_metadata=stamped_volume_market(trade),
        volume_impact=0.325,
        book_impact=0.0,
        is_niche_market=True,
        confidence=0.75,
        factors={"volume_impact": 0.5, "niche": 0.25},
    )
    assessment = RiskAssessment(
        trade_event=trade,
        wallet_address=trade.wallet_address,
        market_id=trade.market_id,
        size_anomaly_signal=sig,
        signals_triggered=1,
        weighted_score=0.450,
        should_alert=True,
        delivery_disposition="delivered",
    )
    return AlertFormatter(verbosity="detailed").format(assessment)


def _research_wording_surfaces() -> list[str]:
    alert = _research_alert()
    return [
        alert.title,
        alert.body,
        alert.plain_text,
        alert.telegram_markdown,
        str(alert.discord_embed),
    ]


def test_alert_title_is_the_suspicious_activity_title() -> None:
    """Research-only wording on the public title (SC-007)."""
    # Arrange
    alert = _research_alert()

    # Act
    title = alert.title

    # Assert
    assert "Suspicious Activity Detected" in title


def test_no_public_surface_uses_a_prohibited_term() -> None:
    """Research-only wording: no prohibited term on any rendered surface (SC-007)."""
    matches = [
        (term, index)
        for index, surface in enumerate(_research_wording_surfaces())
        for term in PROHIBITED_TERMS
        if term in surface.lower()
    ]
    assert matches == []


def _cited_test_nodes(markdown: str) -> list[str]:
    section = markdown.split("### Quickstart Journey Scenarios 1–12 Mapping", 1)[1]
    return re.findall(r"`(tests/[^`\s]+)`", section)


def _collected_test_nodes() -> set[str]:
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests",
        "--ignore=tests/tooling",
        "--collect-only",
        "-q",
        "-o",
        "addopts=",
        "-p",
        "no:cacheprovider",
    ]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, check=True, capture_output=True, text=True)
    nodes = {line.strip() for line in proc.stdout.splitlines() if "::" in line}
    return nodes | {re.sub(r"\[.*\]$", "", node) for node in nodes}


def _implementation_validation_citations() -> list[str]:
    path = REPO_ROOT / "specs/004-truthful-detection-contract/evidence/implementation-validation.md"
    return _cited_test_nodes(path.read_text(encoding="utf-8"))


def test_implementation_validation_pins_its_test_citation_count() -> None:
    # Pinned to the 63 citations in implementation-validation.md; update with that document.
    # Arrange
    # Act
    actual = len(_implementation_validation_citations())

    # Assert
    assert actual == 63


def test_implementation_validation_cited_test_nodes_exist_in_pytest_collection() -> None:
    missing = set(_implementation_validation_citations()) - _collected_test_nodes()
    assert sorted(missing) == []
