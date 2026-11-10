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
from collections.abc import Sequence
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
from polymarket_insider_tracker.detector.evaluation import TradeEvidence
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


def _assert_signals_never_contain_prohibited(signals: Sequence[str]) -> None:
    for prohibited in ("Fresh Wallet", "Sniper", "Funding"):
        assert prohibited not in signals


@pytest.mark.asyncio
async def test_operational_core_only_and_book_depth_non_operational() -> None:
    """T013: core signals only, book-depth non-operational (FR-001, FR-006, SC-004)."""
    bundle_fields = tuple(f.name for f in dataclasses.fields(SignalBundle))
    assert bundle_fields == (
        "trade_event",
        "fresh_wallet_signal",
        "size_anomaly_signal",
        "evidence",
    )

    assessment_fields = tuple(f.name for f in dataclasses.fields(RiskAssessment))
    assert not any("sniper" in f.lower() or "funding" in f.lower() for f in assessment_fields)

    assert set(DEFAULT_WEIGHTS.keys()) == {"fresh_wallet", "size_anomaly", "niche_market"}
    assert DEFAULT_WALLET_GROUP_STATUS is WalletGroupStatus.UNQUALIFIED_PRE_T027

    trade = create_trade_event(size=Decimal("3250"), price=Decimal("1"))
    metadata = FakeMetadataSync()
    metadata.fallback = stamped_volume_market(trade, volume="10000")
    detector = SizeAnomalyDetector(metadata)
    now = datetime.now(UTC)
    eval_with_book = await detector.evaluate(trade, evaluated_at=now, book_depth=Decimal("50000"))
    eval_no_book = await detector.evaluate(trade, evaluated_at=now)

    for evaluation in (eval_with_book, eval_no_book):
        assert evaluation.signal is not None
        assert evaluation.signal.book_impact_measured is None
        evidence = TradeEvidence(now, default_detector_config(), size=evaluation)
        bundle = SignalBundle(trade, None, evaluation.signal, evidence)
        assessment = await RiskScorer(FakeAsyncRedis()).assess(bundle)
        doc = compose_document(assessment, assessment.scoring_config or "{}", evidence)
        assert doc.evaluation_evidence is not None
        size_doc = doc.evaluation_evidence.get("size")
        assert isinstance(size_doc, dict)
        assert size_doc["book_state"] == "unavailable"
        assert size_doc["book_impact"] is None

        dto = assessment_dto(
            assessment, alert_threshold=0.8, detector_config=default_detector_config()
        )
        assert dto.book_impact is None

    empty_assessment = await RiskScorer(FakeAsyncRedis()).assess(
        SignalBundle(trade, None, None, None)
    )
    assert get_triggered_signals(empty_assessment) == []

    non_niche_trade = create_trade_event(size=Decimal("5000"), price=Decimal("1"))
    meta_large = FakeMetadataSync()
    meta_large.fallback = stamped_volume_market(non_niche_trade, volume="100000")
    eval_large = await SizeAnomalyDetector(meta_large).evaluate(non_niche_trade, evaluated_at=now)
    non_niche_assessment = await RiskScorer(FakeAsyncRedis()).assess(
        SignalBundle(non_niche_trade, None, eval_large.signal, None)
    )
    assert get_triggered_signals(non_niche_assessment) == ["Large Position"]

    niche_assessment = await RiskScorer(FakeAsyncRedis()).assess(
        SignalBundle(trade, None, eval_no_book.signal, None)
    )
    assert get_triggered_signals(niche_assessment) == ["Large Position", "Niche Market"]

    for assessed in (empty_assessment, non_niche_assessment, niche_assessment):
        _assert_signals_never_contain_prohibited(get_triggered_signals(assessed))


@pytest.mark.asyncio
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
) -> tuple[Pipeline, FormattedAlert]:
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
    assert len(channel.deliveries) == 1
    return pipeline, channel.deliveries[0]


@pytest.mark.asyncio
async def test_funding_tracing_is_best_effort_enrichment_only(
    db_manager: DatabaseManager,
    async_engine: AsyncEngine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T013: funding tracing is best-effort enrichment only (FR-013, SC-004)."""
    fixed_instant = datetime.now(UTC)
    redis = FakeAsyncRedis()
    trade = create_trade_event(
        wallet_address="0x4444444444444444444444444444444444444444",
        size=Decimal("3250"),
        price=Decimal("1"),
    )
    market = stamped_volume_market(trade)
    channel_a = FakeAlertChannel("discord")
    pipe_a, alert_a = await _run_funding_pipeline(
        SuccessfulFundingTracer(), trade, market, db_manager, redis, channel_a, fixed_instant
    )
    assert pipe_a.stats.errors == 0

    for key in await scanned_keys(redis, "alert:*"):
        await redis.delete(key)

    channel_b = FakeAlertChannel("discord")
    with caplog.at_level(logging.WARNING):
        pipe_b, alert_b = await _run_funding_pipeline(
            FailingFundingTracer(), trade, market, db_manager, redis, channel_b, fixed_instant
        )

    assert pipe_b.stats.errors == 0
    assert any("Failed to persist wallet/funding data" in r.message for r in caplog.records)

    rows = await persisted_assessments(async_engine)
    assert len(rows) == 2
    row_a, row_b = rows[-2], rows[-1]

    assert row_a.weighted_score == row_b.weighted_score == Decimal("0.450")
    assert row_a.signals_triggered == row_b.signals_triggered == 1
    assert row_a.should_alert == row_b.should_alert is True

    doc_a = json.loads(row_a.scoring_config or "{}")["evaluation_evidence"]
    doc_b = json.loads(row_b.scoring_config or "{}")["evaluation_evidence"]
    for section in ("contributions", "wallet", "size", "result"):
        assert doc_a[section] == doc_b[section]

    assert alert_a.title == alert_b.title
    assert alert_a.body == alert_b.body
    assert alert_a.plain_text == alert_b.plain_text
    assert alert_a.telegram_markdown == alert_b.telegram_markdown
    assert alert_a.discord_embed == alert_b.discord_embed
    assert "funding" not in alert_a.plain_text.lower()
    assert "funding" not in alert_b.plain_text.lower()

    async with async_sessionmaker(bind=async_engine, expire_on_commit=False)() as session:
        result = await session.execute(
            select(FundingTransferModel).where(
                FundingTransferModel.to_address == trade.wallet_address.lower()
            )
        )
        persisted_transfers = list(result.scalars().all())
    assert len(persisted_transfers) == 1


def test_no_executable_backtest_runtime_claims_and_research_only_alert_wording() -> None:
    """T013: no backtest CLI options and research-only alert wording (FR-014, SC-004, SC-007)."""
    parser = create_parser()
    action_flags = {opt for action in parser._actions for opt in action.option_strings}
    assert "--backtest" not in action_flags
    assert "--calibrate" not in action_flags

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
    alert = AlertFormatter(verbosity="detailed").format(assessment)

    assert "Suspicious Activity Detected" in alert.title
    assert "Risk Score" in alert.plain_text

    for surface in (
        alert.title,
        alert.body,
        alert.plain_text,
        alert.telegram_markdown,
        str(alert.discord_embed),
    ):
        for prohibited in PROHIBITED_TERMS:
            assert prohibited not in surface.lower()


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


def test_implementation_validation_cited_test_nodes_exist_in_pytest_collection() -> None:
    path = REPO_ROOT / "specs/004-truthful-detection-contract/evidence/implementation-validation.md"
    cited = _cited_test_nodes(path.read_text(encoding="utf-8"))
    # Pinned to the 59 citations in implementation-validation.md; update with that document.
    assert len(cited) == 59
    assert sorted(set(cited) - _collected_test_nodes()) == []
