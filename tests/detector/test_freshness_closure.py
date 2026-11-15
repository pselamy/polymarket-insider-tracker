"""Pre-T027 freshness closure and boundary override (T009c, data-model.md section 2a).

Every fact is a separate case over one observed run: the observation helpers assert nothing; each precondition is a named fact.
The boundary rule is a Hypothesis property over (nonce, boundary, analyzer threshold); the
expected values are written from the spec, never computed by the implementation under test.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from hypothesis import given, settings
from hypothesis import strategies as st
from scripts.spec004_evidence.check_envelope import reconstruct, schema_for, validate
from scripts.spec004_evidence.replay_envelopes import PRE_T027

from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.detector.detector_config import capture_detector_config
from polymarket_insider_tracker.detector.evaluation import (
    TradeEvidence,
    WalletClassification,
    WalletEvaluation,
)
from polymarket_insider_tracker.detector.evidence_document import row_scoring_config
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.models import FreshWalletSignal, RiskAssessment
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.scoring_config import (
    VERSION_004_1,
    DetectorConfig,
    Document004,
    JsonValue,
    read_scoring_config,
)
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.ingestor.volume_observation import GAMMA_SOURCE, VolumeObservation
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
from polymarket_insider_tracker.profiler.models import Transaction, WalletInfo, WalletProfile
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.repos import WalletProfileDTO, WalletRepository
from tests.detector.test_fresh_wallet import create_trade_event
from tests.fakes.metadata import FakeMetadataSync
from tests.integration._journey_harness import stamped_volume_market

EVALUATED_AT = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
WALLET_ADDR = "0x1234567890123456789012345678901234567890"


class WorkingPolygonClient:
    """Working structural fake Polygon client.

    WalletAnalyzer computes wallet age from its own clock (datetime.now, no injected seam), so the
    first-transaction stamp is taken from the same clock. The ages used (24 h, unknown) are far from
    the 48 h threshold, so the outcomes do not depend on the exact wall time.
    """

    def __init__(self, nonce: int, age_hours: float | None) -> None:
        self.nonce = nonce
        self.age_hours = age_hours

    async def get_wallet_info(self, address: str) -> WalletInfo:
        first_tx = None
        if self.age_hours is not None:
            first_tx = Transaction(
                hash="0x" + "a" * 64,
                block_number=1000,
                timestamp=datetime.now(UTC) - timedelta(hours=self.age_hours),
                from_address=address,
                to_address="0x" + "b" * 40,
                value=Decimal("0"),
                gas_used=21000,
                gas_price=Decimal("1000000000"),
            )
        return WalletInfo(
            address=address,
            transaction_count=self.nonce,
            balance_wei=Decimal("1000000000000000000"),
            first_transaction=first_tx,
        )

    async def get_token_balance(self, address: str, token_address: str, /) -> Decimal:
        _ = (address, token_address)
        return Decimal("1000")


def _volume_market_at_evaluation(trade: TradeEvent) -> MarketMetadata:
    """A $10,000 volume stamped 30 s before EVALUATED_AT, so the stamp is never wall-clock dependent."""
    stamp = (EVALUATED_AT - timedelta(seconds=30)).isoformat()
    market = stamped_volume_market(trade)
    return replace(market, volume=VolumeObservation(True, "10000", stamp, GAMMA_SOURCE))


async def _evaluate_and_score(
    trade: TradeEvent,
    wallet_detector: FreshWalletDetector,
    evaluated_at: datetime,
) -> tuple[FreshWalletDetector, TradeEvidence, RiskAssessment, DetectorConfig]:
    wallet_eval = await wallet_detector.evaluate(trade, evaluated_at=evaluated_at)
    metadata = FakeMetadataSync()
    metadata.fallback = _volume_market_at_evaluation(trade)
    size_detector = SizeAnomalyDetector(metadata)
    size_eval = await size_detector.evaluate(trade, evaluated_at=evaluated_at)
    config = capture_detector_config(wallet_detector, size_detector)
    evidence = TradeEvidence(evaluated_at, config, wallet_eval, size_eval)
    bundle = SignalBundle(trade, wallet_eval.signal, size_eval.signal, evidence)
    assessment = await RiskScorer(FakeAsyncRedis()).assess(bundle)
    return wallet_detector, evidence, assessment, config


def _envelope_document(
    assessment: RiskAssessment, config: DetectorConfig
) -> tuple[bool, Document004 | None]:
    """The 004.1 envelope: whether it validates against the pre-T027 schema, and the document read."""
    text = row_scoring_config(assessment, config)
    if text is None:
        return False, None
    raw: object = json.loads(text)
    schema = schema_for(PRE_T027)
    valid = validate(raw, schema, schema) + reconstruct(raw) == []
    doc = read_scoring_config(VERSION_004_1, text)
    return valid, (doc if isinstance(doc, Document004) else None)


def _evidence_member(document: Document004 | None, name: str) -> dict[str, JsonValue]:
    """One evidence section; an absent section reads as an empty mapping (its presence is a fact)."""
    if document is None or document.evaluation_evidence is None:
        return {}
    val = document.evaluation_evidence.get(name)
    if isinstance(val, dict):
        return val
    return {}


async def _persist_and_read_wallet(
    signal: FreshWalletSignal, db_manager: DatabaseManager, address: str
) -> WalletProfileDTO | None:
    profile = signal.wallet_profile
    dto = WalletProfileDTO(
        address=profile.address,
        nonce=profile.nonce,
        first_seen_at=profile.first_seen,
        is_fresh=profile.is_fresh,
        matic_balance=profile.matic_balance,
        usdc_balance=profile.usdc_balance,
        analyzed_at=profile.analyzed_at,
    )
    async with db_manager.get_async_session() as session:
        await WalletRepository(session).upsert(dto)
        await session.commit()
    async with db_manager.get_async_session() as session:
        return await WalletRepository(session).get_by_address(address)


def _surfaces(alert_surfaces: tuple[str, ...], needle: str) -> int:
    return sum(needle in surface for surface in alert_surfaces)


def _wallet_eval_parts(
    wallet_eval: WalletEvaluation | None,
) -> tuple[FreshWalletSignal | None, WalletProfile | None, str | None, int | None]:
    if wallet_eval is None:
        return None, None, None, None
    return (
        wallet_eval.signal,
        wallet_eval.profile,
        wallet_eval.classification.value,
        wallet_eval.boundary,
    )


def _profile_parts(profile: WalletProfile | None) -> tuple[int | None, bool | None, int | None]:
    if profile is None:
        return None, None, None
    return profile.nonce, profile.is_fresh, profile.fresh_threshold


async def _maybe_persist_wallet(
    signal: FreshWalletSignal | None, db_manager: DatabaseManager, address: str
) -> WalletProfileDTO | None:
    if signal is None:
        return None
    return await _persist_and_read_wallet(signal, db_manager, address)


def _persisted_parts(
    persisted: WalletProfileDTO | None,
) -> tuple[int | None, datetime | None, bool | None]:
    if persisted is None:
        return None, None, None
    return persisted.nonce, persisted.first_seen_at, persisted.is_fresh


def _signal_dict(signal: FreshWalletSignal | None) -> dict[str, object]:
    if signal is None:
        return {}
    return signal.to_dict()


def _document_boundary(document: Document004 | None) -> int | None:
    if document is None:
        return None
    return document.detector_config.fresh_transaction_boundary


async def _unknown_age_facts(db_manager: DatabaseManager) -> dict[str, object]:
    """One run of the unknown-age low-activity wallet (nonce 2, no first transaction)."""
    trade = create_trade_event(wallet_address=WALLET_ADDR, size=Decimal("3250"), price=Decimal("1"))
    client = WorkingPolygonClient(nonce=2, age_hours=None)
    analyzer = WalletAnalyzer(client, fresh_threshold=5)
    detector = FreshWalletDetector(analyzer, fresh_transaction_boundary=5)
    _, evidence, assessment, config = await _evaluate_and_score(trade, detector, EVALUATED_AT)
    signal, profile, classification, _ = _wallet_eval_parts(evidence.wallet)
    profile_nonce, _, _ = _profile_parts(profile)
    envelope_valid, document = _envelope_document(assessment, config)
    wallet = _evidence_member(document, "wallet")
    contributions = _evidence_member(document, "contributions")
    result = _evidence_member(document, "result")
    persisted = await _maybe_persist_wallet(signal, db_manager, trade.wallet_address)
    persisted_nonce, persisted_first_seen, persisted_is_fresh = _persisted_parts(persisted)
    signal_dict = _signal_dict(signal)
    alert = AlertFormatter(verbosity="detailed").format(assessment)
    surfaces = (alert.body, alert.plain_text, alert.telegram_markdown, str(alert.discord_embed))
    return {
        "wallet_signal_present": signal is not None,
        "envelope_valid": envelope_valid,
        "document_read": document is not None,
        "persisted_present": persisted is not None,
        "wallet_classification": classification,
        "wallet_profile_nonce": profile_nonce,
        "subject_kind": wallet.get("subject_kind"),
        "group_status": wallet.get("group_status"),
        "document_classification": wallet.get("classification"),
        "boundary": wallet.get("boundary"),
        "tx_count": wallet.get("tx_count"),
        "age_state": wallet.get("age_state"),
        "age_basis": wallet.get("age_basis"),
        "age_hours": wallet.get("age_hours"),
        "confidence": wallet.get("confidence"),
        "factors": wallet.get("factors"),
        "fresh_wallet_contribution": contributions.get("fresh_wallet"),
        "bonus": contributions.get("bonus"),
        "signals_triggered": result.get("signals_triggered"),
        "weighted_score": result.get("weighted_score"),
        "signal_wallet_is_fresh": signal_dict.get("wallet_is_fresh"),
        "signal_wallet_age_hours": signal_dict.get("wallet_age_hours"),
        "persisted_nonce": persisted_nonce,
        "persisted_first_seen": persisted_first_seen,
        "persisted_is_fresh": persisted_is_fresh,
        "surfaces_naming_fresh_wallet": _surfaces(surfaces, "Fresh Wallet"),
        "surfaces_with_age_label": _surfaces(surfaces, "(Age:"),
        "surfaces_with_48h": _surfaces(surfaces, "48"),
        "plain_text_mentions_fresh": "fresh" in alert.plain_text.lower(),
    }


UNKNOWN_AGE_EXPECTED: list[tuple[str, object]] = [
    ("wallet_signal_present", True),
    ("envelope_valid", True),
    ("document_read", True),
    ("persisted_present", True),
    ("wallet_classification", "low_activity_unknown_age"),
    ("wallet_profile_nonce", 2),
    ("subject_kind", "proxy_wallet"),
    ("group_status", "unqualified_pre_T027"),
    ("document_classification", "low_activity_unknown_age"),
    ("boundary", 5),
    ("tx_count", 2),
    ("age_state", "unknown"),
    ("age_basis", "none"),
    ("age_hours", None),
    ("confidence", None),
    ("factors", {}),
    ("fresh_wallet_contribution", "0"),
    ("bonus", "1"),
    ("signals_triggered", 1),
    ("weighted_score", "0.450"),
    ("signal_wallet_is_fresh", True),
    ("signal_wallet_age_hours", None),
    ("persisted_nonce", 2),
    ("persisted_first_seen", None),
    ("persisted_is_fresh", True),
    ("surfaces_naming_fresh_wallet", 0),
    ("surfaces_with_age_label", 0),
    ("surfaces_with_48h", 0),
    ("plain_text_mentions_fresh", False),
]


@pytest.fixture
async def unknown_age(db_manager: DatabaseManager) -> dict[str, object]:
    return await _unknown_age_facts(db_manager)


@pytest.mark.parametrize(
    ("fact", "expected"), UNKNOWN_AGE_EXPECTED, ids=[name for name, _ in UNKNOWN_AGE_EXPECTED]
)
async def test_unknown_age_low_activity_wallet_fact(
    unknown_age: dict[str, object], fact: str, expected: object
) -> None:
    # Arrange
    # Act
    actual = unknown_age[fact]

    # Assert
    assert actual == expected


async def _boundary_10_facts(
    db_manager: DatabaseManager, age_hours: float | None
) -> dict[str, object]:
    """One run of a nonce-9 wallet under a boundary-10 override over a default-5 analyzer."""
    trade = create_trade_event(wallet_address=WALLET_ADDR, size=Decimal("3250"), price=Decimal("1"))
    client = WorkingPolygonClient(nonce=9, age_hours=age_hours)
    analyzer = WalletAnalyzer(client, fresh_threshold=5)
    raw_profile = await analyzer.analyze(trade.wallet_address)
    detector = FreshWalletDetector(analyzer, fresh_transaction_boundary=10)
    _, evidence, assessment, config = await _evaluate_and_score(trade, detector, EVALUATED_AT)
    signal, profile, classification, boundary = _wallet_eval_parts(evidence.wallet)
    _, normalized_is_fresh, normalized_threshold = _profile_parts(profile)
    persisted = await _maybe_persist_wallet(signal, db_manager, trade.wallet_address)
    persisted_nonce, _, persisted_is_fresh = _persisted_parts(persisted)
    envelope_valid, document = _envelope_document(assessment, config)
    wallet = _evidence_member(document, "wallet")
    contributions = _evidence_member(document, "contributions")
    alert = AlertFormatter(verbosity="detailed").format(assessment)
    surfaces = (alert.plain_text, str(alert.discord_embed))
    return {
        "profile_present": profile is not None,
        "signal_present": signal is not None,
        "envelope_valid": envelope_valid,
        "document_read": document is not None,
        "persisted_present": persisted is not None,
        "raw_profile_is_fresh": raw_profile.is_fresh,
        "raw_profile_threshold": raw_profile.fresh_threshold,
        "classification": classification,
        "boundary": boundary,
        "normalized_is_fresh": normalized_is_fresh,
        "normalized_threshold": normalized_threshold,
        "signal_wallet_nonce": _signal_dict(signal).get("wallet_nonce"),
        "persisted_nonce": persisted_nonce,
        "persisted_is_fresh": persisted_is_fresh,
        "document_boundary": _document_boundary(document),
        "document_tx_count": wallet.get("tx_count"),
        "document_age_basis": wallet.get("age_basis"),
        "document_group_status": wallet.get("group_status"),
        "document_confidence": wallet.get("confidence"),
        "fresh_wallet_contribution": contributions.get("fresh_wallet"),
        "surfaces_naming_fresh_wallet": _surfaces(surfaces, "Fresh Wallet"),
    }


BOUNDARY_10_FRESH_EXPECTED: list[tuple[str, object]] = [
    ("profile_present", True),
    ("signal_present", True),
    ("envelope_valid", True),
    ("document_read", True),
    ("persisted_present", True),
    ("raw_profile_is_fresh", False),
    ("raw_profile_threshold", 5),
    ("classification", "fresh"),
    ("boundary", 10),
    ("normalized_is_fresh", True),
    ("normalized_threshold", 10),
    ("signal_wallet_nonce", 9),
    ("persisted_nonce", 9),
    ("persisted_is_fresh", True),
    ("document_boundary", 10),
    ("document_tx_count", 9),
    ("document_age_basis", "chain_first_transaction"),
    ("document_group_status", "unqualified_pre_T027"),
    ("document_confidence", None),
    ("fresh_wallet_contribution", "0"),
    ("surfaces_naming_fresh_wallet", 0),
]

BOUNDARY_10_UNKNOWN_AGE_EXPECTED: list[tuple[str, object]] = [
    ("profile_present", True),
    ("signal_present", True),
    ("envelope_valid", True),
    ("document_read", True),
    ("persisted_present", True),
    ("raw_profile_is_fresh", False),
    ("raw_profile_threshold", 5),
    ("classification", "low_activity_unknown_age"),
    ("boundary", 10),
    ("normalized_is_fresh", True),
    ("normalized_threshold", 10),
    ("signal_wallet_nonce", 9),
    ("persisted_nonce", 9),
    ("persisted_is_fresh", True),
    ("document_boundary", 10),
    ("document_tx_count", 9),
    ("document_age_basis", "none"),
    ("document_group_status", "unqualified_pre_T027"),
    ("document_confidence", None),
    ("fresh_wallet_contribution", "0"),
    ("surfaces_naming_fresh_wallet", 0),
]


@pytest.fixture
async def boundary_10_fresh(db_manager: DatabaseManager) -> dict[str, object]:
    return await _boundary_10_facts(db_manager, 24.0)


@pytest.fixture
async def boundary_10_unknown_age(db_manager: DatabaseManager) -> dict[str, object]:
    return await _boundary_10_facts(db_manager, None)


@pytest.mark.parametrize(
    ("fact", "expected"),
    BOUNDARY_10_FRESH_EXPECTED,
    ids=[name for name, _ in BOUNDARY_10_FRESH_EXPECTED],
)
async def test_boundary_10_override_fresh_wallet_fact(
    boundary_10_fresh: dict[str, object], fact: str, expected: object
) -> None:
    # Arrange
    # Act
    actual = boundary_10_fresh[fact]

    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    ("fact", "expected"),
    BOUNDARY_10_UNKNOWN_AGE_EXPECTED,
    ids=[name for name, _ in BOUNDARY_10_UNKNOWN_AGE_EXPECTED],
)
async def test_boundary_10_override_unknown_age_wallet_fact(
    boundary_10_unknown_age: dict[str, object], fact: str, expected: object
) -> None:
    # Arrange
    # Act
    actual = boundary_10_unknown_age[fact]

    # Assert
    assert actual == expected


async def _rule_observation(
    nonce: int, boundary: int, analyzer_threshold: int
) -> tuple[bool, int | None]:
    """The rule under test: whether the wallet signals, and the threshold it was normalised to."""
    trade = create_trade_event(wallet_address=WALLET_ADDR, size=Decimal("3250"), price=Decimal("1"))
    client = WorkingPolygonClient(nonce=nonce, age_hours=24.0)
    detector = FreshWalletDetector(
        WalletAnalyzer(client, fresh_threshold=analyzer_threshold),
        fresh_transaction_boundary=boundary,
    )
    evaluation = await detector.evaluate(trade, evaluated_at=EVALUATED_AT)
    profile = evaluation.profile
    return evaluation.signal is not None, (profile.fresh_threshold if profile is not None else None)


_RULE_INPUTS = {
    "nonce": st.integers(min_value=0, max_value=20),
    "boundary": st.integers(min_value=1, max_value=20),
    "analyzer_threshold": st.integers(min_value=1, max_value=20),
}


@settings(derandomize=True, max_examples=200, deadline=None)
@given(**_RULE_INPUTS)
def test_a_wallet_is_fresh_iff_its_nonce_is_below_the_exclusive_boundary(
    nonce: int, boundary: int, analyzer_threshold: int
) -> None:
    """Property over (nonce, B, analyzer threshold): fresh iff nonce < B, whatever the analyzer says."""
    # Arrange
    expected_signal = nonce < boundary

    # Act
    signalled, _ = asyncio.run(_rule_observation(nonce, boundary, analyzer_threshold))

    # Assert
    assert signalled == expected_signal


@settings(derandomize=True, max_examples=200, deadline=None)
@given(**_RULE_INPUTS)
def test_the_normalised_threshold_follows_the_detector_boundary_not_the_analyzer(
    nonce: int, boundary: int, analyzer_threshold: int
) -> None:
    """Property: the normalised threshold is the detector's B, never the analyzer's."""
    # Arrange
    expected_threshold = boundary

    # Act
    _, normalised = asyncio.run(_rule_observation(nonce, boundary, analyzer_threshold))

    # Assert
    assert normalised == expected_threshold


async def _negative_3_3_facts() -> dict[str, object]:
    trade = create_trade_event(wallet_address=WALLET_ADDR, size=Decimal("3250"), price=Decimal("1"))
    client = WorkingPolygonClient(nonce=3, age_hours=24.0)
    analyzer = WalletAnalyzer(client, fresh_threshold=5)
    raw_profile = await analyzer.analyze(trade.wallet_address)
    detector = FreshWalletDetector(analyzer, fresh_transaction_boundary=3)
    wallet_eval = await detector.evaluate(trade, evaluated_at=EVALUATED_AT)
    profile = wallet_eval.profile
    return {
        "profile_present": profile is not None,
        "raw_profile_is_fresh": raw_profile.is_fresh,
        "normalized_is_fresh": profile.is_fresh if profile is not None else None,
        "normalized_threshold": profile.fresh_threshold if profile is not None else None,
        "classification": wallet_eval.classification.value,
        "signal": wallet_eval.signal,
    }


NEGATIVE_3_3_EXPECTED: list[tuple[str, object]] = [
    ("profile_present", True),
    ("raw_profile_is_fresh", True),
    ("normalized_is_fresh", False),
    ("normalized_threshold", 3),
    ("classification", WalletClassification.KNOWN_NONQUALIFYING.value),
    ("signal", None),
]


@pytest.fixture
async def negative_3_3() -> dict[str, object]:
    return await _negative_3_3_facts()


@pytest.mark.parametrize(
    ("fact", "expected"), NEGATIVE_3_3_EXPECTED, ids=[name for name, _ in NEGATIVE_3_3_EXPECTED]
)
async def test_negative_control_transaction_override_3_3_fact(
    negative_3_3: dict[str, object], fact: str, expected: object
) -> None:
    # Arrange
    # Act
    actual = negative_3_3[fact]

    # Assert
    assert actual == expected
