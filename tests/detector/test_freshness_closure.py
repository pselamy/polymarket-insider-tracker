"""Pre-T027 joint regression tests for freshness-closure and boundary override (T009c).

Asserts unknown-age low-activity posture (FR-007c / FR-008 / SC-003 / SC-006) and
boundary-10 / nonce-9 override mismatch resolution (data-model.md §2a).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from scripts.spec004_evidence.check_envelope import reconstruct, schema_for, validate
from scripts.spec004_evidence.replay_envelopes import PRE_T027

from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.detector.detector_config import capture_detector_config
from polymarket_insider_tracker.detector.evaluation import (
    TradeEvidence,
    WalletClassification,
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
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer
from polymarket_insider_tracker.profiler.models import Transaction, WalletInfo
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.repos import WalletProfileDTO, WalletRepository
from tests.detector.test_fresh_wallet import create_trade_event
from tests.fakes.metadata import FakeMetadataSync
from tests.integration._journey_harness import stamped_volume_market

EVALUATED_AT = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
WALLET_ADDR = "0x1234567890123456789012345678901234567890"


class WorkingPolygonClient:
    """Working structural fake Polygon client returning configurable WalletInfo."""

    def __init__(self, nonce: int, age_hours: float | None, evaluated_at: datetime) -> None:
        self.nonce = nonce
        self.age_hours = age_hours
        self.evaluated_at = evaluated_at

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


async def _evaluate_and_score(
    trade: TradeEvent,
    wallet_detector: FreshWalletDetector,
    evaluated_at: datetime,
) -> tuple[FreshWalletDetector, TradeEvidence, RiskAssessment, DetectorConfig]:
    wallet_eval = await wallet_detector.evaluate(trade, evaluated_at=evaluated_at)
    metadata = FakeMetadataSync()
    metadata.fallback = stamped_volume_market(trade)
    size_detector = SizeAnomalyDetector(metadata)
    size_eval = await size_detector.evaluate(trade, evaluated_at=evaluated_at)
    config = capture_detector_config(wallet_detector, size_detector)
    evidence = TradeEvidence(evaluated_at, config, wallet_eval, size_eval)
    bundle = SignalBundle(trade, wallet_eval.signal, size_eval.signal, evidence)
    assessment = await RiskScorer(FakeAsyncRedis()).assess(bundle)
    return wallet_detector, evidence, assessment, config


def _validate_004_1_envelope(assessment: RiskAssessment, config: DetectorConfig) -> Document004:
    text = row_scoring_config(assessment, config)
    assert text is not None
    raw: object = json.loads(text)
    schema = schema_for(PRE_T027)
    assert validate(raw, schema, schema) + reconstruct(raw) == []
    doc = read_scoring_config(VERSION_004_1, text)
    assert isinstance(doc, Document004)
    return doc


def _evidence_member(document: Document004, name: str) -> dict[str, JsonValue]:
    assert document.evaluation_evidence is not None
    val = document.evaluation_evidence.get(name)
    assert isinstance(val, dict)
    return val


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


@pytest.mark.asyncio
async def test_unknown_age_low_activity_joint_regression_pre_t027(
    db_manager: DatabaseManager,
) -> None:
    """Pre-T027 unknown-age low-activity posture (FR-007c / FR-008 / SC-003 / SC-006)."""
    trade = create_trade_event(wallet_address=WALLET_ADDR, size=Decimal("3250"), price=Decimal("1"))
    evaluated_at = datetime.now(UTC)
    client = WorkingPolygonClient(nonce=2, age_hours=None, evaluated_at=evaluated_at)
    analyzer = WalletAnalyzer(client, fresh_threshold=5)
    detector = FreshWalletDetector(analyzer, fresh_transaction_boundary=5)

    _, evidence, assessment, config = await _evaluate_and_score(trade, detector, evaluated_at)
    wallet_eval = evidence.wallet
    assert wallet_eval is not None
    assert wallet_eval.classification == WalletClassification.LOW_ACTIVITY_UNKNOWN_AGE
    assert wallet_eval.profile is not None and wallet_eval.profile.nonce == 2

    document = _validate_004_1_envelope(assessment, config)
    w_dict = _evidence_member(document, "wallet")
    assert w_dict["subject_kind"] == "proxy_wallet"
    assert w_dict["group_status"] == "unqualified_pre_T027"
    assert w_dict["classification"] == "low_activity_unknown_age"
    assert w_dict["boundary"] == 5
    assert w_dict["tx_count"] == 2
    assert w_dict["age_state"] == "unknown"
    assert w_dict["age_basis"] == "none"
    assert w_dict["age_hours"] is None
    assert w_dict["confidence"] is None
    assert w_dict["factors"] == {}

    contrib = _evidence_member(document, "contributions")
    assert contrib["fresh_wallet"] == "0"
    assert contrib["bonus"] == "1"
    result = _evidence_member(document, "result")
    assert result["signals_triggered"] == 1
    assert result["weighted_score"] == "0.450"

    assert wallet_eval.signal is not None
    sig_dict = wallet_eval.signal.to_dict()
    assert sig_dict["wallet_is_fresh"] is True
    assert sig_dict["wallet_age_hours"] is None

    persisted = await _persist_and_read_wallet(wallet_eval.signal, db_manager, trade.wallet_address)
    assert persisted is not None
    assert persisted.nonce == 2
    assert persisted.first_seen_at is None
    assert persisted.is_fresh is True

    formatter = AlertFormatter(verbosity="detailed")
    alert = formatter.format(assessment)
    for surface in (
        alert.body,
        alert.plain_text,
        alert.telegram_markdown,
        str(alert.discord_embed),
    ):
        assert "Fresh Wallet" not in surface
        assert "(Age:" not in surface
        assert "48" not in surface
    assert "fresh" not in alert.plain_text.lower()


@pytest.mark.parametrize(
    ("age_hours", "expected_class", "expected_age_basis"),
    [
        (24.0, WalletClassification.FRESH, "chain_first_transaction"),
        (None, WalletClassification.LOW_ACTIVITY_UNKNOWN_AGE, "none"),
    ],
)
@pytest.mark.asyncio
async def test_boundary_10_nonce_9_override_mismatch_joint_regression(
    db_manager: DatabaseManager,
    age_hours: float | None,
    expected_class: WalletClassification,
    expected_age_basis: str,
) -> None:
    """T009c: boundary-10 / nonce-9 override normalizes raw default-5 profile (data-model §2a)."""
    trade = create_trade_event(wallet_address=WALLET_ADDR, size=Decimal("3250"), price=Decimal("1"))
    client = WorkingPolygonClient(nonce=9, age_hours=age_hours, evaluated_at=EVALUATED_AT)
    analyzer = WalletAnalyzer(client, fresh_threshold=5)

    raw_profile = await analyzer.analyze(trade.wallet_address)
    assert raw_profile.is_fresh is False
    assert raw_profile.fresh_threshold == 5

    detector = FreshWalletDetector(analyzer, fresh_transaction_boundary=10)
    _, evidence, assessment, config = await _evaluate_and_score(trade, detector, EVALUATED_AT)
    wallet_eval = evidence.wallet
    assert wallet_eval is not None
    assert wallet_eval.classification == expected_class
    assert wallet_eval.boundary == 10
    assert wallet_eval.profile is not None
    assert wallet_eval.profile.is_fresh is True
    assert wallet_eval.profile.fresh_threshold == 10

    assert wallet_eval.signal is not None
    sig_dict = wallet_eval.signal.to_dict()
    assert sig_dict["wallet_is_fresh"] is True
    assert sig_dict["wallet_nonce"] == 9

    persisted = await _persist_and_read_wallet(wallet_eval.signal, db_manager, trade.wallet_address)
    assert persisted is not None
    assert persisted.nonce == 9
    assert persisted.is_fresh is True

    document = _validate_004_1_envelope(assessment, config)
    assert document.detector_config.fresh_transaction_boundary == 10
    w_dict = _evidence_member(document, "wallet")
    assert w_dict["boundary"] == 10
    assert w_dict["tx_count"] == 9
    assert w_dict["classification"] == expected_class.value
    assert w_dict["age_basis"] == expected_age_basis
    assert w_dict["group_status"] == "unqualified_pre_T027"
    assert w_dict["confidence"] is None
    assert w_dict["factors"] == {}
    contrib = _evidence_member(document, "contributions")
    assert contrib["fresh_wallet"] == "0"
    assert contrib["bonus"] == "1"

    alert = AlertFormatter(verbosity="detailed").format(assessment)
    if age_hours == 24.0:
        assert "(Age: 24h)" in alert.plain_text
    else:
        assert "(Age:" not in alert.plain_text
    assert "Fresh Wallet" not in alert.plain_text
    assert "Fresh Wallet" not in str(alert.discord_embed)


@pytest.mark.asyncio
async def test_negative_control_transaction_override_3_3() -> None:
    """T009c negative control: boundary-3 normalizes raw default-5 profile to nonqualifying."""
    trade = create_trade_event(wallet_address=WALLET_ADDR, size=Decimal("3250"), price=Decimal("1"))
    client = WorkingPolygonClient(nonce=3, age_hours=24.0, evaluated_at=EVALUATED_AT)
    analyzer = WalletAnalyzer(client, fresh_threshold=5)

    raw_profile = await analyzer.analyze(trade.wallet_address)
    assert raw_profile.is_fresh is True

    detector = FreshWalletDetector(analyzer, fresh_transaction_boundary=3)
    wallet_eval = await detector.evaluate(trade, evaluated_at=EVALUATED_AT)

    assert wallet_eval.profile is not None
    assert wallet_eval.profile.is_fresh is False
    assert wallet_eval.profile.fresh_threshold == 3
    assert wallet_eval.classification == WalletClassification.KNOWN_NONQUALIFYING
    assert wallet_eval.signal is None
