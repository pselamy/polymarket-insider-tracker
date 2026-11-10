"""The README sample alert is the real AlertFormatter output for a fixture assessment.

The block between the sample-alert markers in README.md must equal the plain-text rendering of
the fixture below, so the documentation cannot drift from what the formatter emits.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.detector.models import RiskAssessment, SizeAnomalySignal
from polymarket_insider_tracker.detector.volume_state import (
    SizeVolumeEvidence,
    VolumeState,
    VolumeVerdict,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_BLOCK = re.compile(
    r"<!-- sample-alert:start -->\n```text\n(.*?)```\n<!-- sample-alert:end -->",
    re.DOTALL,
)


def _fixture_assessment() -> RiskAssessment:
    trade = TradeEvent(
        market_id="market_abc123",
        trade_id="tx_001",
        wallet_address="0x7a3c9e2f0d4b8a61c5e7f3b2d9a0c4e8f1a2f91d",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.65"),
        size=Decimal("5000"),
        timestamp=datetime(2026, 10, 20, 12, 0, tzinfo=UTC),
        asset_id="token_123",
        event_title="Will X announce Y by March 2026?",
    )
    metadata = MarketMetadata(
        condition_id="market_abc123",
        question="Will X announce Y by March 2026?",
        description="",
        tokens=(),
        category="science",
    )
    verdict = VolumeVerdict(
        state=VolumeState.AVAILABLE,
        reason="gamma",
        daily_volume=Decimal("10000"),
        present=True,
        raw=None,
        source=None,
        acquired_at=None,
        age_seconds=None,
    )
    evidence = SizeVolumeEvidence(
        verdict=verdict,
        volume_impact_measured=Decimal("0.3250"),
        niche_basis="numeric",
        category="science",
        category_source="metadata",
    )
    signal = SizeAnomalySignal(
        trade_event=trade,
        market_metadata=metadata,
        volume_impact=0.325,
        book_impact=0.0,
        is_niche_market=True,
        confidence=0.75,
        factors={},
        volume_evidence=evidence,
        volume_impact_measured=Decimal("0.3250"),
    )
    return RiskAssessment(
        trade_event=trade,
        wallet_address=trade.wallet_address,
        market_id=trade.market_id,
        fresh_wallet_signal=None,
        size_anomaly_signal=signal,
        signals_triggered=1,
        weighted_score=0.45,
        should_alert=True,
    )


def _rendered_fixture() -> str:
    return AlertFormatter(verbosity="detailed").format(_fixture_assessment()).plain_text


def _readme_sample() -> str:
    text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    return SAMPLE_BLOCK.search(text).group(1)


def test_readme_sample_alert_is_the_formatter_output() -> None:
    # Arrange
    expected = _rendered_fixture()

    # Act
    sample = _readme_sample()

    # Assert
    assert sample == expected
