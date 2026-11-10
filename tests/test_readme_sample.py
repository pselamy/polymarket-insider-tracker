"""The README sample alert is the real AlertFormatter output for a fixture assessment.

The block between the sample-alert markers in README.md must equal the plain-text rendering of
the fixture below, so the documentation cannot drift from what the formatter emits.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from markdown_it import MarkdownIt

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
    r"<!-- sample-alert:start -->\n```text\n(.*?)\n```\n<!-- sample-alert:end -->",
    re.DOTALL,
)
SAMPLE_END_MARKER = "<!-- sample-alert:end -->"
ARCHITECTURE_HEADING = "Architecture"


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
    match = SAMPLE_BLOCK.search(text)
    return "" if match is None else match.group(1)


def _readme_text() -> str:
    return (REPO_ROOT / "README.md").read_text(encoding="utf-8")


def _fence_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.lstrip().startswith("```")]


def _heading_texts(text: str) -> list[str]:
    tokens = MarkdownIt("commonmark").parse(text)
    return [
        tokens[index + 1].content
        for index, token in enumerate(tokens)
        if token.type == "heading_open"
    ]


def test_readme_sample_alert_is_the_formatter_output() -> None:
    # Arrange
    expected = _rendered_fixture()

    # Act
    sample = _readme_sample()

    # Assert
    assert sample == expected


def test_the_sample_closing_fence_is_on_its_own_line() -> None:
    # Arrange
    lines = _readme_text().splitlines()

    # Act
    end_index = lines.index(SAMPLE_END_MARKER)

    # Assert
    assert lines[end_index - 1] == "```"


def test_the_readme_has_an_even_number_of_fence_lines() -> None:
    # Arrange
    fences = _fence_lines(_readme_text())

    # Act
    count = len(fences)

    # Assert
    assert count % 2 == 0


def test_the_architecture_heading_is_parsed_as_a_heading_outside_any_code_block() -> None:
    # Arrange
    text = _readme_text()

    # Act
    headings = _heading_texts(text)

    # Assert
    assert ARCHITECTURE_HEADING in headings
