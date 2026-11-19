"""SC-005 display: every rendered alert shows the exact effective threshold of its assessment.

The expected text is derived from the threshold itself (an integer count of thousandths), never
from the formatter, so the oracle is independent of the implementation under test.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from hypothesis import given
from hypothesis import strategies as st

from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.detector.scorer import RiskScorer
from tests.detector.test_fresh_wallet import create_trade_event

CHANNELS = ("compact", "detailed", "plain", "discord", "telegram")


def _expected_text(thousandths: int) -> str:
    """The threshold as three decimals, from its integer count of thousandths."""
    return f"{thousandths // 1000}.{thousandths % 1000:03d}"


def _assessment(threshold: float) -> RiskAssessment:
    trade = create_trade_event(size=Decimal("5000"), price=Decimal("0.65"))
    scorer = RiskScorer(FakeAsyncRedis(), alert_threshold=threshold)
    return RiskAssessment(
        trade_event=trade,
        wallet_address=trade.wallet_address,
        market_id=trade.market_id,
        weighted_score=0.45,
        signals_triggered=1,
        should_alert=True,
        scoring_config=scorer.scoring_config,
    )


def _rendered(channel: str, assessment: RiskAssessment) -> str:
    """The text one channel shows for the assessment."""
    alert: FormattedAlert = AlertFormatter(
        verbosity="compact" if channel == "compact" else "detailed"
    ).format(assessment)
    if channel in ("compact", "detailed"):
        return alert.body
    if channel == "plain":
        return alert.plain_text
    if channel == "discord":
        return str(alert.discord_embed)
    return alert.telegram_markdown


def _expected_in_channel(channel: str, threshold_text: str) -> str:
    """Labeled threshold fragment; Telegram MarkdownV2 escapes decimal points."""
    escaped = threshold_text.replace(".", "\\.") if channel == "telegram" else threshold_text
    return f"; alert threshold {escaped}"


@pytest.mark.parametrize("channel", CHANNELS)
@given(thousandths=st.integers(min_value=0, max_value=1000))
def test_every_valid_threshold_is_shown_exactly(channel: str, thousandths: int) -> None:
    # Arrange
    threshold = thousandths / 1000
    assessment = _assessment(threshold)
    expected = _expected_in_channel(channel, _expected_text(thousandths))

    # Act
    occurrences = _rendered(channel, assessment).count(expected)

    # Assert
    assert occurrences == 1


@pytest.mark.parametrize("channel", CHANNELS)
@pytest.mark.parametrize(
    ("threshold", "thousandths"), [(0.8, 800), (0.4, 400)], ids=["default_0.800", "override_0.400"]
)
def test_the_default_and_override_thresholds_are_shown(
    channel: str, threshold: float, thousandths: int
) -> None:
    # Arrange
    assessment = _assessment(threshold)
    expected = _expected_in_channel(channel, _expected_text(thousandths))

    # Act
    occurrences = _rendered(channel, assessment).count(expected)

    # Assert
    assert occurrences == 1


@pytest.mark.parametrize("channel", CHANNELS)
def test_a_corrupt_recorded_config_shows_no_numeric_threshold(channel: str) -> None:
    # Arrange
    corrupt = replace(_assessment(0.8), scoring_config="not a scoring configuration")

    # Act
    occurrences = _rendered(channel, corrupt).count("; alert threshold unavailable")

    # Assert
    assert occurrences == 1


@pytest.mark.parametrize("channel", CHANNELS)
def test_an_assessment_without_a_recorded_config_shows_no_numeric_threshold(channel: str) -> None:
    # Arrange
    assessment = _assessment(0.8)
    unrecorded = RiskAssessment(
        trade_event=assessment.trade_event,
        wallet_address=assessment.wallet_address,
        market_id=assessment.market_id,
        weighted_score=0.45,
        scoring_config=None,
    )

    # Act
    occurrences = _rendered(channel, unrecorded).count("; alert threshold unavailable")

    # Assert
    assert occurrences == 1
