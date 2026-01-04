"""Tests for alert message formatter."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_insider_tracker.alerter.formatter import (
    COLOR_HIGH_RISK,
    COLOR_LOW_RISK,
    COLOR_MEDIUM_RISK,
    AlertFormatter,
    format_usdc,
    get_risk_color,
    get_risk_level,
    get_triggered_signals,
    truncate_address,
)
from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.detector.models import (
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_trade() -> TradeEvent:
    """Create a sample trade event."""
    return TradeEvent(
        market_id="market_abc123",
        trade_id="tx_001",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.075"),
        size=Decimal("200000"),
        timestamp=datetime.now(UTC),
        asset_id="token_123",
        market_slug="will-x-happen",
        event_title="Will X happen by Y?",
    )


@pytest.fixture
def sample_wallet_profile() -> WalletProfile:
    """Create a sample wallet profile."""
    return WalletProfile(
        address="0x1234567890abcdef1234567890abcdef12345678",
        nonce=2,
        first_seen=datetime.now(UTC),
        age_hours=2.0,
        is_fresh=True,
        total_tx_count=2,
        matic_balance=Decimal("1000000000000000000"),
        usdc_balance=Decimal("1000000"),
    )


@pytest.fixture
def sample_metadata() -> MarketMetadata:
    """Create sample market metadata."""
    return MarketMetadata(
        condition_id="market_abc123",
        question="Will X happen by Y?",
        description="Test market description",
        tokens=(Token(token_id="token_123", outcome="Yes", price=Decimal("0.075")),),
        category="other",
    )


@pytest.fixture
def fresh_wallet_signal(
    sample_trade: TradeEvent, sample_wallet_profile: WalletProfile
) -> FreshWalletSignal:
    """Create a sample fresh wallet signal."""
    return FreshWalletSignal(
        trade_event=sample_trade,
        wallet_profile=sample_wallet_profile,
        confidence=0.8,
        factors={"base": 0.5, "brand_new_bonus": 0.2, "large_trade_bonus": 0.1},
    )


@pytest.fixture
def size_anomaly_signal(
    sample_trade: TradeEvent, sample_metadata: MarketMetadata
) -> SizeAnomalySignal:
    """Create a sample size anomaly signal."""
    return SizeAnomalySignal(
        trade_event=sample_trade,
        market_metadata=sample_metadata,
        volume_impact=0.10,
        book_impact=0.15,
        is_niche_market=True,
        confidence=0.7,
        factors={"volume_impact": 0.4, "book_impact": 0.3},
    )


@pytest.fixture
def high_risk_assessment(
    sample_trade: TradeEvent,
    fresh_wallet_signal: FreshWalletSignal,
    size_anomaly_signal: SizeAnomalySignal,
) -> RiskAssessment:
    """Create a high-risk assessment with multiple signals."""
    return RiskAssessment(
        trade_event=sample_trade,
        wallet_address=sample_trade.wallet_address,
        market_id=sample_trade.market_id,
        fresh_wallet_signal=fresh_wallet_signal,
        size_anomaly_signal=size_anomaly_signal,
        signals_triggered=2,
        weighted_score=0.82,
        should_alert=True,
    )


@pytest.fixture
def medium_risk_assessment(
    sample_trade: TradeEvent,
    fresh_wallet_signal: FreshWalletSignal,
) -> RiskAssessment:
    """Create a medium-risk assessment with one signal."""
    return RiskAssessment(
        trade_event=sample_trade,
        wallet_address=sample_trade.wallet_address,
        market_id=sample_trade.market_id,
        fresh_wallet_signal=fresh_wallet_signal,
        size_anomaly_signal=None,
        signals_triggered=1,
        weighted_score=0.55,
        should_alert=True,
    )


@pytest.fixture
def low_risk_assessment(sample_trade: TradeEvent) -> RiskAssessment:
    """Create a low-risk assessment with no signals."""
    return RiskAssessment(
        trade_event=sample_trade,
        wallet_address=sample_trade.wallet_address,
        market_id=sample_trade.market_id,
        fresh_wallet_signal=None,
        size_anomaly_signal=None,
        signals_triggered=0,
        weighted_score=0.25,
        should_alert=False,
    )


# ============================================================================
# Helper Function Tests
# ============================================================================


class TestTruncateAddress:
    """Tests for truncate_address helper."""

    def test_truncate_standard_address(self) -> None:
        """Test truncating a standard Ethereum address."""
        address = "0x1234567890abcdef1234567890abcdef12345678"
        result = truncate_address(address)
        assert result == "0x1234...5678"

    def test_truncate_with_custom_length(self) -> None:
        """Test truncating with custom character count."""
        address = "0x1234567890abcdef1234567890abcdef12345678"
        result = truncate_address(address, chars=6)
        assert result == "0x123456...345678"

    def test_short_address_not_truncated(self) -> None:
        """Test that short addresses are not truncated."""
        address = "0x1234"
        result = truncate_address(address)
        assert result == "0x1234"


class TestFormatUsdc:
    """Tests for format_usdc helper."""

    def test_format_whole_dollars(self) -> None:
        """Test formatting whole dollar amounts."""
        result = format_usdc(Decimal("15000"))
        assert result == "$15,000.00"

    def test_format_with_cents(self) -> None:
        """Test formatting with decimal places."""
        result = format_usdc(Decimal("1234.56"))
        assert result == "$1,234.56"

    def test_format_large_amount(self) -> None:
        """Test formatting large amounts."""
        result = format_usdc(Decimal("1000000"))
        assert result == "$1,000,000.00"


class TestGetRiskLevel:
    """Tests for get_risk_level helper."""

    def test_high_risk(self) -> None:
        """Test high risk threshold."""
        assert get_risk_level(0.85) == "HIGH"
        assert get_risk_level(0.70) == "HIGH"

    def test_medium_risk(self) -> None:
        """Test medium risk threshold."""
        assert get_risk_level(0.65) == "MEDIUM"
        assert get_risk_level(0.50) == "MEDIUM"

    def test_low_risk(self) -> None:
        """Test low risk threshold."""
        assert get_risk_level(0.40) == "LOW"
        assert get_risk_level(0.10) == "LOW"


class TestGetRiskColor:
    """Tests for get_risk_color helper."""

    def test_high_risk_color(self) -> None:
        """Test high risk returns red color."""
        assert get_risk_color(0.85) == COLOR_HIGH_RISK
        assert get_risk_color(0.70) == COLOR_HIGH_RISK

    def test_medium_risk_color(self) -> None:
        """Test medium risk returns orange color."""
        assert get_risk_color(0.65) == COLOR_MEDIUM_RISK
        assert get_risk_color(0.50) == COLOR_MEDIUM_RISK

    def test_low_risk_color(self) -> None:
        """Test low risk returns yellow color."""
        assert get_risk_color(0.40) == COLOR_LOW_RISK


class TestGetTriggeredSignals:
    """Tests for get_triggered_signals helper."""

    def test_no_signals(self, low_risk_assessment: RiskAssessment) -> None:
        """Test assessment with no signals."""
        signals = get_triggered_signals(low_risk_assessment)
        assert signals == []

    def test_fresh_wallet_only(self, medium_risk_assessment: RiskAssessment) -> None:
        """Test assessment with only fresh wallet signal."""
        signals = get_triggered_signals(medium_risk_assessment)
        assert "Fresh Wallet" in signals
        assert "Large Position" not in signals

    def test_both_signals(self, high_risk_assessment: RiskAssessment) -> None:
        """Test assessment with both signals."""
        signals = get_triggered_signals(high_risk_assessment)
        assert "Fresh Wallet" in signals
        assert "Large Position" in signals
        assert "Niche Market" in signals  # From size anomaly with is_niche_market=True


# ============================================================================
# AlertFormatter Tests
# ============================================================================


class TestAlertFormatterInit:
    """Tests for AlertFormatter initialization."""

    def test_default_verbosity(self) -> None:
        """Test default verbosity is detailed."""
        formatter = AlertFormatter()
        assert formatter.verbosity == "detailed"

    def test_compact_verbosity(self) -> None:
        """Test setting compact verbosity."""
        formatter = AlertFormatter(verbosity="compact")
        assert formatter.verbosity == "compact"


class TestAlertFormatterFormat:
    """Tests for AlertFormatter.format method."""

    def test_format_returns_formatted_alert(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that format returns a FormattedAlert."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert isinstance(result, FormattedAlert)

    def test_format_includes_all_fields(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that all fields are populated."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)

        assert result.title != ""
        assert result.body != ""
        assert result.discord_embed != {}
        assert result.telegram_markdown != ""
        assert result.plain_text != ""
        assert result.links != {}

    def test_format_title_includes_risk_level(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that title includes risk level."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "HIGH" in result.title

    def test_format_includes_wallet_link(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that wallet explorer link is included."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "wallet" in result.links
        assert "polygonscan.com" in result.links["wallet"]

    def test_format_includes_market_link(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that market link is included when slug available."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "market" in result.links
        assert "polymarket.com" in result.links["market"]


class TestDiscordEmbed:
    """Tests for Discord embed format."""

    def test_embed_has_required_fields(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that embed has required Discord fields."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        embed = result.discord_embed

        assert "title" in embed
        assert "color" in embed
        assert "fields" in embed
        assert "footer" in embed

    def test_embed_color_reflects_risk(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that embed color matches risk level."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert result.discord_embed["color"] == COLOR_HIGH_RISK

    def test_embed_includes_wallet_field(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that embed includes wallet field."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        fields = result.discord_embed["fields"]

        wallet_field = next((f for f in fields if f["name"] == "Wallet"), None)
        assert wallet_field is not None
        assert "0x1234" in wallet_field["value"]

    def test_embed_includes_wallet_age(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that wallet age is shown when fresh wallet signal present."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        fields = result.discord_embed["fields"]

        wallet_field = next((f for f in fields if f["name"] == "Wallet"), None)
        assert "Age:" in wallet_field["value"]

    def test_embed_includes_trade_details(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that trade details are in embed."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        fields = result.discord_embed["fields"]

        trade_field = next((f for f in fields if f["name"] == "Trade"), None)
        assert trade_field is not None
        assert "BUY" in trade_field["value"]
        assert "Yes" in trade_field["value"]

    def test_embed_includes_signals_field(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that signals are listed in embed."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        fields = result.discord_embed["fields"]

        signals_field = next((f for f in fields if f["name"] == "Signals"), None)
        assert signals_field is not None
        assert "Fresh Wallet" in signals_field["value"]

    def test_detailed_embed_includes_confidence(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that detailed mode includes confidence breakdown."""
        formatter = AlertFormatter(verbosity="detailed")
        result = formatter.format(high_risk_assessment)
        fields = result.discord_embed["fields"]

        conf_field = next((f for f in fields if f["name"] == "Confidence"), None)
        assert conf_field is not None


class TestTelegramMarkdown:
    """Tests for Telegram markdown format."""

    def test_telegram_includes_header(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that Telegram message has header."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "*Suspicious Activity Detected*" in result.telegram_markdown

    def test_telegram_includes_wallet(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that Telegram message includes wallet."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "`0x1234...5678`" in result.telegram_markdown

    def test_telegram_includes_risk_score(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that Telegram message includes risk score."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "0.82" in result.telegram_markdown
        assert "HIGH" in result.telegram_markdown

    def test_telegram_includes_links(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that Telegram message includes links."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "[View Wallet]" in result.telegram_markdown
        assert "[View Market]" in result.telegram_markdown


class TestPlainText:
    """Tests for plain text format."""

    def test_plain_text_header(self, high_risk_assessment: RiskAssessment) -> None:
        """Test plain text has header."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "SUSPICIOUS ACTIVITY DETECTED" in result.plain_text

    def test_plain_text_wallet(self, high_risk_assessment: RiskAssessment) -> None:
        """Test plain text includes wallet."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "Wallet:" in result.plain_text
        assert "0x1234...5678" in result.plain_text

    def test_plain_text_trade(self, high_risk_assessment: RiskAssessment) -> None:
        """Test plain text includes trade details."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "Trade:" in result.plain_text
        assert "BUY" in result.plain_text

    def test_plain_text_signals(self, high_risk_assessment: RiskAssessment) -> None:
        """Test plain text includes signals."""
        formatter = AlertFormatter()
        result = formatter.format(high_risk_assessment)
        assert "Signals:" in result.plain_text
        assert "Fresh Wallet" in result.plain_text


class TestCompactVerbosity:
    """Tests for compact verbosity mode."""

    def test_compact_body_is_shorter(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that compact mode produces shorter body."""
        detailed_formatter = AlertFormatter(verbosity="detailed")
        compact_formatter = AlertFormatter(verbosity="compact")

        detailed_result = detailed_formatter.format(high_risk_assessment)
        compact_result = compact_formatter.format(high_risk_assessment)

        assert len(compact_result.body) < len(detailed_result.body)

    def test_compact_body_includes_essential_info(
        self, high_risk_assessment: RiskAssessment
    ) -> None:
        """Test that compact mode still has essential info."""
        formatter = AlertFormatter(verbosity="compact")
        result = formatter.format(high_risk_assessment)

        assert "0x1234...5678" in result.body
        assert "0.82" in result.body
        assert "HIGH" in result.body


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_no_market_slug(self, sample_trade: TradeEvent) -> None:
        """Test formatting when market slug is empty."""
        trade = TradeEvent(
            market_id=sample_trade.market_id,
            trade_id=sample_trade.trade_id,
            wallet_address=sample_trade.wallet_address,
            side=sample_trade.side,
            outcome=sample_trade.outcome,
            outcome_index=sample_trade.outcome_index,
            price=sample_trade.price,
            size=sample_trade.size,
            timestamp=sample_trade.timestamp,
            asset_id=sample_trade.asset_id,
            market_slug="",  # Empty slug
            event_title="",  # Empty title
        )
        assessment = RiskAssessment(
            trade_event=trade,
            wallet_address=trade.wallet_address,
            market_id=trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=0,
            weighted_score=0.5,
            should_alert=False,
        )

        formatter = AlertFormatter()
        result = formatter.format(assessment)

        # Should not have market link
        assert "market" not in result.links
        # Should have fallback text
        assert "Unknown Market" in result.plain_text

    def test_very_short_wallet_age(
        self,
        sample_trade: TradeEvent,
        sample_wallet_profile: WalletProfile,
    ) -> None:
        """Test formatting with wallet age less than 1 hour."""
        profile = WalletProfile(
            address=sample_wallet_profile.address,
            nonce=sample_wallet_profile.nonce,
            first_seen=datetime.now(UTC),
            age_hours=0.5,  # 30 minutes
            is_fresh=True,
            total_tx_count=1,
            matic_balance=sample_wallet_profile.matic_balance,
            usdc_balance=sample_wallet_profile.usdc_balance,
        )
        signal = FreshWalletSignal(
            trade_event=sample_trade,
            wallet_profile=profile,
            confidence=0.9,
            factors={},
        )
        assessment = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=signal,
            size_anomaly_signal=None,
            signals_triggered=1,
            weighted_score=0.75,
            should_alert=True,
        )

        formatter = AlertFormatter()
        result = formatter.format(assessment)

        # Should show age in minutes
        assert "30m" in result.plain_text or "Age: 30m" in result.plain_text

    def test_size_anomaly_without_niche(
        self,
        sample_trade: TradeEvent,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test size anomaly signal without niche market flag."""
        signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.10,
            book_impact=0.15,
            is_niche_market=False,  # Not niche
            confidence=0.7,
            factors={},
        )
        assessment = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=signal,
            signals_triggered=1,
            weighted_score=0.6,
            should_alert=True,
        )

        signals = get_triggered_signals(assessment)
        assert "Large Position" in signals
        assert "Niche Market" not in signals
