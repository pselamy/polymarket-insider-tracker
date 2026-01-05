"""Alert message formatter for multi-channel delivery.

This module transforms RiskAssessment objects into human-readable,
actionable alert messages optimized for Discord, Telegram, and plain text.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.detector.models import RiskAssessment

# Polymarket URLs
POLYMARKET_MARKET_URL = "https://polymarket.com/event/{slug}"
POLYGONSCAN_ADDRESS_URL = "https://polygonscan.com/address/{address}"

# Discord embed colors (decimal values)
COLOR_HIGH_RISK = 15158332  # Red (#E74C3C)
COLOR_MEDIUM_RISK = 15105570  # Orange (#E67E22)
COLOR_LOW_RISK = 16776960  # Yellow (#FFFF00)

# Risk level thresholds
HIGH_RISK_THRESHOLD = 0.7
MEDIUM_RISK_THRESHOLD = 0.5


def truncate_address(address: str, chars: int = 4) -> str:
    """Truncate an Ethereum address to 0x1234...5678 format."""
    if len(address) < chars * 2 + 4:
        return address
    return f"{address[: chars + 2]}...{address[-chars:]}"


def format_usdc(amount: Decimal) -> str:
    """Format a USDC amount with commas and 2 decimal places."""
    return f"${amount:,.2f}"


def get_risk_level(score: float) -> str:
    """Get human-readable risk level from score."""
    if score >= HIGH_RISK_THRESHOLD:
        return "HIGH"
    if score >= MEDIUM_RISK_THRESHOLD:
        return "MEDIUM"
    return "LOW"


def get_risk_color(score: float) -> int:
    """Get Discord embed color based on risk score."""
    if score >= HIGH_RISK_THRESHOLD:
        return COLOR_HIGH_RISK
    if score >= MEDIUM_RISK_THRESHOLD:
        return COLOR_MEDIUM_RISK
    return COLOR_LOW_RISK


def get_triggered_signals(assessment: RiskAssessment) -> list[str]:
    """Get list of triggered signal names."""
    signals = []
    if assessment.fresh_wallet_signal:
        signals.append("Fresh Wallet")
    if assessment.size_anomaly_signal:
        signals.append("Large Position")
        if assessment.size_anomaly_signal.is_niche_market:
            signals.append("Niche Market")
    return signals


class AlertFormatter:
    """Formats RiskAssessments into multi-channel alert messages.

    Supports two verbosity levels:
    - compact: Essential info only (wallet, score, market)
    - detailed: Full context (all signals, links, trade details)
    """

    def __init__(
        self,
        verbosity: Literal["compact", "detailed"] = "detailed",
    ) -> None:
        """Initialize the formatter.

        Args:
            verbosity: Level of detail in formatted messages.
        """
        self.verbosity = verbosity

    def format(self, assessment: RiskAssessment) -> FormattedAlert:
        """Format a risk assessment into a multi-channel alert.

        Args:
            assessment: The risk assessment to format.

        Returns:
            FormattedAlert with all channel formats.
        """
        # Build common data
        wallet_short = truncate_address(assessment.wallet_address)
        risk_level = get_risk_level(assessment.weighted_score)
        signals = get_triggered_signals(assessment)

        # Build links
        links = self._build_links(assessment)

        # Build title
        title = f"🚨 Suspicious Activity Detected - {risk_level} Risk"

        # Build body based on verbosity
        body = self._build_body(assessment, wallet_short, risk_level, signals)

        # Build channel-specific formats
        discord_embed = self._build_discord_embed(
            assessment, wallet_short, risk_level, signals, links
        )
        telegram_md = self._build_telegram_markdown(
            assessment, wallet_short, risk_level, signals, links
        )
        plain_text = self._build_plain_text(assessment, wallet_short, risk_level, signals, links)

        return FormattedAlert(
            title=title,
            body=body,
            discord_embed=discord_embed,
            telegram_markdown=telegram_md,
            plain_text=plain_text,
            links=links,
        )

    def _build_links(self, assessment: RiskAssessment) -> dict[str, str]:
        """Build dictionary of relevant links."""
        trade = assessment.trade_event
        links = {
            "wallet": POLYGONSCAN_ADDRESS_URL.format(address=assessment.wallet_address),
        }

        # Add market link if we have the slug
        if trade.market_slug:
            links["market"] = POLYMARKET_MARKET_URL.format(slug=trade.market_slug)

        return links

    def _build_body(
        self,
        assessment: RiskAssessment,
        wallet_short: str,
        risk_level: str,
        signals: list[str],
    ) -> str:
        """Build the main body text."""
        trade = assessment.trade_event

        if self.verbosity == "compact":
            return (
                f"Wallet {wallet_short} made a {trade.side} trade "
                f"({format_usdc(trade.notional_value)}) with risk score "
                f"{assessment.weighted_score:.2f} ({risk_level})"
            )

        # Detailed body
        lines = [
            f"Wallet: {wallet_short}",
            f"Risk Score: {assessment.weighted_score:.2f} ({risk_level})",
            f"Trade: {trade.side} {trade.outcome} @ ${trade.price:.3f}",
            f"Size: {format_usdc(trade.notional_value)}",
        ]

        if signals:
            lines.append(f"Signals: {', '.join(signals)}")

        if trade.event_title:
            lines.append(f"Market: {trade.event_title}")

        return "\n".join(lines)

    def _build_discord_embed(
        self,
        assessment: RiskAssessment,
        wallet_short: str,
        risk_level: str,
        signals: list[str],
        links: dict[str, str],
    ) -> dict[str, object]:
        """Build Discord-optimized embed format."""
        trade = assessment.trade_event
        color = get_risk_color(assessment.weighted_score)

        # Get wallet age if available
        wallet_age_str = ""
        if assessment.fresh_wallet_signal:
            age_hours = assessment.fresh_wallet_signal.wallet_profile.age_hours
            if age_hours is not None:
                if age_hours < 1:
                    wallet_age_str = f" (Age: {int(age_hours * 60)}m)"
                else:
                    wallet_age_str = f" (Age: {age_hours:.0f}h)"

        fields: list[dict[str, object]] = [
            {
                "name": "Wallet",
                "value": f"`{wallet_short}`{wallet_age_str}",
                "inline": True,
            },
            {
                "name": "Risk Score",
                "value": f"{assessment.weighted_score:.2f} ({risk_level})",
                "inline": True,
            },
        ]

        # Market field
        market_title = trade.event_title or trade.market_slug or "Unknown Market"
        market_value = market_title
        if "market" in links:
            market_value = f"[{market_title}]({links['market']})"
        fields.append({"name": "Market", "value": market_value, "inline": False})

        # Trade details
        trade_detail = (
            f"{trade.side} {trade.outcome} @ ${trade.price:.3f} | "
            f"{format_usdc(trade.notional_value)}"
        )
        fields.append({"name": "Trade", "value": trade_detail, "inline": False})

        # Signals (if any)
        if signals:
            fields.append(
                {
                    "name": "Signals",
                    "value": ", ".join(signals),
                    "inline": False,
                }
            )

        # Add detailed info for detailed verbosity
        if self.verbosity == "detailed":
            # Add confidence breakdown
            confidences = []
            if assessment.fresh_wallet_signal:
                conf = assessment.fresh_wallet_signal.confidence
                confidences.append(f"Fresh Wallet: {conf:.0%}")
            if assessment.size_anomaly_signal:
                conf = assessment.size_anomaly_signal.confidence
                confidences.append(f"Size Anomaly: {conf:.0%}")

            if confidences:
                fields.append(
                    {
                        "name": "Confidence",
                        "value": " | ".join(confidences),
                        "inline": False,
                    }
                )

        embed: dict[str, object] = {
            "title": "🚨 Suspicious Activity Detected",
            "color": color,
            "fields": fields,
            "footer": {"text": "Polymarket Insider Tracker"},
        }

        # Add wallet link as URL if available
        if "wallet" in links:
            embed["url"] = links["wallet"]

        return embed

    def _build_telegram_markdown(
        self,
        assessment: RiskAssessment,
        wallet_short: str,
        risk_level: str,
        signals: list[str],
        links: dict[str, str],
    ) -> str:
        """Build Telegram-optimized markdown format."""
        trade = assessment.trade_event

        lines = ["🚨 *Suspicious Activity Detected*", ""]

        # Wallet with link
        wallet_line = f"*Wallet:* `{wallet_short}`"
        if assessment.fresh_wallet_signal:
            age_hours = assessment.fresh_wallet_signal.wallet_profile.age_hours
            if age_hours is not None:
                if age_hours < 1:
                    wallet_line += f" \\(Age: {int(age_hours * 60)}m\\)"
                else:
                    wallet_line += f" \\(Age: {age_hours:.0f}h\\)"
        lines.append(wallet_line)

        # Risk score
        lines.append(f"*Risk Score:* {assessment.weighted_score:.2f} \\({risk_level}\\)")

        # Market
        market_title = trade.event_title or trade.market_slug or "Unknown Market"
        # Escape special Telegram markdown characters
        market_title_escaped = self._escape_telegram_markdown(market_title)
        if "market" in links:
            lines.append(f"*Market:* [{market_title_escaped}]({links['market']})")
        else:
            lines.append(f"*Market:* {market_title_escaped}")

        # Trade details
        usdc_value = format_usdc(trade.notional_value).replace("$", "\\$")
        lines.append(
            f"*Trade:* {trade.side} {trade.outcome} @ \\${trade.price:.3f} \\| {usdc_value}"
        )

        # Signals
        if signals:
            lines.append(f"*Signals:* {', '.join(signals)}")

        # Links
        lines.append("")
        if "wallet" in links:
            lines.append(f"[View Wallet]({links['wallet']})")
        if "market" in links:
            lines.append(f"[View Market]({links['market']})")

        return "\n".join(lines)

    def _escape_telegram_markdown(self, text: str) -> str:
        """Escape special Telegram MarkdownV2 characters."""
        special_chars = [
            "_",
            "*",
            "[",
            "]",
            "(",
            ")",
            "~",
            "`",
            ">",
            "#",
            "+",
            "-",
            "=",
            "|",
            "{",
            "}",
            ".",
            "!",
        ]
        for char in special_chars:
            text = text.replace(char, f"\\{char}")
        return text

    def _build_plain_text(
        self,
        assessment: RiskAssessment,
        wallet_short: str,
        risk_level: str,
        signals: list[str],
        links: dict[str, str],
    ) -> str:
        """Build plain text format for generic channels."""
        trade = assessment.trade_event

        lines = [
            "SUSPICIOUS ACTIVITY DETECTED",
            "=" * 30,
            "",
        ]

        # Wallet info
        wallet_line = f"Wallet: {wallet_short}"
        if assessment.fresh_wallet_signal:
            age_hours = assessment.fresh_wallet_signal.wallet_profile.age_hours
            if age_hours is not None:
                if age_hours < 1:
                    wallet_line += f" (Age: {int(age_hours * 60)}m)"
                else:
                    wallet_line += f" (Age: {age_hours:.0f}h)"
        lines.append(wallet_line)

        # Risk
        lines.append(f"Risk Score: {assessment.weighted_score:.2f} ({risk_level})")

        # Market
        market_title = trade.event_title or trade.market_slug or "Unknown Market"
        lines.append(f"Market: {market_title}")

        # Trade
        lines.append(
            f"Trade: {trade.side} {trade.outcome} @ ${trade.price:.3f} | "
            f"{format_usdc(trade.notional_value)}"
        )

        # Signals
        if signals:
            lines.append(f"Signals: {', '.join(signals)}")

        # Links
        lines.append("")
        if "wallet" in links:
            lines.append(f"Wallet: {links['wallet']}")
        if "market" in links:
            lines.append(f"Market: {links['market']}")

        return "\n".join(lines)
