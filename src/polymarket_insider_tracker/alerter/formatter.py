"""Alert message formatter for multi-channel delivery.

This module transforms RiskAssessment objects into human-readable,
actionable alert messages optimized for Discord, Telegram, and plain text.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

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
    signals: list[str] = []
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

    @staticmethod
    def _format_wallet_age_str(signal: object | None) -> str:
        if signal is None:
            return ""
        age_hours = getattr(getattr(signal, "wallet_profile", None), "age_hours", None)
        if age_hours is None:
            return ""
        if age_hours < 1:
            return f" (Age: {int(age_hours * 60)}m)"
        return f" (Age: {age_hours:.0f}h)"

    @staticmethod
    def _format_telegram_wallet_age_str(signal: object | None) -> str:
        if signal is None:
            return ""
        age_hours = getattr(getattr(signal, "wallet_profile", None), "age_hours", None)
        if age_hours is None:
            return ""
        if age_hours < 1:
            return f" \\(Age: {int(age_hours * 60)}m\\)"
        return f" \\(Age: {age_hours:.0f}h\\)"

    @staticmethod
    def _build_confidence_field(assessment: RiskAssessment) -> dict[str, object] | None:
        confidences: list[str] = []
        if assessment.fresh_wallet_signal:
            confidences.append(f"Fresh Wallet: {assessment.fresh_wallet_signal.confidence:.0%}")
        if assessment.size_anomaly_signal:
            confidences.append(f"Size Anomaly: {assessment.size_anomaly_signal.confidence:.0%}")
        if not confidences:
            return None
        return {"name": "Confidence", "value": " | ".join(confidences), "inline": False}

    @staticmethod
    def _build_discord_market_field(trade: Any, links: dict[str, str]) -> dict[str, object]:
        market_title = trade.event_title or trade.market_slug or "Unknown Market"
        market_value = f"[{market_title}]({links['market']})" if "market" in links else market_title
        return {"name": "Market", "value": market_value, "inline": False}

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
        wallet_age_str = self._format_wallet_age_str(assessment.fresh_wallet_signal)

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
            self._build_discord_market_field(trade, links),
            {
                "name": "Trade",
                "value": (
                    f"{trade.side} {trade.outcome} @ ${trade.price:.3f} | "
                    f"{format_usdc(trade.notional_value)}"
                ),
                "inline": False,
            },
        ]

        if signals:
            fields.append({"name": "Signals", "value": ", ".join(signals), "inline": False})

        if self.verbosity == "detailed":
            conf_field = self._build_confidence_field(assessment)
            if conf_field is not None:
                fields.append(conf_field)

        embed: dict[str, object] = {
            "title": "🚨 Suspicious Activity Detected",
            "color": color,
            "fields": fields,
            "footer": {"text": "Polymarket Insider Tracker"},
        }

        if "wallet" in links:
            embed["url"] = links["wallet"]

        return embed

    def _build_telegram_market_line(self, trade: object, links: dict[str, str]) -> str:
        market_title = (
            getattr(trade, "event_title", None)
            or getattr(trade, "market_slug", None)
            or "Unknown Market"
        )
        escaped_title = self._escape_telegram_markdown(str(market_title))
        if "market" in links:
            return f"*Market:* [{escaped_title}]({links['market']})"
        return f"*Market:* {escaped_title}"

    @staticmethod
    def _build_telegram_link_lines(links: dict[str, str]) -> list[str]:
        lines: list[str] = []
        if "wallet" in links:
            lines.append(f"[View Wallet]({links['wallet']})")
        if "market" in links:
            lines.append(f"[View Market]({links['market']})")
        return lines

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
        wallet_suffix = self._format_telegram_wallet_age_str(assessment.fresh_wallet_signal)
        score_str = self._escape_telegram_markdown(f"{assessment.weighted_score:.2f}")
        usdc_value = self._escape_telegram_markdown(format_usdc(trade.notional_value))
        price_str = self._escape_telegram_markdown(f"{trade.price:.3f}")
        side_escaped = self._escape_telegram_markdown(trade.side)
        outcome_escaped = self._escape_telegram_markdown(trade.outcome)

        lines = [
            "🚨 *Suspicious Activity Detected*",
            "",
            f"*Wallet:* `{wallet_short}`{wallet_suffix}",
            f"*Risk Score:* {score_str} \\({risk_level}\\)",
            self._build_telegram_market_line(trade, links),
            f"*Trade:* {side_escaped} {outcome_escaped} @ \\${price_str} \\| {usdc_value}",
        ]

        if signals:
            signals_escaped = [self._escape_telegram_markdown(s) for s in signals]
            lines.append(f"*Signals:* {', '.join(signals_escaped)}")

        link_lines = self._build_telegram_link_lines(links)
        if link_lines:
            lines.append("")
            lines.extend(link_lines)

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

    @staticmethod
    def _build_plain_links(links: dict[str, str]) -> list[str]:
        lines: list[str] = []
        if "wallet" in links:
            lines.append(f"Wallet: {links['wallet']}")
        if "market" in links:
            lines.append(f"Market: {links['market']}")
        return lines

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
        age_str = self._format_wallet_age_str(assessment.fresh_wallet_signal)
        market_title = trade.event_title or trade.market_slug or "Unknown Market"

        lines = [
            "SUSPICIOUS ACTIVITY DETECTED",
            "=" * 30,
            "",
            f"Wallet: {wallet_short}{age_str}",
            f"Risk Score: {assessment.weighted_score:.2f} ({risk_level})",
            f"Market: {market_title}",
            f"Trade: {trade.side} {trade.outcome} @ ${trade.price:.3f} | {format_usdc(trade.notional_value)}",
        ]

        if signals:
            lines.append(f"Signals: {', '.join(signals)}")

        link_lines = self._build_plain_links(links)
        if link_lines:
            lines.append("")
            lines.extend(link_lines)

        return "\n".join(lines)
