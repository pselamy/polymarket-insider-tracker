"""Data models for the alerter module."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FormattedAlert:
    """A formatted alert message ready for delivery across multiple channels.

    Attributes:
        title: Short alert title/headline.
        body: Main alert body text.
        discord_embed: Discord-optimized embed dictionary.
        telegram_markdown: Telegram-formatted markdown string.
        plain_text: Plain text fallback for other channels.
        links: Dictionary of relevant links (e.g., market, wallet explorer).
    """

    title: str
    body: str
    discord_embed: dict[str, object]
    telegram_markdown: str
    plain_text: str
    links: dict[str, str] = field(default_factory=dict)
