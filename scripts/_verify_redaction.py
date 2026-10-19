"""Preserve configured-secret redaction independently of gate execution."""

from __future__ import annotations

import os
from collections.abc import Mapping

from ._url_redaction import redact_url_occurrences

# Service URLs retain actionable host/path diagnostics. Other configured endpoints are complete-value
# secrets because their credentials can occur in paths; raw and decoded passwords are both hidden.
SERVICE_URL_KEYS = ("DATABASE_URL", "REDIS_URL")


SECRET_VALUE_KEYS = (
    "POLYMARKET_API_KEY",
    "DISCORD_WEBHOOK_URL",
    "TELEGRAM_BOT_TOKEN",
    "POLYGON_RPC_URL",
    "POLYGON_FALLBACK_RPC_URL",
    "POLYMARKET_TRADES_URL",
)


def _redact_urls(text: str, values: Mapping[str, str]) -> str:
    redacted = text
    for key in SERVICE_URL_KEYS:
        value = values.get(key)
        if value:
            redacted = redact_url_occurrences(redacted, value)
    return redacted


def _redact_secrets(text: str, values: Mapping[str, str]) -> str:
    redacted = text
    for key in SECRET_VALUE_KEYS:
        value = values.get(key)
        if value:
            redacted = redacted.replace(value, "***")
    return redacted


def redact_text(text: str, environment: Mapping[str, str] | None = None) -> str:
    """Redact configured credentials and complete sensitive values from diagnostics."""
    values = environment if environment is not None else os.environ
    return _redact_secrets(_redact_urls(text, values), values)
