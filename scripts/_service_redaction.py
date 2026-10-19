"""Redact explicit service URLs and all raw/decoded password spellings."""

from __future__ import annotations

from ._url_redaction import redact_url_occurrences


def _redact_text(value: str, *sensitive_urls: str) -> str:
    """Hide every given URL, and every spelling of its password, inside diagnostic text."""
    redacted = value
    for url in sensitive_urls:
        if url:
            redacted = redact_url_occurrences(redacted, url)
    return redacted


redact_text = _redact_text
