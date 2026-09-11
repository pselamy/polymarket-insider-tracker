"""Centralized redaction of credential-bearing URLs in output and error text.

Secrets ride in a URL's userinfo (``user:password@`` or a lone token before the
``@``) and in query values (``?apikey=...``). Every operator-facing surface —
configuration summaries, CLI output, logs, health responses, and exception text
that may embed a URL — must pass through these helpers so a secret never leaks
verbatim (Constitution: never log secrets or full credential-bearing URLs).
"""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

MASK = "***"

# URL-shaped substrings inside arbitrary text (exception messages, log lines).
_URL_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>]+")
# Fallbacks when a URL cannot be parsed: credentials before an ``@`` and
# everything after a ``?`` may both carry secrets.
_USERINFO_FALLBACK = re.compile(r"://[^/@\s]*@")
_QUERY_FALLBACK = re.compile(r"\?\S*")


def _redacted_userinfo(username: str | None, password: str | None) -> str:
    """A username is kept only when it is paired with a maskable password.

    A lone userinfo token (for example an API key used as the user part) is
    itself the credential and must disappear entirely.
    """
    if password is not None:
        return f"{username or ''}:{MASK}"
    return MASK


def _redacted_query(query: str) -> str:
    """Mask every query value while keeping the keys readable."""
    pairs = parse_qsl(query, keep_blank_values=True)
    if not pairs:
        return MASK
    return urlencode([(key, MASK) for key, _ in pairs])


def _fallback_redaction(url: str) -> str:
    """Mask a string that looks like a URL but cannot be parsed as one."""
    masked = _USERINFO_FALLBACK.sub(f"://{MASK}@", url)
    return _QUERY_FALLBACK.sub(f"?{MASK}", masked)


def redact_url(url: str) -> str:
    """Mask userinfo credentials and query values while keeping the routable shape."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return _fallback_redaction(url)
    if "@" in parts.path:
        # A malformed nested-scheme URL (e.g. ``wss://https://user:secret@host``)
        # smuggles its credentials into the parsed path; mask by shape instead.
        return _fallback_redaction(url)
    netloc = parts.netloc
    if "@" in netloc:
        host = netloc.rpartition("@")[2]
        netloc = f"{_redacted_userinfo(parts.username, parts.password)}@{host}"
    query = _redacted_query(parts.query) if parts.query else ""
    fragment = MASK if parts.fragment else ""
    return urlunsplit((parts.scheme, netloc, parts.path, query, fragment))


def redact_text(text: str) -> str:
    """Mask credentials inside every URL-shaped substring of ``text``."""
    return _URL_PATTERN.sub(lambda match: redact_url(match.group(0)), text)
