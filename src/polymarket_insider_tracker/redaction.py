"""Centralized redaction of credential-bearing URLs in output and error text.

Secrets may ride in a URL's userinfo (``user:password@`` or a lone token before
the ``@``), in query values (``?apikey=...``), or in the final path segment of
a dedicated-provider endpoint (``https://<host>/v2/<key>``). Every
operator-facing surface — configuration summaries, CLI output, logs, health
responses, and exception text that may embed a URL — must pass through these
helpers so a secret never leaks verbatim (Constitution: never log secrets or
full credential-bearing URLs).
"""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlsplit, urlunsplit

MASK = "***"

# URL-shaped substrings inside arbitrary text (exception messages, log lines).
# A trailing run of closing delimiters (``)``, ``]``, ``}``, quotes, or angle
# brackets) belongs to the surrounding prose, not the URL: without trimming,
# a parenthesized URL would absorb its closing ``)`` into the match and the
# redacted output would lose that character.
_URL_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>]+")
_URL_TRAILING_TRIM = ")]}\"'<>.,;:!?"
# Fallbacks when a URL cannot be parsed: credentials before an ``@`` and
# everything after a ``?`` may both carry secrets.
_USERINFO_FALLBACK = re.compile(r"://[^/@\s]*@")
_QUERY_FALLBACK = re.compile(r"\?\S*")

# A dedicated-provider endpoint may carry its credential as the final path
# segment (``https://<host>/v2/<key>``). The configuration contract cannot
# distinguish that segment from a benign version prefix, so redaction is
# fail-closed: on any URL-shaped value with a non-root path, scheme and host
# (plus port) identify the endpoint while the path is never emitted.
_PATH_CREDENTIAL_MARKER = "***path***"


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
    return "&".join(f"{key}={MASK}" for key, _ in pairs)


def _fallback_redaction(url: str) -> str:
    """Mask a string that looks like a URL but cannot be parsed as one."""
    masked = _USERINFO_FALLBACK.sub(f"://{MASK}@", url)
    return _QUERY_FALLBACK.sub(f"?{MASK}", masked)


def _masked_netloc(netloc: str, username: str | None, password: str | None) -> str:
    """Mask userinfo credentials while keeping the host (and port) readable."""
    if "@" not in netloc:
        return netloc
    host = netloc.rpartition("@")[2]
    return f"{_redacted_userinfo(username, password)}@{host}"


def _masked_path(path: str) -> str:
    """Mask a non-root endpoint path, which may carry a provider credential."""
    if path in ("", "/"):
        return path
    return _PATH_CREDENTIAL_MARKER


def redact_url(url: str) -> str:
    """Mask userinfo credentials, query values, fragments, and endpoint paths.

    The path policy is fail-closed and grounded in the configuration contract:
    endpoint settings (``POLYGON_RPC_URL``, ``POLYGON_FALLBACK_RPC_URL``,
    ``POLYMARKET_TRADES_URL``, database and Redis URLs) accept URLs whose path
    may carry a provider credential, and no static shape distinguishes a key
    segment from a benign prefix. The masked output therefore keeps the scheme
    plus host (and port when present) so the endpoint stays diagnosable, while
    the path itself is never emitted.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return _fallback_redaction(url)
    if "@" in parts.path:
        # A malformed nested-scheme URL (e.g. ``wss://https://user:secret@host``)
        # smuggles its credentials into the parsed path; mask by shape instead.
        return _fallback_redaction(url)
    netloc = _masked_netloc(parts.netloc, parts.username, parts.password)
    query = _redacted_query(parts.query) if parts.query else ""
    fragment = MASK if parts.fragment else ""
    return urlunsplit((parts.scheme, netloc, _masked_path(parts.path), query, fragment))


def _split_trailing_delimiters(match: str) -> tuple[str, str]:
    """Split prose delimiters off the end of a URL-shaped match."""
    end = len(match)
    while end > 0 and match[end - 1] in _URL_TRAILING_TRIM:
        end -= 1
    return match[:end], match[end:]


def redact_text(text: str) -> str:
    """Mask credentials inside every URL-shaped substring of ``text``."""
    return _URL_PATTERN.sub(
        lambda match: _redact_url_match(match.group(0)),
        text,
    )


def _redact_url_match(matched: str) -> str:
    """Redact one URL-shaped match, preserving surrounding prose delimiters."""
    url, trailing = _split_trailing_delimiters(matched)
    if not url or "://" not in url:
        return matched
    return redact_url(url) + trailing
