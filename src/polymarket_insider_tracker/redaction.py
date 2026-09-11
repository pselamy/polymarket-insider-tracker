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
# redacted output would lose that character. An ``@``-terminated token inside
# the path position (``https://host/<segment>@...``) is credential-shaped under
# this policy's contract, so it stays inside the match: ``@`` must not act as
# a boundary that lets the path secret after it escape into prose.
_URL_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>]+")
_URL_TRAILING_TRIM = ")]}\"'<>.,;:!?"
# Fallbacks when a URL cannot be parsed: credentials before an ``@`` and
# everything after a ``?`` may both carry secrets. A path-like segment after an
# embedded host inside the parsed path (``wss://https://user:pw@host/<path>``)
# is masked by the same shape so the fallback cannot re-emit it.
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
    """Mask every query token while keeping only unambiguous keys readable.

    A bare token (``?SECRET``) and a valuated pair (``?key=SECRET``) are
    indistinguishable without inventing parsing semantics: ``parse_qsl`` maps
    both to a single pair. Fail closed — mask any pair whose value is empty,
    so the key-like token is never re-emitted.
    """
    pairs = parse_qsl(query, keep_blank_values=True)
    if not pairs:
        return MASK
    return "&".join(_rendered_query_pair(key, value) for key, value in pairs)


def _rendered_query_pair(key: str, value: str) -> str:
    """Render one parsed query pair with its value masked fail-closed."""
    if not value:
        return MASK
    return f"{key}={MASK}"


def _fallback_redaction(url: str) -> str:
    """Mask a string that looks like a URL but cannot be parsed as one.

    Fail closed: after masking embedded userinfo and query shapes, also mask
    any remaining path-like credential the parser could not isolate.
    """
    masked = _USERINFO_FALLBACK.sub(f"://{MASK}@", url)
    masked = _QUERY_FALLBACK.sub(f"?{MASK}", masked)
    return _mask_post_userinfo_path(masked)


def _mask_post_userinfo_path(url: str) -> str:
    """Mask the path after the ``@``-delimited embedded host in a fallback.

    A malformed nested-scheme URL (``wss://https://user:pw@host/<path>``) keeps
    its true path after the embedded host; the fragments before it are already
    masked by the userinfo rule. The embedded host itself stays readable so the
    endpoint remains diagnosable; everything path-shaped after it is masked.
    """
    at = _first_userinfo_at(url)
    if at < 0:
        return url
    slash = url.find("/", at)
    if slash < 0:
        return url
    end = _path_end(url, slash)
    return url[:slash] + _masked_tail(url[slash:end]) + _masked_suffix(url[end:])


def _first_userinfo_at(url: str) -> int:
    """Index of the first ``@`` that can delimit fallback userinfo, else -1."""
    start = url.find("://") + 3 if "://" in url else 0
    return url.find("@", start)


def _path_end(url: str, slash: int) -> int:
    """Index where the fallback path stops (before any query/fragment)."""
    end = len(url)
    for delimiter in ("?", "#"):
        found = url.find(delimiter, slash)
        if found >= 0:
            end = min(end, found)
    return end


def _masked_tail(tail: str) -> str:
    """Mask a fallback path tail, which may itself contain ``@`` segments."""
    if tail.startswith("/"):
        return "/" + MASK
    return tail


def _masked_suffix(suffix: str) -> str:
    """Keep a masked query marker visible while preserving fragments as-is."""
    if suffix.startswith("?") and not suffix.startswith(f"?{MASK}"):
        return f"?{MASK}" + suffix[1:]
    return suffix


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
    nested = _nested_scheme_kind(parts.path, parts.netloc, parts.hostname)
    if nested is not None:
        return _fallback_redaction(url)
    netloc = _masked_netloc(parts.netloc, parts.username, parts.password)
    query = _redacted_query(parts.query) if parts.query else ""
    fragment = MASK if parts.fragment else ""
    return urlunsplit((parts.scheme, netloc, _masked_path(parts.path), query, fragment))


def _nested_scheme_kind(path: str, netloc: str, hostname: str | None) -> str | None:
    """Classify a malformed nested-scheme/userinfo shape, else None.

    Every ``@``-bearing shape the structured parser cannot attribute to a real
    netloc userinfo is credential-shaped under this policy's contract and must
    go through the shape-based fallback instead of emitting any part of it.
    Single-condition predicates keep each check flat and explicit.
    """
    if _has_smuggled_userinfo(path, netloc):
        return "smuggled-userinfo"
    if _has_split_nested_scheme(netloc, path):
        return "split-nested-scheme"
    if _has_embedded_nested_scheme(path):
        return "embedded-nested-scheme"
    if _has_hostless_userinfo(netloc, hostname):
        return "hostless-userinfo"
    return None


def _has_smuggled_userinfo(path: str, netloc: str) -> bool:
    """Credentials hide in the parsed path beside a netloc ``@``."""
    return "@" in path and "@" in netloc


def _has_split_nested_scheme(netloc: str, path: str) -> bool:
    """The inner ``://`` split across the outer netloc/path boundary."""
    return netloc.endswith(":") and path.startswith("//")


def _has_embedded_nested_scheme(path: str) -> bool:
    """The parsed path embeds a second URL-shaped value."""
    return "@" in path and "://" in path


def _has_hostless_userinfo(netloc: str, hostname: str | None) -> bool:
    """An ``@``-bearing netloc produced no usable host."""
    return "@" in netloc and not hostname


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
