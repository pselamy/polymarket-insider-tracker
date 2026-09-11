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
# Placeholder emitted for ambiguous/malformed URL shapes the policy cannot
# safely decompose. The path-shaped marker keeps the output stable under
# repeated redaction: re-running the policy over its own placeholder yields
# the identical string, and no input bytes are re-emitted.
_AMBIGUOUS_MASK = _PATH_CREDENTIAL_MARKER


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
    """Render one parsed query pair with its value masked fail-closed.

    The key is emitted only when it is unambiguous readable text: a decoded
    key that reintroduces query syntax (``&``, ``=``, ``?``, ``#``, ``%``) is
    itself masked so an encoded key can never decode into fresh query syntax.
    """
    if not value or _is_ambiguous_query_key(key):
        return MASK
    return f"{key}={MASK}"


def _is_ambiguous_query_key(key: str) -> bool:
    """A query key carrying fresh syntax or encoding residue is not readable."""
    return any(char in key for char in ("&", "=", "?", "#", "%"))


def _fallback_redaction(url: str) -> str:
    """Mask a string that looks like a URL but cannot be parsed as one.

    Fail closed and deterministic: no part of the input is re-emitted. The
    placeholder keeps the output URL-shaped so downstream ``://`` handling
    cannot re-derive structure from it.
    """
    scheme = url.split("://", 1)[0] if "://" in url else ""
    prefix = _safe_scheme_prefix(scheme)
    return prefix + _AMBIGUOUS_MASK


def _safe_scheme_prefix(scheme: str) -> str:
    """The scheme label is readable only when it is plain ASCII alphanumerics."""
    if scheme and all(char.isascii() and char.isalnum() for char in scheme):
        return f"{scheme.lower()}://"
    return ""


def _masked_netloc(netloc: str, username: str | None, password: str | None) -> str:
    """Mask userinfo credentials while keeping a validated host (and port) readable."""
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
    the path itself is never emitted. Nested-scheme shapes (``://`` inside the
    parsed path, a split ``netloc == "scheme:"`` with a ``//`` path, or any
    ``@`` the structured parser cannot attribute to real netloc userinfo),
    bracket/IPv6-malformed inputs, and netlocs with an invalid port all fail
    closed through the same placeholder.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return _fallback_redaction(url)
    if _is_invalid_port_shape(parts):
        return _fail_closed_label(url, parts)
    if _is_fail_closed_shape(parts):
        return _fail_closed_label(url, parts)
    netloc = _masked_netloc(parts.netloc, parts.username, parts.password)
    query = _redacted_query(parts.query) if parts.query else ""
    fragment = MASK if parts.fragment else ""
    return urlunsplit((parts.scheme, netloc, _masked_path(parts.path), query, fragment))


def _is_invalid_port_shape(parts: object) -> bool:
    """A netloc whose port is non-numeric or out of range is fail-closed.

    The token after the host colon may itself be the credential (a mistyped
    ``host:key`` for ``host/key``); accessing ``parts.port`` raises
    ``ValueError`` for exactly this shape, so probing it is both the detector
    and the proof that the port is not a diagnosable endpoint label.
    """
    netloc = str(getattr(parts, "netloc", ""))
    if _is_port_probe_exempt(netloc):
        return False
    return _port_probe_fails(parts)


def _is_port_probe_exempt(netloc: str) -> bool:
    """Shapes the port probe cannot attribute: userinfo, brackets, or no colon."""
    if "@" in netloc:
        return True
    if "[" in netloc or "]" in netloc:
        return True
    return ":" not in netloc


def _port_probe_fails(parts: object) -> bool:
    """The port probe raises, or resolves outside the valid range."""
    try:
        port = getattr(parts, "port", None)
    except ValueError:
        return True
    return port is not None and not 0 <= port <= 65535


def _is_fail_closed_shape(parts: object) -> bool:
    """Any ambiguous structured shape fails closed without emitting input."""
    netloc = str(getattr(parts, "netloc", ""))
    path = str(getattr(parts, "path", ""))
    hostname = getattr(parts, "hostname", None)
    if _has_split_nested_scheme(netloc, path):
        return True
    if _is_malformed_bracket_netloc(netloc, hostname):
        return True
    if _is_unparseable_netloc(parts):
        return True
    return _nested_scheme_kind(path, netloc, hostname) is not None


def _fail_closed_label(url: str, parts: object) -> str:
    """The deterministic masked label for an ambiguous structured shape."""
    scheme = str(getattr(parts, "scheme", ""))
    netloc = str(getattr(parts, "netloc", ""))
    path = str(getattr(parts, "path", ""))
    hostname = getattr(parts, "hostname", None)
    if _has_split_nested_scheme(netloc, path):
        return _fallback_redaction(url)
    if _is_malformed_bracket_netloc(netloc, hostname):
        return _fallback_redaction(url)
    if _is_invalid_port_shape(parts):
        return _port_fail_closed_label(parts)
    if _is_unparseable_netloc(parts):
        return _fallback_redaction(url)
    return _nested_masked_label(scheme, netloc)


def _port_fail_closed_label(parts: object) -> str:
    """Mask an invalid-port shape without re-emitting the port token.

    The scheme stays readable (it names which setting produced the label) and
    the host resolves only through the safe outer-host check; a host the check
    cannot prove safe collapses to the bare placeholder.
    """
    scheme = str(getattr(parts, "scheme", ""))
    hostname = getattr(parts, "hostname", None)
    prefix = _safe_scheme_prefix(scheme)
    if isinstance(hostname, str) and _safe_outer_host(hostname) == hostname:
        return f"{prefix}{hostname}/{_PATH_CREDENTIAL_MARKER}"
    return prefix + _AMBIGUOUS_MASK


def _is_malformed_bracket_netloc(netloc: str, hostname: str | None) -> bool:
    """An unbalanced bracket netloc is ambiguous: fail closed without echo."""
    return ("[" in netloc or "]" in netloc) and hostname is None


def _is_unparseable_netloc(parts: object) -> bool:
    """A netloc with no usable host is not a diagnosable endpoint label."""
    hostname = getattr(parts, "hostname", None)
    netloc = str(getattr(parts, "netloc", ""))
    return bool(netloc) and not hostname and "@" not in netloc


def _nested_masked_label(scheme: str, netloc: str) -> str:
    """Mask a nested/ambiguous shape while keeping a safe outer endpoint label."""
    outer = _safe_outer_host(netloc)
    prefix = _safe_scheme_prefix(scheme)
    if outer is None:
        return prefix + _AMBIGUOUS_MASK
    return f"{prefix}{outer}/{_PATH_CREDENTIAL_MARKER}"


def _safe_outer_host(netloc: str) -> str | None:
    """The readable outer host of a nested shape, or None when ambiguous."""
    host = netloc.split("@")[-1]
    if not host or "://" in host or "@" in host:
        return None
    if "[" in host or "]" in host:
        return None
    if not all(char.isascii() and (char.isalnum() or char in ".-:") for char in host):
        return None
    return host


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
