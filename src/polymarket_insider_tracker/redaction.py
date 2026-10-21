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
from collections.abc import Iterable
from urllib.parse import SplitResult, parse_qsl, quote, unquote, urlsplit, urlunsplit

MASK = "***"
# Emitted when rendering an exception itself fails (a raising ``args``
# property or ``__str__``): sanitization must never turn an error report
# into a new uncontrolled error, and the fixed placeholder re-emits no
# input bytes.
FAILSAFE_EXCEPTION_MESSAGE = "*** (exception sanitization failed)"

# URL-shaped substrings inside arbitrary text (exception messages, log lines).
# A trailing run of closing delimiters (``)``, ``]``, ``}``, quotes, or angle
# brackets) belongs to the surrounding prose, not the URL: without trimming,
# a parenthesized URL would absorb its closing ``)`` into the match and the
# redacted output would lose that character. An ``@``-terminated token inside
# the path position (``https://host/<segment>@...``) is credential-shaped under
# this policy's contract, so it stays inside the match: ``@`` must not act as
# a boundary that lets the path secret after it escape into prose. A single
# quote likewise stays inside the match: it is valid raw URI text, so an
# accepted endpoint path may carry one ahead of a credential tail
# (``/v2/prefix'KEY``), and stopping there would re-emit that tail as prose;
# a purely trailing quote is prose punctuation and is trimmed back off. The
# remaining boundary characters (whitespace, control, ``"``, ``<``, ``>``)
# are not valid raw URI text, and ``is_scannable_url_text`` keeps configured
# endpoint URLs inside exactly this accepted set.
_URL_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s\"<>]+")
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
# ``quote`` leaves ``/`` readable unless told nothing is safe; every reserved character of a
# credential spelling must be percent-encoded.
_NOTHING_SAFE = ""
# Non-alphanumeric characters a readable host label may carry (dots, hyphens, port colon).
_HOST_PUNCTUATION = ".-:"
# Printable characters that still end a text-scan match (see ``_URL_PATTERN``).
_URL_SCAN_BOUNDARY = '"<>'


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
    cannot re-derive structure from it. A string without ``://`` is its own
    scheme candidate, and a candidate holding ``/`` is never plain ASCII
    alphanumerics, so it contributes no prefix.
    """
    return _safe_scheme_prefix(url.partition("://")[0]) + _AMBIGUOUS_MASK


def _safe_scheme_prefix(scheme: str) -> str:
    """The scheme label is readable only when it is plain ASCII alphanumerics."""
    if scheme.isascii() and scheme.isalnum():
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
    closed = _fail_closed_label(url, parts)
    if closed is not None:
        return closed
    netloc = _masked_netloc(parts.netloc, parts.username, parts.password)
    query = _redacted_query(parts.query) if parts.query else ""
    fragment = MASK if parts.fragment else ""
    return urlunsplit((parts.scheme, netloc, _masked_path(parts.path), query, fragment))


def _fail_closed_label(url: str, parts: SplitResult) -> str | None:
    """The deterministic masked label for an ambiguous structured shape, else None.

    Every ambiguous structured shape fails closed without emitting input. The
    checks run in a fixed order because the first match decides which label
    (bare placeholder, safe-host label) is emitted.
    """
    if _has_split_nested_scheme(parts.netloc, parts.path):
        return _fallback_redaction(url)
    if _has_invalid_port(parts):
        return _port_fail_closed_label(parts)
    if _is_unparseable_netloc(parts.netloc, parts.hostname):
        return _fallback_redaction(url)
    if _has_nested_scheme_shape(parts.path, parts.netloc, parts.hostname):
        return _nested_masked_label(parts.scheme, parts.netloc)
    return None


def _has_invalid_port(parts: SplitResult) -> bool:
    """A netloc whose port is non-numeric or out of range is fail-closed.

    The token after the host colon may itself be the credential (a mistyped
    ``host:key`` for ``host/key``); reading ``parts.port`` raises
    ``ValueError`` for exactly this shape (non-numeric or outside 0-65535),
    so probing it is both the detector and the proof that the port is not a
    diagnosable endpoint label. The probe attributes the colon correctly
    through userinfo and bracketed IPv6 hosts, so no netloc shape is exempt:
    ``user@host:KEY`` and ``[::1]:KEY`` must never re-emit the port-position
    token through the netloc mask.
    """
    try:
        _ = parts.port
    except ValueError:
        return True
    return False


def _port_fail_closed_label(parts: SplitResult) -> str:
    """Mask an invalid-port shape without re-emitting the port token.

    The scheme stays readable (it names which setting produced the label) and
    the host resolves only through the safe host check; a host the check
    cannot prove safe collapses to the bare placeholder.
    """
    prefix = _safe_scheme_prefix(parts.scheme)
    host = None if parts.hostname is None else _validated_host_label(parts.hostname)
    if host is None:
        return prefix + _AMBIGUOUS_MASK
    return f"{prefix}{host}/{_PATH_CREDENTIAL_MARKER}"


def _is_unparseable_netloc(netloc: str, hostname: str | None) -> bool:
    """A netloc with no usable host is not a diagnosable endpoint label.

    A bracketed netloc whose host cannot be read lands here too: ``urlsplit``
    rejects unbalanced or non-address brackets itself, and any other bracket
    shape without a hostname is a netloc with no usable host.
    """
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
    if not host:
        return None
    return _validated_host_label(host)


def _validated_host_label(host: str) -> str | None:
    """A host label with only safe characters and, when present, a real port tail.

    The character set excludes ``@``, ``/``, ``:`` runs and brackets, so a
    nested URL or bracketed fragment can never pass as a host.
    """
    if not all(char.isascii() and (char.isalnum() or char in _HOST_PUNCTUATION) for char in host):
        return None
    if not _has_valid_port_tail(host):
        return None
    return host


def _has_valid_port_tail(host: str) -> bool:
    """Any colon tail in an outer-host label must be a number.

    An empty or non-numeric tail is credential-shaped under this policy's
    contract and must not survive as a fake port; more than one colon leaves
    a colon in the tail, which is not numeric, so it is not an attributable
    host:port shape at all. The range was already enforced by the port probe
    of the caller's parsed result, which runs before any label is built.
    """
    _, colon, tail = host.partition(":")
    return not colon or tail.isdigit()


def _has_nested_scheme_shape(path: str, netloc: str, hostname: str | None) -> bool:
    """A malformed nested-scheme/userinfo shape (the split shape is checked earlier).

    Every ``@``-bearing shape the structured parser cannot attribute to a real
    netloc userinfo is credential-shaped under this policy's contract and must
    go through the shape-based fallback instead of emitting any part of it.
    """
    return (
        _has_smuggled_userinfo(path, netloc)
        or _has_embedded_nested_scheme(path)
        or _has_hostless_userinfo(netloc, hostname)
    )


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


def redact_argument(arg: object) -> object:
    """Scrub one diagnostic argument; only plain strings have safe redaction.

    Numbers, booleans, and ``None`` cannot embed a URL-shaped secret and stay
    readable so numeric format specifiers (``%d``, ``%.1f``) keep rendering.
    Containers (dicts, lists, tuples, bytes) may carry a credential in a
    shape ``redact_text`` cannot see as a URL, so they become the
    deterministic placeholder.
    """
    if isinstance(arg, str):
        return redact_text(arg)
    if arg is None or isinstance(arg, (int, float, complex)):
        return arg
    return MASK


def redact_exception_message(exc: BaseException) -> str:
    """Render an exception's message with every argument scrubbed fail-closed.

    ``str(exc)`` interpolates raw arguments, so a bytes or container argument
    re-emits its credential verbatim on the message line before any log
    filter or text-level redaction can see it. When every argument is plain
    text or a safe scalar the rendered message is redacted as text (custom
    ``__str__`` output included); otherwise the message is rebuilt from the
    scrubbed arguments alone and no raw argument is ever rendered.

    Rendering is total: a pathological exception (a raising ``args``
    property or ``__str__``) collapses to the fixed placeholder instead of
    raising, so no logging or status entry point needs its own guard.
    """
    try:
        return _rendered_exception_message(exc)
    except Exception:
        return FAILSAFE_EXCEPTION_MESSAGE


def _rendered_exception_message(exc: BaseException) -> str:
    """The argument-aware rendering that the total fail-safe wrapper guards."""
    if all(_is_safe_message_argument(arg) for arg in exc.args):
        return redact_text(str(exc))
    scrubbed = tuple(redact_argument(arg) for arg in exc.args)
    if len(scrubbed) == 1:
        return str(scrubbed[0])
    return str(scrubbed)


def _is_safe_message_argument(arg: object) -> bool:
    """Only text and scalar arguments render safely through ``str(exc)``."""
    return arg is None or isinstance(arg, (str, int, float, complex))


def _split_trailing_delimiters(match: str) -> tuple[str, str]:
    """Split prose delimiters off the end of a URL-shaped match."""
    url = match.rstrip(_URL_TRAILING_TRIM)
    return url, match[len(url) :]


def redact_text(text: str) -> str:
    """Mask credentials inside every URL-shaped substring of ``text``."""
    return _URL_PATTERN.sub(
        lambda match: _redact_url_match(match.group(0)),
        text,
    )


def is_scannable_url_text(value: str) -> bool:
    """True when every character of ``value`` stays inside one text-scan match.

    This is the consistency invariant between validation and text redaction:
    a configured endpoint URL containing a raw character that terminates a
    ``redact_text`` match (whitespace, ``"``, ``<``, ``>``) or a control
    character would split at that character inside diagnostic text, and the
    tail — possibly a path credential — would escape the mask as prose. No
    such character is valid raw URI text; percent-encoded forms stay
    scannable and accepted.
    """
    return not any(_is_scan_boundary_char(char) for char in value)


def _is_scan_boundary_char(char: str) -> bool:
    """A character the text scanner cannot keep inside a URL-shaped match."""
    return char.isspace() or not char.isprintable() or char in _URL_SCAN_BOUNDARY


def redact_text_with_secrets(text: str, secrets: Iterable[str]) -> str:
    """Mask known secret substrings, then every URL-shaped substring.

    The exact replacements catch forms the URL scanner cannot see — a bare
    credential component on its own, or a JSON-escaped or percent-encoded
    echo of it — and the central text policy then masks anything still
    URL-shaped. ``secrets`` must be ordered longest-first so a shorter
    component never splits a longer one before that longer form is replaced.
    """
    for secret in secrets:
        text = text.replace(secret, MASK)
    return redact_text(text)


def redact_failure_with_secrets(exc: BaseException, secrets: Iterable[str]) -> str:
    """Render a failure argument-aware, then mask known secret spellings.

    ``str(exc)`` interpolates raw arguments, so a bytes or container
    argument would re-emit its secret verbatim before any text-level
    replacement could see it; the shared total exception renderer runs
    first, and the rendered text then takes the exact-replacement pass plus
    the central URL policy.
    """
    return redact_text_with_secrets(redact_exception_message(exc), secrets)


def url_credential_components(url: str) -> tuple[str, ...]:
    """Every substring of a configured URL that must never reach diagnostics.

    A server response or transport error can echo the request credential in
    partial or re-encoded form the URL scanner cannot attribute: the bare
    token alone, an individual query value, the decoded form of a
    percent-encoded segment, a JSON ``\\/``-escaped spelling of the URL, or
    a percent-encoded one. Masking each component (userinfo parts, path
    segments, query tokens, fragment) independently — raw and
    percent-decoded, each also percent-encoded and slash-escaped — covers
    those reversible forms. Components are ordered longest-first for
    ``redact_text_with_secrets``.
    """
    components: set[str] = set()
    for component in _raw_credential_components(url):
        components.update(_component_spellings(component))
    return tuple(sorted(components, key=len, reverse=True))


def _component_spellings(component: str) -> set[str]:
    """One component in its raw and decoded forms, each re-encoded both ways.

    A whitespace-only spelling is not a credential and replacing it would
    shred surrounding prose, so it is dropped.
    """
    forms = {component, unquote(component)}
    spellings = set(forms)
    for form in forms:
        spellings.add(quote(form, safe=_NOTHING_SAFE))
        spellings.add(form.replace("/", "\\/"))
    return {spelling for spelling in spellings if spelling.strip()}


def _raw_credential_components(url: str) -> set[str]:
    """The URL itself plus each structural component that may be the credential."""
    components = {url}
    try:
        parts = urlsplit(url)
    except ValueError:
        return components
    candidates = [parts.username, parts.password, parts.fragment]
    candidates.extend(parts.path.split("/"))
    candidates.extend(_query_credential_tokens(parts.query))
    components.update(part for part in candidates if part)
    return components


def _query_credential_tokens(query: str) -> list[str]:
    """The whole query plus every raw and decoded key/value token inside it.

    A response may echo one query value alone, so the raw pair split and the
    decoded ``parse_qsl`` views are all credential candidates.
    """
    if not query:
        return []
    tokens = [query]
    for pair in query.split("&"):
        tokens.append(pair)
        tokens.extend(pair.split("=", 1))
    for key, value in parse_qsl(query, keep_blank_values=True):
        tokens.extend((key, value))
    return tokens


def _redact_url_match(matched: str) -> str:
    """Redact one URL-shaped match, preserving surrounding prose delimiters."""
    url, trailing = _split_trailing_delimiters(matched)
    return redact_url(url) + trailing
