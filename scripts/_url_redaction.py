"""Single home for the URL credential parsing and rendering shared by the verifier scripts.

Both ``scripts/runtime_services.py`` (through ``_service_redaction``) and
``scripts/verify.py`` (through ``_verify_redaction``) must hide a configured
URL's password wherever a subprocess or library echoes it. This module owns the
one URL grammar, the credential extraction, and the partial rendering; each
script composes it with its own policy for which configured values are
sensitive.
"""

from __future__ import annotations

import re
from urllib.parse import unquote

_URL_PATTERN = re.compile(
    r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://(?P<authority>[^/?#]*)(?P<path>[^?#]*)(?P<tail>[?#].*)?$",
    re.DOTALL,
)


def _split_userinfo(authority: str) -> tuple[str | None, str | None, str]:
    """Split an authority into (username, password, hostinfo) exactly like URL consumers do."""
    userinfo, separator, hostinfo = authority.rpartition("@")
    if not separator:
        return None, None, hostinfo
    username, separator, password = userinfo.partition(":")
    return username, (password if separator else None), hostinfo


def _extract_query_password(pair: str) -> str | None:
    name, separator, raw_value = pair.partition("=")
    if not separator or not raw_value:
        return None
    return raw_value if name.casefold() == "password" else None


def _query_credentials(tail: str) -> list[str]:
    if not tail.startswith("?"):
        return []
    query = tail[1:].partition("#")[0]
    results: list[str] = []
    for pair in query.split("&"):
        pwd = _extract_query_password(pair)
        if pwd:
            results.append(pwd)
    return results


def _url_credentials(value: str) -> tuple[str, ...]:
    """Return the raw credentials a consumer would read from ``value``.

    Both the userinfo password and any ``password`` query parameter count, because redis-py and
    libpq accept either spelling. Parsing never requires a well-formed URL.
    """
    match = _URL_PATTERN.match(value)
    if match is None:
        return ()
    credentials: list[str] = []
    password = _split_userinfo(match.group("authority"))[1]
    if password:
        credentials.append(password)
    credentials.extend(_query_credentials(match.group("tail") or ""))
    return tuple(credentials)


def credential_forms(value: str) -> tuple[str, ...]:
    """Return every encoded or decoded credential spelling a tool might echo from ``value``."""
    raw_forms: list[str] = []
    for credential in _url_credentials(value):
        raw_forms.extend((credential, unquote(credential)))
    return tuple(dict.fromkeys(raw_forms))


def redacted_url(value: str) -> str:
    """Render a service URL with its credential and query hidden, or ``***`` when unparseable.

    The host, port, and path are copied verbatim, so an out-of-range or non-numeric port can
    never break redaction. Query strings are hidden because Redis and libpq accept passwords there.
    """
    match = _URL_PATTERN.match(value)
    if match is None:
        return "***"
    username, password, hostinfo = _split_userinfo(match.group("authority"))
    if username is None:
        credential = ""
    elif password is None:
        credential = f"{username}@"
    else:
        credential = f"{username}:***@"
    query = "?***" if match.group("tail") else ""
    return f"{match.group('scheme')}://{credential}{hostinfo}{match.group('path')}{query}"


def redact_url_occurrences(text: str, value: str) -> str:
    """Mask one sensitive URL and every spelling of its credential inside ``text``."""
    redacted = text.replace(value, redacted_url(value))
    for form in credential_forms(value):
        redacted = redacted.replace(form, "***")
    return redacted
