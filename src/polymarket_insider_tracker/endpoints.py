"""Single source of truth for the official Polymarket public trades endpoints.

``OFFICIAL_V1_TRADES_URL`` is the retiring official v1 trades query and ``OFFICIAL_V2_TRADES_URL``
is its documented successor (now the configuration default). They are declared here once so the
configuration default, the v1-rejection rule, the live smoke check, and the logical source
namespace all derive from the same literals. This module has no intra-package imports so it is
safe to import from ``config`` at module load.
"""

from __future__ import annotations

from urllib.parse import urlsplit

OFFICIAL_V1_TRADES_URL = "https://data-api.polymarket.com/trades"
OFFICIAL_V2_TRADES_URL = "https://data-api.polymarket.com/v2/trades"

V1_RETIREMENT_DATE = "2026-10-24"

_V1_PARTS = urlsplit(OFFICIAL_V1_TRADES_URL)
_V2_PARTS = urlsplit(OFFICIAL_V2_TRADES_URL)


def is_official_v1_trades_url(url: str) -> bool:
    """Whether a configured URL targets the official v1 trades route (its host and path).

    The comparison is route-level, not string-exact: a trailing slash, a query string, a plain
    ``http`` scheme, or userinfo credentials all still name the retiring endpoint and must not
    slip past validation into a post-retirement outage. The host is compared dot-normalized and
    the port must be the default one, so a distinct origin on the same hostname stays accepted.
    """
    parts = urlsplit(url)
    return (
        parts.port in (None, _V1_PARTS.port)
        and (parts.hostname or "").rstrip(".") == (_V1_PARTS.hostname or "").rstrip(".")
        and parts.path.rstrip("/") == _V1_PARTS.path.rstrip("/")
    )


def is_v2_trades_url(url: str) -> bool:
    """Whether a configured URL mounts the documented v2 trades route (official or a proxy).

    This mirrors the acquisition seam's source selection: the official v2 endpoint and a proxy
    mounted at its path speak the v2 envelope; every other URL keeps the v1 list body.
    """
    parts = urlsplit(url)
    return parts.path.rstrip("/").endswith(_V2_PARTS.path)


def official_v1_retirement_message() -> str:
    """The actionable, value-free text raised when configuration names the retiring endpoint."""
    return (
        "POLYMARKET_TRADES_URL targets the official v1 trades endpoint, which Polymarket retires "
        f"on {V1_RETIREMENT_DATE}. Set it to {OFFICIAL_V2_TRADES_URL} instead. For the official "
        "v1/v2 pair with the same coverage mode the saved checkpoint and identity window carry "
        "over automatically, so this URL change is the whole migration; a custom proxy URL keeps "
        "its own source identity and needs an explicit operator migration."
    )


def reject_v1_trades_url(value: str) -> str:
    """Config validator: an explicitly configured official v1 URL fails validation.

    The message is built only from constants, so no configured credential or query string can
    ever reach it.
    """
    if is_official_v1_trades_url(value):
        raise ValueError(official_v1_retirement_message())
    return value


__all__ = [
    "OFFICIAL_V1_TRADES_URL",
    "OFFICIAL_V2_TRADES_URL",
    "V1_RETIREMENT_DATE",
    "is_official_v1_trades_url",
    "is_v2_trades_url",
    "official_v1_retirement_message",
    "reject_v1_trades_url",
]
