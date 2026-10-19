"""The approved 2026-10-18 v2 amendment, item 1: endpoint default and the v1 rejection rule.

With ``POLYMARKET_TRADES_URL`` unset the default is the official v2 endpoint. An explicitly
configured official v1 URL fails validation with an actionable migration message; checkpoint
continuity for the official pair is automatic, so the migration step is only the URL itself.
Custom HTTP(S) proxy URLs stay accepted unchanged and the environment variable name is
unchanged. No fallback and no credentials are added.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.endpoints import (
    OFFICIAL_V1_TRADES_URL,
    OFFICIAL_V2_TRADES_URL,
)

SECRET = "MIGRATION_SECRET_R24"


def test_default_is_the_official_v2_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the variable unset, the supported source is the official v2 endpoint."""
    monkeypatch.delenv("POLYMARKET_TRADES_URL", raising=False)

    settings = PolymarketSettings()

    assert settings.trades_url == OFFICIAL_V2_TRADES_URL


def test_the_environment_variable_name_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same variable still feeds the same field; only the default changed."""
    url = "https://proxy.example.com/v2/trades"
    monkeypatch.setenv("POLYMARKET_TRADES_URL", url)

    settings = PolymarketSettings()

    assert settings.trades_url == url


def test_explicit_official_v1_url_is_rejected_with_the_migration_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Configuring the official v1 endpoint fails with the v2 replacement and migration step."""
    monkeypatch.setenv("POLYMARKET_TRADES_URL", OFFICIAL_V1_TRADES_URL)

    with pytest.raises(ValidationError) as exc_info:
        PolymarketSettings()

    message = str(exc_info.value)
    assert OFFICIAL_V2_TRADES_URL in message
    assert "2026-10-24" in message
    assert "checkpoint" in message
    assert "migration" in message.lower()


@pytest.mark.parametrize(
    "configured",
    [
        f"https://user:{SECRET}@data-api.polymarket.com/trades?apikey={SECRET}",
        f"https://data-api.polymarket.com/trades/?x={SECRET}",
        f"https://data-api.polymarket.com/trades?limit={SECRET}",
        f"http://data-api.polymarket.com/trades#{SECRET}",
        f"https://data-api.polymarket.com:443/trades#{SECRET}",
        f"http://data-api.polymarket.com:80/trades?x={SECRET}",
        f"https://DATA-API.POLYMARKET.COM/trades?x={SECRET}",
    ],
    ids=[
        "credentials-and-query",
        "trailing-slash",
        "query",
        "plain-http",
        "explicit-443",
        "explicit-80",
        "upper-case-host",
    ],
)
def test_v1_route_variants_are_rejected_without_echoing_the_value(
    monkeypatch: pytest.MonkeyPatch, configured: str
) -> None:
    """Every shape that targets the official v1 route is rejected, and nothing is echoed."""
    monkeypatch.setenv("POLYMARKET_TRADES_URL", configured)

    with pytest.raises(ValidationError) as exc_info:
        PolymarketSettings()

    message = str(exc_info.value)
    assert SECRET not in message
    assert configured not in message
    assert OFFICIAL_V2_TRADES_URL in message


@pytest.mark.parametrize(
    "custom",
    [
        "https://proxy.example.com/v2/trades",
        "https://proxy.example.com/trades",
        f"https://user:{SECRET}@proxy.example.com/trades?apikey={SECRET}",
        "http://localhost:8080/trades",
        "https://data-api.polymarket.com:8443/trades",
    ],
    ids=["v2-path-proxy", "v1-path-proxy", "credentials-and-query", "loopback", "distinct-port"],
)
def test_custom_proxy_urls_remain_accepted_unchanged(
    monkeypatch: pytest.MonkeyPatch, custom: str
) -> None:
    """Only the official v1 route is rejected; every other HTTP(S) URL validates as before."""
    monkeypatch.setenv("POLYMARKET_TRADES_URL", custom)

    settings = PolymarketSettings()

    assert settings.trades_url == custom
