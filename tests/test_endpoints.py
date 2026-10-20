"""Behavioural pins for the official v1/v2 trades endpoint rules (mutation step 1)."""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.endpoints import (
    OFFICIAL_V2_TRADES_URL,
    is_official_v1_trades_url,
    official_v1_retirement_message,
    reject_v1_trades_url,
)

EXPECTED_RETIREMENT_MESSAGE = (
    "POLYMARKET_TRADES_URL targets the official v1 trades endpoint, which Polymarket retires "
    "on 2026-10-24. Set it to https://data-api.polymarket.com/v2/trades instead. For the official "
    "v1/v2 pair with the same coverage mode the saved checkpoint and identity window carry "
    "over automatically, so this URL change is the whole migration; a custom proxy URL keeps "
    "its own source identity and needs an explicit operator migration."
)


@pytest.mark.parametrize(
    "url",
    [
        "https://data-api.polymarket.com/trades",
        "https://data-api.polymarket.com/trades/",
        "https://data-api.polymarket.com./trades",
        "https://data-api.polymarket.com/trades?limit=10",
        "http://data-api.polymarket.com/trades",
        "https://user:secret@data-api.polymarket.com:443/trades",
    ],
)
def test_route_level_v1_spellings_are_the_retiring_endpoint(url: str) -> None:
    assert is_official_v1_trades_url(url)


@pytest.mark.parametrize(
    "url",
    [
        OFFICIAL_V2_TRADES_URL,
        "https://data-api.polymarket.com:8443/trades",
        "https://other.example.com/trades",
        "https://.data-api.polymarket.com/trades",
        "https://data-api.polymarket.com/other",
        "https://data-api.polymarket.com/tradesX",
        "https://data-api.polymarket.com:notaport/trades",
        "/trades",
    ],
)
def test_other_routes_and_origins_are_not_the_retiring_endpoint(url: str) -> None:
    assert not is_official_v1_trades_url(url)


def test_retirement_message_is_the_exact_actionable_text() -> None:
    assert official_v1_retirement_message() == EXPECTED_RETIREMENT_MESSAGE


def test_rejecting_v1_raises_the_retirement_message_and_passes_others_through() -> None:
    with pytest.raises(ValueError) as caught:
        reject_v1_trades_url("https://data-api.polymarket.com/trades?key=s3cret")
    assert str(caught.value) == EXPECTED_RETIREMENT_MESSAGE
    assert reject_v1_trades_url(OFFICIAL_V2_TRADES_URL) == OFFICIAL_V2_TRADES_URL
