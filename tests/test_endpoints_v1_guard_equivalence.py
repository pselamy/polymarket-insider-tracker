"""The v1 trades URL guard accepts and rejects exactly what it did before mutation step 1.

The verdicts below were recorded by running ``is_official_v1_trades_url`` as it exists on
``origin/main`` (3d5ab59) over each URL, so this table passes unchanged on both main and the
mutation-hardened implementation. The guard is security-relevant: a wrong accept lets the retiring
v1 endpoint slip past configuration validation, a wrong reject blocks a legitimate origin.
"""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.endpoints import is_official_v1_trades_url

MAIN_VERDICTS = [
    ("https://data-api.polymarket.com/trades", True),
    ("https://data-api.polymarket.com/trades/", True),
    ("https://data-api.polymarket.com/trades//", True),
    ("https://data-api.polymarket.com./trades", True),
    ("https://data-api.polymarket.com../trades", True),
    ("https://data-api.polymarket.com.../trades/", True),
    ("https://DATA-API.Polymarket.COM/trades", True),
    ("HTTPS://DATA-API.POLYMARKET.COM/trades", True),
    ("https://data-api.polymarket.com/Trades", False),
    ("https://user@data-api.polymarket.com/trades", True),
    ("https://user:secret@data-api.polymarket.com/trades", True),
    ("https://user:secret@data-api.polymarket.com:443/trades", True),
    ("http://data-api.polymarket.com:80/trades", True),
    ("http://data-api.polymarket.com:443/trades", False),
    ("https://data-api.polymarket.com:80/trades", False),
    ("https://data-api.polymarket.com:8443/trades", False),
    ("https://data-api.polymarket.com:/trades", True),
    ("https://data-api.polymarket.com:notaport/trades", False),
    ("https://data-api.polymarket.com:99999/trades", False),
    ("ftp://data-api.polymarket.com/trades", True),
    ("ftp://data-api.polymarket.com:21/trades", False),
    ("data-api.polymarket.com/trades", False),
    ("//data-api.polymarket.com/trades", True),
    ("/trades", False),
    ("", False),
    ("https:///trades", False),
    ("https://", False),
    ("https://.data-api.polymarket.com/trades", False),
    ("https://data-api.polymarket.com/trades?limit=1#frag", True),
    ("https://data-api.polymarket.com/v2/trades", False),
    ("https://data-api.polymarket.com/tradesX", False),
    ("https://data-api.polymarket.com/", False),
    ("https://data-api.polymarket.com", False),
    ("https://sub.data-api.polymarket.com/trades", False),
    ("https://data-api.polymarket.com.evil.example/trades", False),
    ("https://[::1]/trades", False),
    ("https://[::1/trades", False),
    ("https://data-api.polymarket.com\\trades", False),
    ("https://evil.example@data-api.polymarket.com/trades", True),
    ("https://data-api.polymarket.com@evil.example/trades", False),
]


@pytest.mark.parametrize(("url", "expected"), MAIN_VERDICTS)
def test_guard_verdict_matches_origin_main(url: str, expected: bool) -> None:
    assert is_official_v1_trades_url(url) is expected
