"""Response evidence on the combined v2 page: real cache headers and a raw-body digest.

The v2 path combines many cursor pages into one v1-shaped ``TradesPage``. This module pins the
provider-cache evidence that survives that combination: the cache headers of the last walked page
and a body digest over the raw response bytes of every page, in walk order.
"""

from __future__ import annotations

import hashlib

from polymarket_insider_tracker.ingestor.acquisition import V2TradesSource
from tests.fakes import FakeClock, FakeV2TradesServer, trade_row, v2_row

NOW = 1_788_983_720.0
T0 = int(NOW)
V2_TRADES_URL = "https://data-api.polymarket.com/v2/trades"


async def test_combined_v2_page_carries_last_page_headers_and_raw_body_digest() -> None:
    clock = FakeClock(NOW)
    server = FakeV2TradesServer(clock=clock, page_size=1)
    server.publish(v2_row(trade_row(timestamp=T0, transaction=2)))
    server.publish(v2_row(trade_row(timestamp=T0 - 1, transaction=1)))
    source = V2TradesSource(
        server.client(),
        url=V2_TRADES_URL,
        coverage="all",
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )

    page = await source.fetch_primary(boundary_time=None, horizon_seconds=600)

    assert len(page.rows) == 2
    assert len(server.served_bodies) == 2
    assert page.headers.cache_control == "public, max-age=300"
    assert page.headers.cf_cache_status == "MISS"
    # The fake stamps each served page's ``age`` with its 1-based position, so the combined page's
    # age proves the headers came from the *last* page, not the first.
    assert page.headers.age == "2"
    assert page.body_sha256 == hashlib.sha256(b"".join(server.served_bodies)).hexdigest()
