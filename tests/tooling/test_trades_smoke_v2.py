"""The live-safe smoke check against the v2 envelope: shared parsing, same seven-case contract.

The official v2 endpoint (the smoke's default URL) is probed through the same record machinery as
v1; only the transport body differs, so these tests pin the envelope handling: rows decode through
the shared ``trade_row_v2`` decoder, envelope failures are terminal, and the documented 1,000-row
v2 page limit drives the saturation case. Every wallet and transaction is synthetic; no test
contacts the real endpoint.
"""

from __future__ import annotations

import asyncio
import json
import re
from types import ModuleType

import httpx
import pytest
from scripts.trades_smoke import SmokeRecord
from tests.fakes import FakeClock, synthetic_wallet, trade_row, v2_row
from tests.tooling.module_loading import load_registered_module
from tests.tooling.test_trades_smoke import NOW, SMOKE_PATH, T0

from polymarket_insider_tracker.endpoints import OFFICIAL_V2_TRADES_URL
from polymarket_insider_tracker.ingestor.v2_page import V2_PAGE_LIMIT


class FakeV2Server:
    """The documented ``{data, pagination}`` envelope behind ``httpx.MockTransport``."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.ledger: list[tuple[dict[str, object], bool]] = []
        self.requests: list[dict[str, str]] = []
        self.faults: list[httpx.Response] = []

    def publish(self, row: dict[str, object], *, taker: bool = True) -> None:
        self.ledger.append((row, taker))

    def fail_next(self, response: httpx.Response, times: int = 1) -> None:
        self.faults.extend([response] * times)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self._handle))

    def _handle(self, request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params.items())
        self.requests.append(params)
        if self.faults:
            return self.faults.pop(0)
        # taker_only=true is the provider's taker subset; taker_only=false is every observation.
        taker_only = params.get("taker_only") == "true"
        rows = [row for row, taker in self.ledger if taker or not taker_only]
        body = {"data": rows, "pagination": {"next_cursor": None, "has_more": False}}
        return httpx.Response(200, json=body)


@pytest.fixture
def smoke() -> ModuleType:
    return load_registered_module("trades_smoke_v2_under_test", SMOKE_PATH)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(NOW)


@pytest.fixture
def server(clock: FakeClock) -> FakeV2Server:
    return FakeV2Server(clock)


async def _run(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock, *, url: str = OFFICIAL_V2_TRADES_URL
) -> SmokeRecord:
    records = await smoke.run_smoke(
        server.client(),
        url=url,
        coverage_modes=["all"],
        window_seconds=5,
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )
    assert len(records) == 1
    record: SmokeRecord = records[0]
    return record


def test_smoke_default_url_is_the_official_v2_endpoint(smoke: ModuleType) -> None:
    assert smoke.DEFAULT_TRADES_URL == OFFICIAL_V2_TRADES_URL


async def test_v2_route_variants_and_proxies_speak_the_v2_envelope(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock
) -> None:
    """A trailing slash or a proxy mounted at the v2 route still selects the v2 path.

    Under the v1 path an envelope body would be a non-list terminal failure, so a passing
    wallet-bearing record proves the v2 selection fired.
    """
    for url, count in (
        (OFFICIAL_V2_TRADES_URL + "/", 1),
        ("https://proxy.example.com/v2/trades", 2),
    ):
        server.publish(v2_row(trade_row(timestamp=T0 - 1, transaction=1)))

        record = await _run(smoke, server, clock, url=url)

        assert record.case == "valid-wallet-bearing"
        assert record.row_count == count
        assert record.passed is True


async def test_v2_wallet_bearing_page_passes(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock
) -> None:
    """Rows decode through the shared v2 decoder and count exactly as the v1 path does."""
    for offset in range(20):
        server.publish(v2_row(trade_row(timestamp=T0 - 1 - offset, transaction=offset)))

    record = await _run(smoke, server, clock)

    assert record.case == "valid-wallet-bearing"
    assert record.passed is True
    assert record.reachable is True
    assert record.http_status == 200
    assert record.row_count == 20
    assert record.valid_rows == 20
    assert record.invalid_rows == 0
    assert record.newest_timestamp == T0 - 1
    assert record.in_window_rows == 5
    assert record.response_sha256 is not None
    assert record.retained_wallet_identifiers is False


async def test_v2_empty_page_passes(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock
) -> None:
    record = await _run(smoke, server, clock)

    assert record.case == "valid-empty"
    assert record.passed is True
    assert record.newest_timestamp is None
    assert record.error is None


async def test_v2_malformed_row_is_counted_without_invention(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock
) -> None:
    """A v2 row missing ``proxy_wallet`` reaches the shared parser and is counted, not invented."""
    for offset in range(30):
        server.publish(v2_row(trade_row(timestamp=T0 - 1, transaction=offset)))
    malformed = v2_row(trade_row(timestamp=T0 - 1, transaction=77))
    del malformed["proxy_wallet"]
    server.publish(malformed)

    record = await _run(smoke, server, clock)

    assert record.case == "malformed-row"
    assert record.passed is True
    assert record.row_count == 31
    assert record.valid_rows == 30
    assert record.invalid_rows == 1
    # The shared decoder translates v2 names onto the v1 parser's contract before parsing,
    # so the reported missing-field name is the v1 spelling.
    assert record.missing_required_fields == {"proxyWallet": 1}


async def test_v2_envelope_failure_is_terminal(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock
) -> None:
    """A body that is not the documented envelope is incompatible-schema, never retried."""
    server.fail_next(httpx.Response(200, json={"rows": []}))

    record = await _run(smoke, server, clock)

    assert record.case == "incompatible-schema"
    assert record.passed is False
    assert record.http_status == 200
    assert record.error is not None
    assert "incompatible-schema" in record.error
    assert len(server.requests) == 1


async def test_v2_throttled_response_is_honoured_without_spinning(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock
) -> None:
    server.fail_next(httpx.Response(429, headers={"Retry-After": "2"}), times=10)

    record = await _run(smoke, server, clock)

    assert record.case == "throttled"
    assert record.passed is True
    assert record.http_status == 429
    assert record.transient is True
    assert record.retry_after_honoured is True
    assert len(server.requests) == smoke.SMOKE_MAX_RETRIES + 1
    assert clock.sleeps == [2.0] * smoke.SMOKE_MAX_RETRIES


async def test_v2_full_page_saturates_at_the_v2_page_limit(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock
) -> None:
    """A full v2 page (1,000 rows) newer than the window saturates — not v1's 10,000."""
    for offset in range(V2_PAGE_LIMIT):
        server.publish(v2_row(trade_row(timestamp=T0 - 1, transaction=offset)))

    record = await _run(smoke, server, clock)

    assert record.case == "possible-page-saturation"
    assert record.passed is True
    assert record.page_saturated is True
    assert record.row_count == V2_PAGE_LIMIT
    assert record.warning is not None
    assert "not" in record.warning and "loss" in record.warning


def test_v2_request_params_are_the_documented_ones(
    smoke: ModuleType, server: FakeV2Server, clock: FakeClock
) -> None:
    """First page: ``limit`` plus explicit ``taker_only``/``start``/``end``; no ``offset``."""
    asyncio.run(_run(smoke, server, clock))
    params = server.requests[0]

    assert params["limit"] == str(V2_PAGE_LIMIT)
    assert params["taker_only"] == "false"
    assert set(params) == {"limit", "taker_only", "start", "end"}


def test_json_output_for_a_v2_run_stays_wallet_free(
    smoke: ModuleType,
    server: FakeV2Server,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    row = v2_row(trade_row(timestamp=T0 - 1, transaction=1, wallet=5))
    row["name"] = "Real Name"
    server.publish(row)
    monkeypatch.setenv("POLYMARKET_TRADES_URL", OFFICIAL_V2_TRADES_URL)

    code = smoke.main(["--live", "--coverage", "all", "--json"], client_factory=server.client)

    out = capsys.readouterr().out
    payload = json.loads(out.strip())
    assert code == 0
    assert payload["coverage"] == "all"
    assert re.search(r"0x[0-9a-fA-F]{40}", out) is None
    assert synthetic_wallet(5) not in out
    assert "Real Name" not in out


def test_configured_official_v1_url_is_a_usage_error(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The smoke refuses a configured official v1 route exactly as configuration validation does."""
    from polymarket_insider_tracker.endpoints import OFFICIAL_V1_TRADES_URL

    monkeypatch.setenv("POLYMARKET_TRADES_URL", OFFICIAL_V1_TRADES_URL)

    code = smoke.main(["--live"])

    out_err = capsys.readouterr().err
    assert code == smoke.EXIT_USAGE
    assert "2026-10-24" in out_err
    assert OFFICIAL_V2_TRADES_URL in out_err
    assert "checkpoint" in out_err
