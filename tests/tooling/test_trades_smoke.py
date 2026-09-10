"""The live-safe smoke check: seven deterministic cases, wallet-free records, and exit codes."""

from __future__ import annotations

import ast
import importlib.util
import json
import re
import sys
from pathlib import Path
from types import ModuleType

import pytest
from tests.fakes import (
    FakeClock,
    FakeTradesServer,
    non_list_body,
    synthetic_wallet,
    terminal,
    throttled,
    timeout,
    trade_row,
)

from polymarket_insider_tracker.ingestor.trades_source import PAGE_LIMIT

SMOKE_PATH = Path(__file__).parents[2] / "scripts" / "trades_smoke.py"
VERIFY_PATH = Path(__file__).parents[2] / "scripts" / "verify.py"
URL = "https://trades.invalid/trades"
NOW = 1_788_983_720.0
T0 = int(NOW)
WALLET_PATTERN = re.compile(r"0x[0-9a-fA-F]{40}")


def _load(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolve postponed annotations through sys.modules
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def smoke() -> ModuleType:
    return _load(SMOKE_PATH, "trades_smoke_under_test")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(NOW)


@pytest.fixture
def server(clock: FakeClock) -> FakeTradesServer:
    return FakeTradesServer(clock=clock)


def _wallet_bearing_rows(count: int, *, newest: int = T0 - 1) -> list[dict[str, object]]:
    return [
        trade_row(timestamp=newest - offset, transaction=offset, wallet=offset)
        for offset in range(count)
    ]


async def _run(smoke: ModuleType, server: FakeTradesServer, clock: FakeClock) -> object:
    records = await smoke.run_smoke(
        server.client(),
        url=URL,
        coverage_modes=["all"],
        window_seconds=5,
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )
    assert len(records) == 1
    return records[0]


class TestSevenCases:
    async def test_valid_wallet_bearing_page_passes(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        for row in _wallet_bearing_rows(20):
            server.publish(row)
        row = trade_row(timestamp=T0 - 3, transaction=99, wallet=99)
        row["name"] = "Real Name"
        row["pseudonym"] = "Pseudo"
        row["bio"] = "A biography"
        row["profileImage"] = "https://img.invalid/p.png"
        server.publish(row)

        record = await _run(smoke, server, clock)

        assert record.case == "valid-wallet-bearing"
        assert record.passed is True
        assert record.reachable is True
        assert record.http_status == 200
        assert record.row_count == 21
        assert record.valid_rows == 21
        assert record.invalid_rows == 0
        assert record.missing_required_fields == {}
        assert record.newest_timestamp == T0 - 1
        assert record.oldest_timestamp == T0 - 20
        assert record.provider_lag_seconds == pytest.approx(1.0)
        assert record.in_window_rows == 6
        assert record.page_saturated is False
        assert record.cache_control == "public, max-age=300"
        assert record.cf_cache_status == "MISS"
        assert record.age is None
        assert record.response_sha256 == server.body_sha256(server.requests[-1].params)
        assert record.retained_wallet_identifiers is False
        assert record.endpoint == URL
        assert record.coverage == "all"
        assert smoke.exit_code_for([record]) == smoke.EXIT_PASSED

    async def test_valid_empty_page_passes_with_reachability(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        record = await _run(smoke, server, clock)

        assert record.case == "valid-empty"
        assert record.passed is True
        assert record.reachable is True
        assert record.newest_timestamp is None
        assert record.provider_lag_seconds is None
        assert record.error is None
        assert smoke.exit_code_for([record]) == smoke.EXIT_PASSED

    async def test_throttled_response_is_honoured_without_spinning(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(throttled("2"), times=10)

        record = await _run(smoke, server, clock)

        assert record.case == "throttled"
        assert record.passed is True
        assert record.reachable is False
        assert record.http_status == 429
        assert record.transient is True
        assert record.retry_after_honoured is True
        assert len(server.requests) == smoke.SMOKE_MAX_RETRIES + 1
        assert clock.sleeps == [2.0] * smoke.SMOKE_MAX_RETRIES
        assert smoke.exit_code_for([record]) == smoke.EXIT_FAILED

    async def test_timeout_is_reported_without_a_status(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(timeout(), times=10)

        record = await _run(smoke, server, clock)

        assert record.case == "timeout"
        assert record.passed is True
        assert record.timeout is True
        assert record.http_status is None
        assert record.reachable is False
        assert smoke.exit_code_for([record]) == smoke.EXIT_FAILED

    async def test_malformed_row_is_counted_without_invention(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        for row in _wallet_bearing_rows(30):
            server.publish(row)
        malformed = trade_row(timestamp=T0 - 2, transaction=77, wallet=77)
        del malformed["proxyWallet"]
        server.publish(malformed)

        record = await _run(smoke, server, clock)

        assert record.case == "malformed-row"
        assert record.passed is True
        assert record.valid_rows == 30
        assert record.invalid_rows == 1
        assert record.missing_required_fields == {"proxyWallet": 1}
        assert record.row_count == 31
        assert smoke.exit_code_for([record]) == smoke.EXIT_PASSED

    async def test_malformed_share_above_five_percent_fails(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        for row in _wallet_bearing_rows(5):
            server.publish(row)
        malformed = trade_row(timestamp=T0 - 2, transaction=77, wallet=77)
        del malformed["price"]
        server.publish(malformed)

        record = await _run(smoke, server, clock)

        assert record.case == "malformed-row"
        assert record.passed is False
        assert record.missing_required_fields == {"price": 1}
        assert smoke.exit_code_for([record]) == smoke.EXIT_FAILED

    async def test_incompatible_schema_fails(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(non_list_body())

        record = await _run(smoke, server, clock)

        assert record.case == "incompatible-schema"
        assert record.passed is False
        assert record.http_status == 200
        assert record.error is not None
        assert "incompatible-schema" in record.error
        assert smoke.exit_code_for([record]) == smoke.EXIT_FAILED

    async def test_terminal_status_is_reported_as_a_failure(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(terminal(401))

        record = await _run(smoke, server, clock)

        assert record.case == "incompatible-schema"
        assert record.passed is False
        assert record.http_status == 401
        assert record.error is not None
        assert "HTTP 401" in record.error
        assert "?" not in record.error

    async def test_possible_page_saturation_passes_with_a_warning(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        for offset in range(PAGE_LIMIT):
            server.publish(trade_row(timestamp=T0 - 1 - offset // 2500, transaction=offset))

        record = await _run(smoke, server, clock)

        assert record.case == "possible-page-saturation"
        assert record.passed is True
        assert record.page_saturated is True
        assert record.row_count == PAGE_LIMIT
        assert record.oldest_timestamp == T0 - 4
        assert record.warning is not None
        assert "not" in record.warning and "loss" in record.warning
        assert smoke.exit_code_for([record]) == smoke.EXIT_PASSED

    async def test_saturation_does_not_override_the_row_quality_floor(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        for offset in range(PAGE_LIMIT):
            row = trade_row(timestamp=T0 - 1, transaction=offset)
            if offset < PAGE_LIMIT // 10:
                del row["price"]
            server.publish(row)

        record = await _run(smoke, server, clock)

        assert record.case == "possible-page-saturation"
        assert record.page_saturated is True
        assert record.passed is False
        assert record.invalid_rows == PAGE_LIMIT // 10
        assert smoke.exit_code_for([record]) == smoke.EXIT_FAILED

    async def test_a_full_page_reaching_the_window_start_is_not_saturation(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        for offset in range(PAGE_LIMIT):
            server.publish(trade_row(timestamp=T0 - 1 - offset, transaction=offset))

        record = await _run(smoke, server, clock)

        assert record.case == "valid-wallet-bearing"
        assert record.page_saturated is True
        assert record.passed is True


class TestRecordPrivacy:
    async def test_serialized_record_retains_no_wallet_identifier_or_profile_value(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        row = trade_row(timestamp=T0 - 1, transaction=1, wallet=5)
        row["name"] = "Real Name"
        row["pseudonym"] = "Pseudo"
        row["bio"] = "A biography"
        row["profileImage"] = "https://img.invalid/p.png"
        server.publish(row)

        record = await _run(smoke, server, clock)
        rendered = record.to_json()
        payload = json.loads(rendered)

        assert WALLET_PATTERN.search(rendered) is None
        assert synthetic_wallet(5) not in rendered
        for forbidden in ("Real Name", "Pseudo", "A biography", "img.invalid", "transactionHash"):
            assert forbidden not in rendered
        assert payload["retained_wallet_identifiers"] is False
        assert payload["response_sha256"] == server.body_sha256(server.requests[-1].params)
        assert set(payload) >= {
            "endpoint",
            "captured_at",
            "case",
            "http_status",
            "row_count",
            "valid_rows",
            "invalid_rows",
            "missing_required_fields",
            "newest_timestamp",
            "oldest_timestamp",
            "provider_lag_seconds",
            "page_saturated",
            "cache_control",
            "cf_cache_status",
            "age",
            "response_sha256",
            "retained_wallet_identifiers",
            "passed",
        }

    async def test_both_coverage_modes_produce_one_record_each(
        self, smoke: ModuleType, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.publish(trade_row(timestamp=T0 - 1, transaction=1), taker=True)
        server.publish(trade_row(timestamp=T0 - 1, transaction=1, wallet=2), taker=False)

        records = await smoke.run_smoke(
            server.client(),
            url=URL,
            coverage_modes=["all", "taker-only"],
            window_seconds=5,
            clock=clock,
            sleeper=clock.sleep,
            random_source=lambda: 0.0,
        )

        assert [record.coverage for record in records] == ["all", "taker-only"]
        assert [record.row_count for record in records] == [2, 1]
        assert [request.params["takerOnly"] for request in server.requests] == ["false", "true"]
        assert len({request.url for request in server.requests}) == 2


class TestCommandLine:
    def test_live_is_required_and_explained(
        self, smoke: ModuleType, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert smoke.main([]) == smoke.EXIT_USAGE
        captured = capsys.readouterr()
        assert "--live" in captured.err

    def test_invalid_options_are_usage_errors(self, smoke: ModuleType) -> None:
        assert smoke.main(["--live", "--window-seconds", "0"]) == smoke.EXIT_USAGE
        assert smoke.main(["--live", "--coverage", "makers"]) == smoke.EXIT_USAGE
        assert smoke.main(["--live", "--bogus"]) == smoke.EXIT_USAGE

    def test_invalid_trades_url_is_a_usage_error(
        self, smoke: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("POLYMARKET_TRADES_URL", "wss://trades.invalid/trades")

        assert smoke.main(["--live"]) == smoke.EXIT_USAGE

    def test_live_run_emits_one_json_object_per_record(
        self,
        smoke: ModuleType,
        server: FakeTradesServer,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setenv("POLYMARKET_TRADES_URL", URL)
        server.publish(trade_row(timestamp=int(NOW) - 1, transaction=1))

        code = smoke.main(["--live", "--coverage", "both", "--json"], client_factory=server.client)

        out = capsys.readouterr().out
        payloads = [json.loads(line) for line in out.strip().splitlines()]
        assert code == smoke.EXIT_PASSED
        assert [payload["coverage"] for payload in payloads] == ["all", "taker-only"]
        assert all(payload["retained_wallet_identifiers"] is False for payload in payloads)

    def test_live_run_exit_code_follows_the_records(
        self,
        smoke: ModuleType,
        server: FakeTradesServer,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("POLYMARKET_TRADES_URL", URL)
        server.fail_next(non_list_body())

        assert (
            smoke.main(["--live", "--coverage", "all"], client_factory=server.client)
            == smoke.EXIT_FAILED
        )

    def test_default_url_is_the_documented_endpoint(
        self, smoke: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("POLYMARKET_TRADES_URL", raising=False)

        assert smoke.resolve_trades_url() == "https://data-api.polymarket.com/trades"


class TestIsolation:
    def test_script_never_touches_redis_alerts_pipeline_or_the_legacy_setting(self) -> None:
        source = SMOKE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported = {
            name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import | ast.ImportFrom)
            for name in [alias.name for alias in node.names]
            + ([node.module] if isinstance(node, ast.ImportFrom) and node.module else [])
        }

        assert not any(name.startswith("redis") or name == "fakeredis" for name in imported)
        forbidden_modules = ("alerter", "pipeline", "storage", "observation_boundary")
        assert not any(any(part in name for part in forbidden_modules) for name in imported)
        assert "POLYMARKET_WS_URL" not in source

    def test_smoke_is_not_part_of_any_verifier_profile(self) -> None:
        verify = _load(VERIFY_PATH, "verify_under_smoke_test")

        assert not any("trades_smoke" in gate.command_text for gate in verify.GATES.values())

    def test_default_endpoint_is_the_documented_query(self, smoke: ModuleType) -> None:
        assert smoke.DEFAULT_TRADES_URL == "https://data-api.polymarket.com/trades"
