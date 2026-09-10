"""Acquisition requests, retry classification, backoff, and accounting for the trades source."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from polymarket_insider_tracker import __version__
from polymarket_insider_tracker.ingestor.trades_source import (
    BACKOFF_CAP_SECONDS,
    MAX_RETRIES,
    PAGE_LIMIT,
    RECOVERY_OFFSET,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_AFTER_CAP_SECONDS,
    RequestAttempt,
    TradesPage,
    TradesRequest,
    TradesSourceClient,
    TradesSourceError,
    TradesTerminalError,
    TradesTransientError,
    parse_retry_after,
    start_for,
)
from tests.fakes import (
    FakeClock,
    FakeTradesServer,
    invalid_json,
    non_list_body,
    server_error,
    terminal,
    throttled,
    timeout,
    trade_row,
)

URL = "https://trades.invalid/trades"
NOW = 1_788_983_720.4


def _client(
    server: FakeTradesServer,
    clock: FakeClock,
    *,
    coverage: str = "all",
    random_value: float = 1.0,
    max_retries: int = MAX_RETRIES,
) -> TradesSourceClient:
    return TradesSourceClient(
        server.client(),
        url=URL,
        coverage=coverage,
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: random_value,
        max_retries=max_retries,
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(NOW)


@pytest.fixture
def server(clock: FakeClock) -> FakeTradesServer:
    return FakeTradesServer(clock=clock)


def _query(server: FakeTradesServer, index: int = -1) -> dict[str, str]:
    parsed = parse_qs(urlsplit(server.requests[index].url).query)
    return {key: values[0] for key, values in parsed.items()}


class TestRequestShape:
    async def test_primary_request_sends_exactly_the_documented_parameters(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        page = await _client(server, clock).fetch_primary(
            boundary_time=1_788_983_000, horizon_seconds=600
        )

        recorded = server.requests[-1]
        assert urlsplit(recorded.url)._replace(query="").geturl() == URL
        assert _query(server) == {
            "limit": str(PAGE_LIMIT),
            "offset": "0",
            "takerOnly": "false",
            "start": str(1_788_983_000 - 600),
            "end": str(int(NOW)),
        }
        assert recorded.headers["accept"] == "application/json"
        assert recorded.headers["user-agent"] == f"polymarket-insider-tracker/{__version__}"
        assert "authorization" not in recorded.headers
        assert "cookie" not in recorded.headers
        assert recorded.timeout == {
            "connect": REQUEST_TIMEOUT_SECONDS,
            "read": REQUEST_TIMEOUT_SECONDS,
            "write": REQUEST_TIMEOUT_SECONDS,
            "pool": REQUEST_TIMEOUT_SECONDS,
        }
        assert page.request == TradesRequest(
            offset=0, taker_only=False, start=1_788_983_000 - 600, end=int(NOW)
        )

    async def test_taker_only_coverage_is_sent_explicitly(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        await _client(server, clock, coverage="taker-only").fetch_primary(
            boundary_time=None, horizon_seconds=600
        )

        assert _query(server)["takerOnly"] == "true"
        assert _query(server)["start"] == "0"

    def test_start_is_the_boundary_minus_the_horizon_or_zero(self) -> None:
        assert start_for(None, 600) == 0
        assert start_for(1_000, 600) == 400
        assert start_for(100, 600) == 0

    async def test_end_is_strictly_increasing_even_when_the_clock_stalls(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        client = _client(server, clock)

        first = await client.fetch_primary(boundary_time=None, horizon_seconds=600)
        second = await client.fetch_primary(boundary_time=None, horizon_seconds=600)
        clock.advance(0.5)
        third = await client.fetch_primary(boundary_time=None, horizon_seconds=600)
        clock.advance(100)
        fourth = await client.fetch_primary(boundary_time=None, horizon_seconds=600)

        ends = [page.request.end for page in (first, second, third, fourth)]
        assert ends[0] == int(NOW)
        assert ends[1] == int(NOW) + 1
        assert ends[2] == int(NOW) + 2
        assert ends[3] == int(NOW) + 100 + 2
        assert [round(sleep, 3) for sleep in clock.sleeps] == [0.6, 0.5]
        assert client.last_request_end == ends[3]
        assert len({request.url for request in server.requests}) == 4

    async def test_restored_last_end_keeps_the_cache_key_moving_after_restart(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        client = _client(server, clock)
        client.restore_last_end(int(NOW) + 5)

        page = await client.fetch_primary(boundary_time=None, horizon_seconds=600)

        assert page.request.end == int(NOW) + 6
        assert clock.sleeps == [pytest.approx(int(NOW) + 6 - NOW)]

    async def test_recovery_request_reuses_the_window_at_the_documented_offset(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        client = _client(server, clock, coverage="taker-only")
        primary = await client.fetch_primary(boundary_time=5_000, horizon_seconds=600)

        recovery = await client.fetch_recovery(primary.request)

        assert _query(server) == {
            "limit": str(PAGE_LIMIT),
            "offset": str(RECOVERY_OFFSET),
            "takerOnly": "true",
            "start": "4400",
            "end": str(int(NOW)),
        }
        assert recovery.request == TradesRequest(
            offset=RECOVERY_OFFSET, taker_only=True, start=4_400, end=int(NOW)
        )
        assert client.last_request_end == int(NOW)


class TestResponses:
    async def test_successful_page_carries_rows_status_digest_and_headers(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.publish(trade_row(timestamp=100))
        server.publish(trade_row(timestamp=200))

        page = await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert isinstance(page, TradesPage)
        assert [row["timestamp"] for row in page.rows] == [200, 100]
        assert page.http_status == 200
        assert page.attempts == 1
        assert page.received_at == NOW
        assert page.body_sha256 == server.body_sha256(server.requests[-1].params)
        assert page.headers.cache_control == "public, max-age=300"
        assert page.headers.cf_cache_status == "MISS"
        assert page.headers.age is None

    async def test_empty_list_is_a_successful_empty_acquisition(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        page = await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert page.rows == ()
        assert page.http_status == 200

    async def test_identical_url_is_served_from_the_fake_cache_until_the_ledger_changes(
        self, server: FakeTradesServer
    ) -> None:
        server.publish(trade_row(timestamp=100))
        async with server.client() as client:
            first = await client.get(URL, params={"limit": 10, "end": 5})
            second = await client.get(URL, params={"limit": 10, "end": 5})
            server.publish(trade_row(timestamp=101))
            third = await client.get(URL, params={"limit": 10, "end": 5})
            moved = await client.get(URL, params={"limit": 10, "end": 6})

        assert first.headers["cf-cache-status"] == "MISS"
        assert second.headers["cf-cache-status"] == "HIT"
        assert second.content == first.content
        assert second.headers["age"] == "2"
        assert third.headers["cf-cache-status"] == "MISS"
        assert third.content != first.content
        assert moved.headers["cf-cache-status"] == "MISS"

    async def test_non_list_body_is_terminal_without_retry(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(non_list_body())

        with pytest.raises(TradesTerminalError) as excinfo:
            await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert excinfo.value.reason == "incompatible-schema"
        assert excinfo.value.status == 200
        assert len(server.requests) == 1
        assert isinstance(excinfo.value, TradesSourceError)

    async def test_unparseable_body_is_retried_then_terminal(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(invalid_json(), times=MAX_RETRIES + 1)

        with pytest.raises(TradesTerminalError) as excinfo:
            await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert excinfo.value.reason == "incompatible-schema"
        assert len(server.requests) == MAX_RETRIES + 1

    async def test_unparseable_body_recovers_when_a_retry_succeeds(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(invalid_json())

        page = await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert page.attempts == 2


class TestClassification:
    @pytest.mark.parametrize("status", [408, 425, 429, 500, 502, 503, 599])
    async def test_transient_statuses_are_retried_then_raised(
        self, server: FakeTradesServer, clock: FakeClock, status: int
    ) -> None:
        fault = throttled(None) if status == 429 else server_error(status)
        server.fail_next(fault, times=MAX_RETRIES + 1)

        with pytest.raises(TradesTransientError) as excinfo:
            await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert excinfo.value.status == status
        assert excinfo.value.timeout is False
        assert len(server.requests) == MAX_RETRIES + 1

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 418, 300])
    async def test_terminal_statuses_fail_immediately_with_a_redacted_url(
        self, server: FakeTradesServer, clock: FakeClock, status: int
    ) -> None:
        server.fail_next(terminal(status))

        with pytest.raises(TradesTerminalError) as excinfo:
            await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert excinfo.value.status == status
        assert excinfo.value.reason == "http-status"
        assert str(status) in str(excinfo.value)
        assert URL in str(excinfo.value)
        assert "?" not in str(excinfo.value)
        assert len(server.requests) == 1

    async def test_timeouts_are_transient_and_recover_within_the_retry_budget(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(timeout(), times=2)
        server.publish(trade_row(timestamp=100))

        page = await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert page.attempts == 3
        assert len(page.rows) == 1

    async def test_exhausted_timeouts_raise_a_timeout_transient_error(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(timeout(), times=MAX_RETRIES + 1)

        with pytest.raises(TradesTransientError) as excinfo:
            await _client(server, clock).fetch_primary(boundary_time=None, horizon_seconds=600)

        assert excinfo.value.timeout is True
        assert excinfo.value.status is None

    async def test_transport_errors_are_transient(self, clock: FakeClock) -> None:
        def refuse(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        client = TradesSourceClient(
            httpx.AsyncClient(transport=httpx.MockTransport(refuse)),
            url=URL,
            coverage="all",
            clock=clock,
            sleeper=clock.sleep,
            random_source=lambda: 0.0,
            max_retries=1,
        )

        with pytest.raises(TradesTransientError) as excinfo:
            await client.fetch_primary(boundary_time=None, horizon_seconds=600)

        assert excinfo.value.timeout is False
        assert excinfo.value.status is None
        assert [attempt.outcome for attempt in client.attempts] == ["transient", "transient"]


class TestBackoff:
    async def test_full_jitter_backoff_doubles_from_one_second(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(server_error(), times=MAX_RETRIES + 1)

        with pytest.raises(TradesTransientError):
            await _client(server, clock, random_value=1.0).fetch_primary(
                boundary_time=None, horizon_seconds=600
            )

        assert clock.sleeps == [1.0, 2.0, 4.0, 8.0]

    async def test_zero_jitter_yields_zero_delays(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(server_error(), times=MAX_RETRIES + 1)

        with pytest.raises(TradesTransientError):
            await _client(server, clock, random_value=0.0).fetch_primary(
                boundary_time=None, horizon_seconds=600
            )

        assert clock.sleeps == [0.0, 0.0, 0.0, 0.0]

    async def test_backoff_is_capped(self, server: FakeTradesServer, clock: FakeClock) -> None:
        server.fail_next(server_error(), times=8)

        with pytest.raises(TradesTransientError):
            await _client(server, clock, random_value=1.0, max_retries=7).fetch_primary(
                boundary_time=None, horizon_seconds=600
            )

        assert clock.sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, BACKOFF_CAP_SECONDS, BACKOFF_CAP_SECONDS]

    async def test_retry_after_is_added_and_capped(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(throttled("3"))
        server.fail_next(throttled("120"))
        server.publish(trade_row(timestamp=100))

        page = await _client(server, clock, random_value=1.0).fetch_primary(
            boundary_time=None, horizon_seconds=600
        )

        assert clock.sleeps == [1.0 + 3.0, 2.0 + RETRY_AFTER_CAP_SECONDS]
        assert page.attempts == 3

    def test_retry_after_parsing_accepts_seconds_and_http_dates(self) -> None:
        assert parse_retry_after("7", now=NOW) == 7.0
        assert parse_retry_after(None, now=NOW) == 0.0
        assert parse_retry_after("soon", now=NOW) == 0.0
        assert parse_retry_after("-5", now=NOW) == 0.0
        assert parse_retry_after("Wed, 09 Sep 2026 20:00:00 GMT", now=1_788_984_000.0) == 0.0
        assert parse_retry_after("Wed, 09 Sep 2026 20:00:00 GMT", now=1_788_983_990.0) == 10.0


class TestAccounting:
    async def test_every_attempt_is_recorded_and_counted_in_the_budget_window(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(server_error(), times=2)
        client = _client(server, clock, random_value=1.0)

        page = await client.fetch_primary(boundary_time=None, horizon_seconds=600)

        assert [attempt.outcome for attempt in client.attempts] == [
            "transient",
            "transient",
            "success",
        ]
        assert [attempt.status for attempt in client.attempts] == [503, 503, 200]
        assert all(isinstance(attempt, RequestAttempt) for attempt in client.attempts)
        assert page.attempts == 3
        assert client.requests_in_window() == 3
        clock.advance(7.5)
        assert client.requests_in_window() == 2
        clock.advance(30)
        assert client.requests_in_window() == 0

    async def test_terminal_attempts_are_recorded_too(
        self, server: FakeTradesServer, clock: FakeClock
    ) -> None:
        server.fail_next(terminal(404))
        client = _client(server, clock)

        with pytest.raises(TradesTerminalError):
            await client.fetch_primary(boundary_time=None, horizon_seconds=600)

        assert [attempt.outcome for attempt in client.attempts] == ["terminal"]
        assert client.attempts[0].duration_seconds >= 0.0
