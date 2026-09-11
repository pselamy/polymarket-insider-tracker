"""The trade poller: cycles, delivery, dedup, status, metrics, failure, and recovery behavior."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from fakeredis import FakeAsyncRedis, FakeServer
from prometheus_client import REGISTRY

from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.ingestor.observation_boundary import (
    BoundaryOrigin,
    LossEvent,
    LossReason,
)
from polymarket_insider_tracker.ingestor.trade_poller import (
    IngestionState,
    IngestionStatus,
    PageAnalysis,
    TradePoller,
)
from polymarket_insider_tracker.ingestor.trade_rows import RowDisposition
from tests.fakes import (
    FakeClock,
    FakeMetadataSync,
    FakeTradesServer,
    make_test_settings,
    non_list_body,
    server_error,
    synthetic_wallet,
    terminal,
    throttled,
    timeout,
    trade_row,
)
from tests.fakes.trades import Fault

NOW = 1_788_983_720.0
T0 = int(NOW)


class RecordingTradeCallback:
    """The downstream trade consumer; keeps every delivered event in order."""

    def __init__(self, failing: Exception | None = None) -> None:
        self.trades: list[TradeEvent] = []
        self.failing = failing

    async def __call__(self, trade: TradeEvent) -> None:
        self.trades.append(trade)
        if self.failing is not None:
            raise self.failing

    def timestamps(self) -> list[int]:
        return [int(trade.timestamp.timestamp()) for trade in self.trades]


class RecordingStateCallback:
    def __init__(self) -> None:
        self.states: list[IngestionState] = []

    def __call__(self, state: IngestionState) -> None:
        self.states.append(state)


def _metric(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(NOW)


@pytest.fixture
def server(clock: FakeClock) -> FakeTradesServer:
    return FakeTradesServer(clock=clock)


@pytest.fixture
def settings() -> PolymarketSettings:
    return make_test_settings(trades_url="https://trades.invalid/trades").polymarket


@pytest.fixture
def callback() -> RecordingTradeCallback:
    return RecordingTradeCallback()


@pytest.fixture
def states() -> RecordingStateCallback:
    return RecordingStateCallback()


def _poller(
    server: FakeTradesServer,
    clock: FakeClock,
    fake_redis: FakeAsyncRedis,
    settings: PolymarketSettings,
    callback: RecordingTradeCallback,
    *,
    states: RecordingStateCallback | None = None,
    metadata: FakeMetadataSync | None = None,
) -> TradePoller:
    return TradePoller(
        callback,
        redis=fake_redis,
        settings=settings,
        metadata=metadata,
        on_state_change=states,
        http_client=server.client(),
        clock=clock,
        sleeper=clock.sleep,
        random_source=lambda: 0.0,
    )


def _seed_history(server: FakeTradesServer, *, count: int = 5, newest: int = T0 - 1) -> None:
    for offset in range(count):
        server.publish(trade_row(timestamp=newest - offset * 60, transaction=offset, wallet=offset))


async def _run_until(predicate: Callable[[], bool], *, cycles: int = 2000) -> None:
    for _ in range(cycles):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition was not reached")


class TestFirstStart:
    async def test_first_page_anchors_the_boundary_without_emission(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
        states: RecordingStateCallback,
    ) -> None:
        _seed_history(server, count=5)
        poller = _poller(server, clock, fake_redis, settings, callback, states=states)

        await poller.run_cycle()

        status = poller.status
        assert callback.trades == []
        assert status.state is IngestionState.RUNNING
        assert status.boundary_time == T0 - 1
        assert status.boundary_origin is BoundaryOrigin.FIRST_START
        assert status.counts[RowDisposition.ANCHOR_HISTORY.value] == 5
        assert status.counts[RowDisposition.EMITTED.value] == 0
        assert states.states == [IngestionState.STARTING, IngestionState.RUNNING]
        assert await fake_redis.zcard(poller.boundary.identities_key) == 5

    async def test_first_start_pads_history_beyond_the_horizon(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        server.publish(trade_row(timestamp=T0 - 1, transaction=1))
        server.publish(trade_row(timestamp=T0 - 1 - 600, transaction=2))
        server.publish(trade_row(timestamp=T0 - 1 - 601, transaction=3))
        poller = _poller(server, clock, fake_redis, settings, callback)

        await poller.run_cycle()

        assert poller.status.counts[RowDisposition.ANCHOR_HISTORY.value] == 2
        assert poller.status.counts[RowDisposition.PADDING.value] == 1
        assert await fake_redis.zcard(poller.boundary.identities_key) == 2


class TestHealthyCycles:
    async def test_new_distinct_observations_are_delivered_oldest_first_exactly_once(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        _seed_history(server, count=5)
        poller = _poller(server, clock, fake_redis, settings, callback)
        await poller.run_cycle()
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 3, transaction=13, wallet=3))
        server.publish(trade_row(timestamp=T0 + 1, transaction=11, wallet=1))
        server.publish(trade_row(timestamp=T0 + 2, transaction=12, wallet=2))

        await poller.run_cycle()
        second = poller.status
        clock.advance(5)
        await poller.run_cycle()
        third = poller.status

        assert callback.timestamps() == [T0 + 1, T0 + 2, T0 + 3]
        assert [trade.wallet_address for trade in callback.trades] == [
            synthetic_wallet(1),
            synthetic_wallet(2),
            synthetic_wallet(3),
        ]
        assert second.counts[RowDisposition.EMITTED.value] == 3
        assert second.counts[RowDisposition.DUPLICATE.value] == 1
        assert second.boundary_time == T0 + 3
        assert second.boundary_origin is BoundaryOrigin.PROVEN
        assert third.counts[RowDisposition.EMITTED.value] == 3
        assert third.counts[RowDisposition.DUPLICATE.value] == 5
        assert len(callback.trades) == 3
        assert dict(poller.boundary.window) == {
            member.decode(): int(score)
            for member, score in await fake_redis.zrangebyscore(
                poller.boundary.identities_key, "-inf", "+inf", withscores=True
            )
        }

    async def test_exact_repeats_are_suppressed_while_multi_wallet_rows_are_distinct(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        _seed_history(server, count=2)
        poller = _poller(server, clock, fake_redis, settings, callback)
        await poller.run_cycle()
        clock.advance(5)
        taker = trade_row(timestamp=T0 + 1, transaction=7, wallet=1, side="BUY")
        maker = trade_row(timestamp=T0 + 1, transaction=7, wallet=2, side="SELL")
        server.publish(taker)
        server.publish(maker)
        server.publish(dict(taker))

        await poller.run_cycle()

        assert sorted(trade.wallet_address for trade in callback.trades) == [
            synthetic_wallet(1),
            synthetic_wallet(2),
        ]
        assert {trade.trade_id for trade in callback.trades} == {taker["transactionHash"]}
        assert poller.status.counts[RowDisposition.EMITTED.value] == 2
        assert poller.status.counts[RowDisposition.DUPLICATE.value] == 2

    async def test_rows_newer_than_the_cutoff_are_deferred_then_reacquired(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        _seed_history(server, count=2)
        poller = _poller(server, clock, fake_redis, settings, callback)
        await poller.run_cycle()
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 5, transaction=5))
        server.publish(trade_row(timestamp=T0 + 9, transaction=9))

        await poller.run_cycle()
        deferred = poller.status
        clock.advance(5)
        await poller.run_cycle()

        assert deferred.counts[RowDisposition.DEFERRED_FUTURE_CYCLE.value] == 1
        assert deferred.boundary_time == T0 + 5
        assert deferred.latest_seen_at == datetime.fromtimestamp(T0 + 9, tz=UTC)
        assert deferred.last_trade_at == datetime.fromtimestamp(T0 + 5, tz=UTC)
        assert callback.timestamps() == [T0 + 5, T0 + 9]
        assert poller.status.boundary_time == T0 + 9

    async def test_missing_outcome_is_repaired_from_cached_metadata_or_reported_unknown(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        metadata = FakeMetadataSync()
        metadata.markets["0xmarket"] = MarketMetadata(
            condition_id="0xmarket",
            question="Synthetic?",
            description="",
            tokens=(
                Token(token_id="asset-no", outcome="No"),
                Token(token_id="asset-yes", outcome="Yes"),
            ),
        )
        _seed_history(server, count=2)
        poller = _poller(server, clock, fake_redis, settings, callback, metadata=metadata)
        await poller.run_cycle()
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 1, transaction=1, outcome=None, outcome_index=None))
        server.publish(
            trade_row(
                timestamp=T0 + 2,
                transaction=2,
                market="0xunknown",
                outcome=None,
                outcome_index=None,
            )
        )
        server.publish(trade_row(timestamp=T0 + 3, transaction=3))

        await poller.run_cycle()

        assert [(trade.outcome, trade.outcome_index) for trade in callback.trades] == [
            ("Yes", 1),
            ("", 0),
            ("Yes", 0),
        ]
        assert poller.status.outcome_counts == {"provided": 5, "repaired": 1, "unknown": 1}

    async def test_callback_errors_are_counted_and_do_not_stop_the_cycle(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        callback = RecordingTradeCallback(failing=RuntimeError("downstream exploded"))
        _seed_history(server, count=2)
        poller = _poller(server, clock, fake_redis, settings, callback)
        await poller.run_cycle()
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 1, transaction=1))
        server.publish(trade_row(timestamp=T0 + 2, transaction=2))

        with caplog.at_level(logging.ERROR):
            await poller.run_cycle()

        assert len(callback.trades) == 2
        assert poller.status.counts["callback_errors"] == 2
        assert poller.status.state is IngestionState.RUNNING
        assert poller.status.boundary_time == T0 + 2
        assert "downstream exploded" in caplog.text


class TestStatusAndMetrics:
    async def test_status_snapshot_reports_every_documented_field(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        _seed_history(server, count=3, newest=T0 - 10)
        poller = _poller(server, clock, fake_redis, settings, callback)
        initial = poller.status
        await poller.run_cycle()
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 4, transaction=4))
        await poller.run_cycle()

        status = poller.status

        assert isinstance(status, IngestionStatus)
        assert initial.state is IngestionState.STOPPED
        assert initial.boundary_time is None
        assert initial.last_success_at is None
        assert initial.provider_lag_seconds is None
        assert status.state is IngestionState.RUNNING
        assert status.coverage == "all"
        assert status.boundary_time == T0 + 4
        assert status.boundary_origin is BoundaryOrigin.PROVEN
        assert status.last_acquisition_at == datetime.fromtimestamp(T0 + 5, tz=UTC)
        assert status.last_success_at == datetime.fromtimestamp(T0 + 5, tz=UTC)
        assert status.last_trade_at == datetime.fromtimestamp(T0 + 4, tz=UTC)
        assert status.latest_seen_at == datetime.fromtimestamp(T0 + 4, tz=UTC)
        assert status.provider_lag_seconds == pytest.approx(1.0)
        assert status.processing_lag_seconds >= 0.0
        assert status.consecutive_failures == 0
        assert status.last_error is None
        assert set(status.counts) == {disposition.value for disposition in RowDisposition} | {
            "polls",
            "recovery_pages",
            "empty_responses",
            "retries",
            "callback_errors",
        }
        assert status.counts["polls"] == 2
        assert status.counts["recovery_pages"] == 0
        assert status.outcome_counts == {"provided": 7, "repaired": 0, "unknown": 0}
        assert status.page_rows == 4
        assert status.page_span_seconds == 4 + 10 + 120
        assert status.loss_events == ()
        assert status.requests_last_10s == 2

    async def test_metrics_mirror_state_rows_boundary_and_requests(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        emitted_before = _metric("polymarket_ingest_rows_total", disposition="emitted")
        success_before = _metric("polymarket_ingest_requests_total", outcome="success")
        _seed_history(server, count=2)
        poller = _poller(server, clock, fake_redis, settings, callback)
        await poller.run_cycle()
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 1, transaction=1))
        await poller.run_cycle()

        assert _metric("polymarket_ingest_state") == 2.0
        assert _metric("polymarket_ingest_rows_total", disposition="emitted") - emitted_before == 1
        assert _metric("polymarket_ingest_requests_total", outcome="success") - success_before == 2
        assert _metric("polymarket_ingest_boundary_timestamp_seconds") == float(T0 + 1)
        assert _metric("polymarket_ingest_provider_lag_seconds") == pytest.approx(4.0)
        assert _metric("polymarket_ingest_page_span_seconds") == 62.0
        assert _metric("polymarket_ingest_request_duration_seconds_count") >= 2


class TestLifecycle:
    async def test_start_runs_cycles_on_the_poll_cadence_until_stopped(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
        states: RecordingStateCallback,
    ) -> None:
        _seed_history(server, count=2)
        poller = _poller(server, clock, fake_redis, settings, callback, states=states)

        task = asyncio.create_task(poller.start())
        await _run_until(lambda: len(server.requests) >= 3)
        await poller.stop()
        await asyncio.wait_for(task, timeout=1.0)

        assert [request.at for request in server.requests][:3] == [NOW, NOW + 5, NOW + 10]
        assert states.states[:2] == [IngestionState.STARTING, IngestionState.RUNNING]
        assert states.states[-1] is IngestionState.STOPPED
        assert poller.state is IngestionState.STOPPED

    async def test_stop_before_start_is_a_no_op(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        poller = _poller(server, clock, fake_redis, settings, callback)

        await poller.stop()

        assert poller.state is IngestionState.STOPPED
        assert server.requests == []


class StoppingCallback(RecordingTradeCallback):
    """A consumer that asks the poller to stop while its first delivery is in progress."""

    def __init__(self) -> None:
        super().__init__()
        self.poller: TradePoller | None = None

    async def __call__(self, trade: TradeEvent) -> None:
        await super().__call__(trade)
        assert self.poller is not None
        await self.poller.stop()


class InterruptedCallback(RecordingTradeCallback):
    """A consumer whose process is cancelled right after it has taken the first delivery."""

    async def __call__(self, trade: TradeEvent) -> None:
        await super().__call__(trade)
        if len(self.trades) == 1:
            raise asyncio.CancelledError


async def _anchored(
    server: FakeTradesServer,
    clock: FakeClock,
    fake_redis: FakeAsyncRedis,
    settings: PolymarketSettings,
    callback: RecordingTradeCallback,
    *,
    states: RecordingStateCallback | None = None,
) -> TradePoller:
    """A poller that has completed its first-start cycle over two rows of history."""
    _seed_history(server, count=2)
    poller = _poller(server, clock, fake_redis, settings, callback, states=states)
    await poller.run_cycle()
    clock.advance(5)
    return poller


class TestTransientFailures:
    @pytest.mark.parametrize(
        ("fault", "fragment"),
        [
            (throttled("1"), "HTTP 429"),
            (server_error(503), "HTTP 503"),
            (timeout(), "timeout"),
        ],
    )
    async def test_exhausted_retries_degrade_and_the_next_success_recovers(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
        states: RecordingStateCallback,
        fault: Fault,
        fragment: str,
    ) -> None:
        poller = await _anchored(server, clock, fake_redis, settings, callback, states=states)
        server.fail_next(fault, times=5)

        await poller.run_cycle()
        degraded = poller.status
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 6, transaction=6))
        await poller.run_cycle()

        assert degraded.state is IngestionState.DEGRADED
        assert degraded.consecutive_failures == 1
        assert degraded.last_error is not None
        assert fragment in degraded.last_error
        assert "?" not in degraded.last_error
        assert degraded.counts["retries"] == 4
        assert degraded.boundary_time == T0 - 1
        assert poller.status.state is IngestionState.RUNNING
        assert poller.status.consecutive_failures == 0
        assert poller.status.last_error is None
        assert callback.timestamps() == [T0 + 6]
        assert states.states[-2:] == [IngestionState.DEGRADED, IngestionState.RUNNING]

    async def test_a_recovered_retry_within_the_budget_never_degrades(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        poller = await _anchored(server, clock, fake_redis, settings, callback)
        server.fail_next(throttled("2"), times=2)
        server.publish(trade_row(timestamp=T0 + 1, transaction=1))

        await poller.run_cycle()

        assert poller.status.state is IngestionState.RUNNING
        assert poller.status.counts["retries"] == 2
        assert callback.timestamps() == [T0 + 1]
        assert clock.sleeps[-2:] == [2.0, 2.0]


class TestTerminalFailures:
    @pytest.mark.parametrize(
        ("fault", "fragment"),
        [
            (terminal(401), "HTTP 401"),
            (terminal(404), "HTTP 404"),
            (non_list_body(), "incompatible-schema"),
        ],
    )
    async def test_terminal_responses_fail_the_poller_with_a_redacted_error(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
        states: RecordingStateCallback,
        fault: Fault,
        fragment: str,
    ) -> None:
        poller = await _anchored(server, clock, fake_redis, settings, callback, states=states)
        server.fail_next(fault)
        requests_before = len(server.requests)

        await poller.run_cycle()
        clock.advance(5)
        await poller.run_cycle()

        status = poller.status
        assert status.state is IngestionState.FAILED
        assert status.last_error is not None
        assert fragment in status.last_error
        assert "trades.invalid" in status.last_error
        assert "***path***" in status.last_error
        assert "?" not in status.last_error
        assert len(server.requests) == requests_before + 1
        assert states.states[-1] is IngestionState.FAILED
        assert callback.trades == []

    async def test_start_returns_once_the_poller_has_failed(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        _seed_history(server, count=2)
        server.fail_next(terminal(403))
        poller = _poller(server, clock, fake_redis, settings, callback)

        await asyncio.wait_for(poller.start(), timeout=1.0)

        assert poller.state is IngestionState.FAILED
        assert poller.status.last_error is not None
        assert "HTTP 403" in poller.status.last_error
        assert len(server.requests) == 1


class TestRowQuality:
    async def test_recovery_placeholder_cannot_replace_a_complete_primary_outcome(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        poller = _poller(server, clock, fake_redis, settings, callback)
        analysis = PageAnalysis(cutoff=T0)
        analysis.add_rows([trade_row(timestamp=T0, outcome="No", outcome_index=1)], now=T0)
        await poller._resolve_and_count_outcomes(analysis)
        analysis.add_rows([trade_row(timestamp=T0, outcome="No", outcome_index=999)], now=T0)

        await poller._resolve_and_count_outcomes(analysis)

        observation = next(iter(analysis.observations.values()))
        assert (observation.event.outcome, observation.event.outcome_index) == ("No", 1)
        assert poller.status.outcome_counts == {
            "provided": 1,
            "repaired": 0,
            "unknown": 1,
        }

    async def test_invalid_rows_are_counted_per_field_while_valid_rows_flow(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        poller = await _anchored(server, clock, fake_redis, settings, callback)
        missing_wallet = trade_row(timestamp=T0 + 1, transaction=1, wallet=7)
        del missing_wallet["proxyWallet"]
        bad_side = trade_row(timestamp=T0 + 2, transaction=2, wallet=8, side="HOLD")
        server.publish(missing_wallet)
        server.publish(bad_side)
        server.publish("not an object")
        server.publish(trade_row(timestamp=T0 + 3, transaction=3))
        server.publish(trade_row(timestamp=T0 + 4, transaction=4))

        with caplog.at_level(logging.WARNING):
            await poller.run_cycle()

        counts = poller.status.counts
        assert counts[RowDisposition.INVALID_PROXY_WALLET.value] == 1
        assert counts[RowDisposition.INVALID_SIDE.value] == 1
        assert counts[RowDisposition.INVALID_SCHEMA.value] == 1
        assert counts[RowDisposition.EMITTED.value] == 2
        assert callback.timestamps() == [T0 + 3, T0 + 4]
        assert poller.status.page_rows == 7
        assert poller.status.state is IngestionState.RUNNING
        assert "invalid trade row" in caplog.text
        assert synthetic_wallet(7) not in caplog.text
        assert synthetic_wallet(8) not in caplog.text

    async def test_out_of_order_and_equal_second_rows_are_delivered_once_in_order(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        poller = await _anchored(server, clock, fake_redis, settings, callback)
        server.publish(trade_row(timestamp=T0 + 2, transaction=22, wallet=2))
        server.publish(trade_row(timestamp=T0 + 1, transaction=11, wallet=1))
        server.publish(trade_row(timestamp=T0 + 2, transaction=21, wallet=3))
        server.publish(trade_row(timestamp=T0 + 1, transaction=12, wallet=4))

        await poller.run_cycle()
        clock.advance(5)
        await poller.run_cycle()

        assert callback.timestamps() == [T0 + 1, T0 + 1, T0 + 2, T0 + 2]
        assert len({trade.trade_id for trade in callback.trades}) == 4
        assert poller.status.counts[RowDisposition.EMITTED.value] == 4
        assert poller.status.boundary_time == T0 + 2

    async def test_empty_response_updates_reachability_without_emission(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        poller = _poller(server, clock, fake_redis, settings, callback)

        await poller.run_cycle()

        status = poller.status
        assert status.state is IngestionState.RUNNING
        assert status.counts["empty_responses"] == 1
        assert status.last_success_at == datetime.fromtimestamp(T0, tz=UTC)
        assert status.last_trade_at is None
        assert status.boundary_time is None
        assert status.page_rows == 0
        assert callback.trades == []


class TestRestart:
    async def test_failed_reanchor_is_retried_without_replaying_history(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        settings: PolymarketSettings,
    ) -> None:
        redis_server = FakeServer()
        fake_redis = FakeAsyncRedis(server=redis_server)
        original = await _anchored(server, clock, fake_redis, settings, RecordingTradeCallback())
        await original.stop()
        clock.advance(700)
        server.publish(trade_row(timestamp=T0 + 650, transaction=650))
        callback = RecordingTradeCallback()
        restarted = _poller(server, clock, fake_redis, settings, callback)
        await restarted._ensure_loaded()
        vars(redis_server)["connected"] = False

        await restarted.run_cycle()

        assert restarted.status.state is IngestionState.DEGRADED
        assert restarted.status.loss_events == ()
        assert callback.trades == []
        vars(redis_server)["connected"] = True
        await restarted.run_cycle()

        assert restarted.status.state is IngestionState.RUNNING
        assert restarted.status.boundary_origin is BoundaryOrigin.RE_ANCHORED
        assert len(restarted.status.loss_events) == 1
        assert callback.trades == []
        await fake_redis.aclose()

    async def test_restart_inside_the_horizon_delivers_only_missed_observations(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
    ) -> None:
        first_run = RecordingTradeCallback()
        poller = await _anchored(server, clock, fake_redis, settings, first_run)
        server.publish(trade_row(timestamp=T0 + 1, transaction=1))
        server.publish(trade_row(timestamp=T0 + 2, transaction=2))
        await poller.run_cycle()
        await poller.stop()
        clock.advance(120)
        server.publish(trade_row(timestamp=T0 + 100, transaction=3))
        server.publish(trade_row(timestamp=T0 + 101, transaction=4))
        second_run = RecordingTradeCallback()
        restarted = _poller(server, clock, fake_redis, settings, second_run)

        await restarted.run_cycle()

        assert first_run.timestamps() == [T0 + 1, T0 + 2]
        assert second_run.timestamps() == [T0 + 100, T0 + 101]
        assert restarted.status.counts[RowDisposition.DUPLICATE.value] == 3
        assert restarted.status.boundary_time == T0 + 101
        assert restarted.status.boundary_origin is BoundaryOrigin.PROVEN
        assert restarted.status.loss_events == ()

    async def test_restart_beyond_the_horizon_records_loss_and_re_anchors_silently(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
    ) -> None:
        loss_before = _metric(
            "polymarket_ingest_loss_events_total", reason="restart-beyond-horizon"
        )
        poller = await _anchored(server, clock, fake_redis, settings, RecordingTradeCallback())
        await poller.stop()
        clock.advance(700)
        for offset in range(5):
            server.publish(trade_row(timestamp=T0 + 650 + offset, transaction=10 + offset))
        callback = RecordingTradeCallback()
        restarted = _poller(server, clock, fake_redis, settings, callback)

        await restarted.run_cycle()

        status = restarted.status
        assert callback.trades == []
        assert status.state is IngestionState.RUNNING
        assert status.boundary_time == T0 + 654
        assert status.boundary_origin is BoundaryOrigin.RE_ANCHORED
        assert status.counts[RowDisposition.ANCHOR_HISTORY.value] == 5
        assert status.counts[RowDisposition.PADDING.value] == 2
        assert status.loss_events == (
            LossEvent(
                from_time=T0 - 1,
                to_time=T0 + 654,
                reason=LossReason.RESTART_BEYOND_HORIZON,
                detected_at=T0 + 705,
                recorded_at=T0 + 705,
                pages_examined=1,
            ),
        )
        assert await fake_redis.llen(restarted.boundary.loss_events_key) == 1
        assert (
            _metric("polymarket_ingest_loss_events_total", reason="restart-beyond-horizon")
            - loss_before
            == 1
        )


class TestSaturation:
    async def test_recovery_rows_are_repaired_and_counted(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        metadata = FakeMetadataSync()
        metadata.markets["0xmarket"] = MarketMetadata(
            condition_id="0xmarket",
            question="Synthetic?",
            description="",
            tokens=(
                Token(token_id="asset-no", outcome="No"),
                Token(token_id="asset-yes", outcome="Yes"),
            ),
        )
        server.page_limit = 2
        _seed_history(server, count=2)
        poller = _poller(server, clock, fake_redis, settings, callback, metadata=metadata)
        await poller.run_cycle()
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 1, transaction=1, outcome=None, outcome_index=None))
        server.publish(trade_row(timestamp=T0 + 2, transaction=2))
        server.publish(trade_row(timestamp=T0 + 3, transaction=3))

        await poller.run_cycle()

        repaired = next(
            trade for trade in callback.trades if int(trade.timestamp.timestamp()) == T0 + 1
        )
        assert (repaired.outcome, repaired.outcome_index) == ("Yes", 1)
        assert poller.status.outcome_counts["repaired"] == 1

    async def test_recovery_page_proves_a_saturated_primary_page(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        server.page_limit = 2
        poller = await _anchored(server, clock, fake_redis, settings, callback)
        server.publish(trade_row(timestamp=T0 + 1, transaction=1))
        server.publish(trade_row(timestamp=T0 + 2, transaction=2))
        requests_before = len(server.requests)

        await poller.run_cycle()

        status = poller.status
        assert callback.timestamps() == [T0 + 1, T0 + 2]
        assert status.state is IngestionState.RUNNING
        assert status.boundary_time == T0 + 2
        assert status.counts["recovery_pages"] == 1
        assert len(server.requests) == requests_before + 2
        assert server.requests[-1].params["offset"] == "10000"
        assert status.loss_events == ()

    async def test_unproven_pages_freeze_the_boundary_and_keep_flowing_provisionally(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
        states: RecordingStateCallback,
    ) -> None:
        server.page_limit = 2
        poller = await _anchored(server, clock, fake_redis, settings, callback, states=states)
        for offset in range(1, 6):
            server.publish(trade_row(timestamp=T0 + offset, transaction=offset))

        await poller.run_cycle()
        frozen = poller.status
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 6, transaction=6))
        await poller.run_cycle()
        continued = poller.status

        assert frozen.state is IngestionState.POSSIBLE_DATA_LOSS
        assert frozen.boundary_time == T0 - 1
        assert frozen.boundary_origin is BoundaryOrigin.FIRST_START
        assert frozen.counts["recovery_pages"] == 1
        assert frozen.loss_events == ()
        assert callback.timestamps()[:4] == [T0 + 2, T0 + 3, T0 + 4, T0 + 5]
        assert (
            await fake_redis.hget(poller.boundary.checkpoint_key, "boundary_time")
            == str(T0 - 1).encode()
        )
        assert continued.state is IngestionState.POSSIBLE_DATA_LOSS
        assert continued.boundary_time == T0 - 1
        assert continued.counts["recovery_pages"] == 2
        assert callback.timestamps() == [T0 + 2, T0 + 3, T0 + 4, T0 + 5, T0 + 6]
        assert continued.last_trade_at == datetime.fromtimestamp(T0 + 6, tz=UTC)
        assert states.states[-1] is IngestionState.POSSIBLE_DATA_LOSS

    async def test_a_later_page_proving_the_frozen_boundary_closes_the_gap_without_loss(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        server.page_limit = 2
        poller = await _anchored(server, clock, fake_redis, settings, callback)
        for offset in range(1, 6):
            server.publish(trade_row(timestamp=T0 + offset, transaction=offset))
        await poller.run_cycle()
        assert poller.state is IngestionState.POSSIBLE_DATA_LOSS
        clock.advance(5)
        server.page_limit = 100

        await poller.run_cycle()

        status = poller.status
        assert status.state is IngestionState.RUNNING
        assert status.boundary_time == T0 + 5
        assert status.boundary_origin is BoundaryOrigin.PROVEN
        assert status.loss_events == ()
        assert callback.timestamps() == [T0 + 2, T0 + 3, T0 + 4, T0 + 5, T0 + 1]

    async def test_horizon_expiry_records_the_gap_and_re_anchors(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
        states: RecordingStateCallback,
    ) -> None:
        loss_before = _metric("polymarket_ingest_loss_events_total", reason="horizon-expired")
        server.page_limit = 2
        poller = await _anchored(server, clock, fake_redis, settings, callback, states=states)
        for offset in range(1, 6):
            server.publish(trade_row(timestamp=T0 + offset, transaction=offset))
        await poller.run_cycle()
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 6, transaction=6))
        await poller.run_cycle()
        clock.advance(600)

        await poller.run_cycle()

        status = poller.status
        assert status.state is IngestionState.RUNNING
        assert status.boundary_time == T0 + 6
        assert status.boundary_origin is BoundaryOrigin.RE_ANCHORED
        assert status.loss_events == (
            LossEvent(
                from_time=T0 - 1,
                to_time=T0 + 2,
                reason=LossReason.HORIZON_EXPIRED,
                detected_at=T0 + 5,
                recorded_at=T0 + 610,
                pages_examined=6,
            ),
        )
        assert (
            await fake_redis.hget(poller.boundary.checkpoint_key, "boundary_time")
            == str(T0 + 6).encode()
        )
        assert (
            _metric("polymarket_ingest_loss_events_total", reason="horizon-expired") - loss_before
            == 1
        )
        assert states.states[-2:] == [IngestionState.POSSIBLE_DATA_LOSS, IngestionState.RUNNING]

    async def test_missing_retained_identity_is_a_continuity_mismatch(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        poller = await _anchored(server, clock, fake_redis, settings, callback)
        retained = trade_row(timestamp=T0 + 1, transaction=31)
        server.publish(retained)
        server.publish(trade_row(timestamp=T0 + 2, transaction=32))
        await poller.run_cycle()
        clock.advance(5)
        assert server.retract(retained["transactionHash"]) == 1
        server.publish(trade_row(timestamp=T0 + 7, transaction=7))

        await poller.run_cycle()
        mismatch = poller.status
        clock.advance(600)
        await poller.run_cycle()

        assert mismatch.state is IngestionState.POSSIBLE_DATA_LOSS
        assert mismatch.boundary_time == T0 + 2
        assert callback.timestamps() == [T0 + 1, T0 + 2, T0 + 7]
        assert poller.status.state is IngestionState.RUNNING
        assert poller.status.loss_events[0].reason is LossReason.CONTINUITY_MISMATCH
        assert poller.status.loss_events[0].from_time == T0 + 2
        assert poller.status.loss_events[0].to_time == T0 - 61


class TestPreStartHistory:
    async def test_sparse_window_does_not_hide_a_late_unseen_post_start_row(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        server.publish(trade_row(timestamp=T0 - 2, transaction=0))
        server.publish(trade_row(timestamp=T0 - 1, transaction=1))
        poller = _poller(server, clock, fake_redis, settings, callback)
        await poller.run_cycle()
        clock.advance(655)
        server.publish(trade_row(timestamp=T0 + 650, transaction=650))
        await poller.run_cycle()
        server.publish(trade_row(timestamp=T0 + 620, transaction=620))

        await poller.run_cycle()

        assert callback.timestamps() == [T0 + 650, T0 + 620]

    async def test_recovery_page_never_replays_history_older_than_the_anchor(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
        callback: RecordingTradeCallback,
    ) -> None:
        """Rows inside the horizon but deeper than the first page are pre-start history."""
        server.page_limit = 2
        server.publish(trade_row(timestamp=T0 - 121, transaction=121))
        server.publish(trade_row(timestamp=T0 - 61, transaction=61))
        server.publish(trade_row(timestamp=T0 - 1, transaction=1))
        poller = _poller(server, clock, fake_redis, settings, callback)
        await poller.run_cycle()
        assert poller.status.counts[RowDisposition.ANCHOR_HISTORY.value] == 2
        clock.advance(5)
        server.publish(trade_row(timestamp=T0 + 1, transaction=2))

        await poller.run_cycle()

        status = poller.status
        assert callback.timestamps() == [T0 + 1]
        assert status.state is IngestionState.RUNNING
        assert status.boundary_time == T0 + 1
        assert status.counts["recovery_pages"] == 1
        assert status.counts[RowDisposition.PADDING.value] == 2
        assert status.counts[RowDisposition.EMITTED.value] == 1


class TestStopAndCrash:
    async def test_graceful_stop_mid_page_leaves_no_partial_checkpoint(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
    ) -> None:
        stopping = StoppingCallback()
        poller = await _anchored(server, clock, fake_redis, settings, stopping)
        stopping.poller = poller
        for offset in range(1, 4):
            server.publish(trade_row(timestamp=T0 + offset, transaction=offset))

        await poller.run_cycle()

        assert stopping.timestamps() == [T0 + 1]
        assert poller.status.counts[RowDisposition.EMITTED.value] == 1
        assert poller.status.boundary_time == T0 - 1
        assert (
            await fake_redis.hget(poller.boundary.checkpoint_key, "boundary_time")
            == str(T0 - 1).encode()
        )
        assert poller.state is IngestionState.STOPPED

        resumed = RecordingTradeCallback()
        restarted = _poller(server, clock, fake_redis, settings, resumed)
        await restarted.run_cycle()

        assert resumed.timestamps() == [T0 + 2, T0 + 3]
        assert restarted.status.boundary_time == T0 + 3

    async def test_a_crash_after_delivery_re_delivers_at_most_that_observation(
        self,
        server: FakeTradesServer,
        clock: FakeClock,
        fake_redis: FakeAsyncRedis,
        settings: PolymarketSettings,
    ) -> None:
        interrupted = InterruptedCallback()
        poller = await _anchored(server, clock, fake_redis, settings, interrupted)
        for offset in range(1, 4):
            server.publish(trade_row(timestamp=T0 + offset, transaction=offset))

        with pytest.raises(asyncio.CancelledError):
            await poller.run_cycle()

        assert interrupted.timestamps() == [T0 + 1]
        assert await fake_redis.zcard(poller.boundary.identities_key) == 2

        resumed = RecordingTradeCallback()
        restarted = _poller(server, clock, fake_redis, settings, resumed)
        await restarted.run_cycle()

        assert resumed.timestamps() == [T0 + 1, T0 + 2, T0 + 3]
        assert restarted.status.boundary_time == T0 + 3
