"""The Gamma refresh runs on its own cadence, independent of the CLOB crawl (T012b, T012d)."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from redis.typing import EncodableT, ExpiryT, KeyT

from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.gamma_client import GammaCrawl
from polymarket_insider_tracker.ingestor.gamma_volume_refresh import GammaVolumeRefresh
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync, MetadataSyncError
from polymarket_insider_tracker.ingestor.models import Market, TradeEvent
from polymarket_insider_tracker.ingestor.volume_store import VolumeStore
from tests.fakes import FakeGammaClient
from tests.ingestor.test_metadata_sync import FakeClobClient
from tests.ingestor.test_volume_store import CID, MARKET, MARKET_METADATA, volume_stats


class CountingGamma(FakeGammaClient):
    def __init__(self) -> None:
        super().__init__({CID: volume_stats()})
        self.crawls = 0

    async def crawl_active_markets(self) -> GammaCrawl:
        self.crawls += 1
        return await super().crawl_active_markets()


class SlowCountingClob(FakeClobClient):
    """A CLOB crawl that takes a measurable time and counts how often it runs."""

    def __init__(self, markets: list[Market], seconds: float) -> None:
        super().__init__(markets)
        self.calls = 0
        self._seconds = seconds

    def get_markets(self, active_only: bool = True) -> list[Market]:
        self.calls += 1
        time.sleep(self._seconds)
        return super().get_markets(active_only)


class BrokenRedis(FakeAsyncRedis):
    async def setex(self, name: KeyT, time: ExpiryT, value: EncodableT) -> bool:
        _ = (name, time, value)
        raise ConnectionError("redis is down")


@pytest.mark.asyncio
async def test_gamma_refreshes_repeatedly_while_the_slow_clob_sync_runs_once(
    fake_redis: FakeAsyncRedis,
) -> None:
    gamma, clob = CountingGamma(), SlowCountingClob([MARKET], 0.05)
    sync = MarketMetadataSync(
        redis=fake_redis, clob_client=clob, gamma_client=gamma, gamma_refresh_interval_seconds=0.01
    )
    await sync.start()
    try:
        await asyncio.sleep(0.2)
    finally:
        await sync.stop()

    assert clob.calls == 1
    assert gamma.crawls >= 4


@pytest.mark.asyncio
async def test_stop_ends_the_refresh_loop(fake_redis: FakeAsyncRedis) -> None:
    gamma = CountingGamma()
    sync = MarketMetadataSync(
        redis=fake_redis,
        clob_client=FakeClobClient([MARKET]),
        gamma_client=gamma,
        gamma_refresh_interval_seconds=0.01,
    )
    await sync.start()
    await sync.stop()
    seen = gamma.crawls

    await asyncio.sleep(0.05)

    assert gamma.crawls == seen


@pytest.mark.asyncio
async def test_a_gamma_failure_records_the_outcome_and_does_not_raise(
    fake_redis: FakeAsyncRedis, caplog: pytest.LogCaptureFixture
) -> None:
    store = VolumeStore(fake_redis)
    refresh = GammaVolumeRefresh(FakeGammaClient(raise_error=RuntimeError("down")), store)

    with caplog.at_level(logging.WARNING):
        await refresh.refresh_once()

    assert "gamma stats fetch failed" in caplog.text
    merged = await store.merge(MARKET_METADATA)
    assert merged.volume_unavailable_reason == "gamma_fetch_failed"


@pytest.mark.asyncio
async def test_a_store_failure_is_logged_and_never_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    refresh = GammaVolumeRefresh(CountingGamma(), VolumeStore(BrokenRedis()))

    with caplog.at_level(logging.WARNING):
        await refresh.refresh_once()

    assert "gamma volume store failed" in caplog.text


@pytest.mark.asyncio
async def test_stop_before_start_is_a_no_op(fake_redis: FakeAsyncRedis) -> None:
    await GammaVolumeRefresh(CountingGamma(), VolumeStore(fake_redis)).stop()


@pytest.mark.asyncio
async def test_a_failed_initial_clob_sync_cancels_the_first_gamma_refresh(
    fake_redis: FakeAsyncRedis,
) -> None:
    gamma = CountingGamma()
    sync = MarketMetadataSync(
        redis=fake_redis,
        clob_client=FakeClobClient([], raise_error=RuntimeError("clob down")),
        gamma_client=gamma,
    )

    with pytest.raises(MetadataSyncError):
        await sync.start()

    await asyncio.sleep(0)
    assert not [task for task in asyncio.all_tasks() if "refresh_once" in repr(task)]


def _failing_clock() -> datetime:
    raise RuntimeError("clock boom")


@pytest.mark.asyncio
async def test_stop_surfaces_the_failure_of_a_loop_that_died_before_it() -> None:
    """The refresh loop dies when even the failure path cannot read the clock; stop() reports it."""
    gamma = FakeGammaClient(raise_error=RuntimeError("gamma down"))
    refresh = GammaVolumeRefresh(
        gamma, VolumeStore(FakeAsyncRedis()), interval_seconds=0, clock=_failing_clock
    )
    refresh.start()
    await asyncio.sleep(0.05)

    with pytest.raises(RuntimeError, match="clock boom"):
        await refresh.stop()


class ManualClock:
    """A clock the test moves explicitly; nothing in these tests reads wall time."""

    def __init__(self, start: datetime) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


class ClockedSlowClob(FakeClobClient):
    """A CLOB crawl that takes simulated time: each crawl advances the manual clock."""

    def __init__(self, markets: list[Market], clock: ManualClock, seconds: float) -> None:
        super().__init__(markets)
        self.calls = 0
        self._sim_clock = clock
        self._seconds = seconds

    def get_markets(self, active_only: bool = True) -> list[Market]:
        self.calls += 1
        self._sim_clock.advance(self._seconds)
        return super().get_markets(active_only)


class ClockedGamma(FakeGammaClient):
    """A Gamma fake whose crawls are stamped from the manual clock and can be made to fail."""

    def __init__(self, clock: ManualClock) -> None:
        super().__init__({CID: volume_stats(acquired_at=clock.now())})
        self._sim_clock = clock
        self.crawls = 0
        self._failing: Exception | None = None

    def set_failing(self, err: Exception) -> None:
        self._failing = err

    async def crawl_active_markets(self) -> GammaCrawl:
        self.crawls += 1
        if self._failing is not None:
            raise self._failing
        now = self._sim_clock.now()
        return GammaCrawl(
            stats={CID: volume_stats(acquired_at=now)},
            pages_ok=1,
            pages_failed=0,
            truncated=False,
            started=now,
            finished=now,
        )


def _sample_trade(instant: datetime) -> TradeEvent:
    return TradeEvent(
        market_id=CID,
        trade_id="tx_steady_state",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("1"),
        size=Decimal("1000"),
        timestamp=instant,
        asset_id="token_gamma",
    )


CYCLES = 4
CLOB_CRAWL_SECONDS = 204


@dataclass(frozen=True)
class SteadyStateRun:
    """What one driven run of the slow-CLOB scenario observed; the run itself asserts nothing."""

    positive_states: list[str]
    positive_measured: list[bool]
    positive_ages: list[object]
    clob_calls: int
    gamma_crawls: int
    negative_state: str
    negative_reason: str
    negative_impact: object
    negative_signal_absent: bool


async def _evaluate_sample(
    detector: SizeAnomalyDetector, now: datetime
) -> tuple[str, str, object, bool, object]:
    size_eval = await detector.evaluate(_sample_trade(now), evaluated_at=now)
    verdict = size_eval.volume.verdict
    return (
        verdict.state.value,
        verdict.reason,
        size_eval.volume.volume_impact_measured,
        size_eval.signal is None,
        verdict.age_seconds,
    )


async def _run_steady_state(fake_redis: FakeAsyncRedis) -> SteadyStateRun:
    clock = ManualClock(datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC))
    gamma = ClockedGamma(clock)
    clob = ClockedSlowClob([MARKET], clock, seconds=CLOB_CRAWL_SECONDS)
    sync = MarketMetadataSync(redis=fake_redis, clob_client=clob, gamma_client=gamma)
    refresh = GammaVolumeRefresh(gamma, VolumeStore(fake_redis), clock=clock.now)
    detector = SizeAnomalyDetector(sync, clock=clock.now)

    positive_states: list[str] = []
    positive_measured: list[bool] = []
    positive_ages: list[object] = []
    for _ in range(CYCLES):
        # Gamma stamps the volume at the crawl instant; the slow CLOB then advances the clock,
        # so the evaluation sees the volume aged by the CLOB crawl duration.
        await refresh.refresh_once()
        await sync.force_sync()
        state, _, impact, _, age = await _evaluate_sample(detector, clock.now())
        positive_states.append(state)
        positive_measured.append(impact is not None)
        positive_ages.append(age)

    gamma.set_failing(RuntimeError("gamma degraded"))
    clock.advance(601.0)
    await refresh.refresh_once()
    await sync.force_sync()
    negative_state, negative_reason, negative_impact, signal_absent, _ = await _evaluate_sample(
        detector, clock.now()
    )
    return SteadyStateRun(
        positive_states=positive_states,
        positive_measured=positive_measured,
        positive_ages=positive_ages,
        clob_calls=clob.calls,
        gamma_crawls=gamma.crawls,
        negative_state=negative_state,
        negative_reason=negative_reason,
        negative_impact=negative_impact,
        negative_signal_absent=signal_absent,
    )


@pytest.fixture
async def steady_state(fake_redis: FakeAsyncRedis) -> SteadyStateRun:
    return await _run_steady_state(fake_redis)


async def test_slow_clob_steady_state_sample_is_available_every_cycle(
    steady_state: SteadyStateRun,
) -> None:
    assert steady_state.positive_states == ["available"] * CYCLES


async def test_slow_clob_ages_each_sample_by_the_clob_crawl_duration(
    steady_state: SteadyStateRun,
) -> None:
    assert steady_state.positive_ages == [str(CLOB_CRAWL_SECONDS)] * CYCLES


async def test_slow_clob_steady_state_measures_the_impact_every_cycle(
    steady_state: SteadyStateRun,
) -> None:
    assert steady_state.positive_measured == [True] * CYCLES


async def test_slow_clob_is_crawled_once_per_cycle_and_once_degraded(
    steady_state: SteadyStateRun,
) -> None:
    assert steady_state.clob_calls == CYCLES + 1


async def test_gamma_is_crawled_once_per_cycle_and_once_degraded(
    steady_state: SteadyStateRun,
) -> None:
    assert steady_state.gamma_crawls == CYCLES + 1


async def test_degraded_gamma_leaves_the_volume_stale(steady_state: SteadyStateRun) -> None:
    assert steady_state.negative_state == "stale"


async def test_degraded_gamma_gives_the_older_than_600s_reason(
    steady_state: SteadyStateRun,
) -> None:
    assert steady_state.negative_reason == "older_than_600s"


async def test_degraded_gamma_measures_no_volume_impact(steady_state: SteadyStateRun) -> None:
    assert steady_state.negative_impact is None


async def test_degraded_gamma_emits_no_signal(steady_state: SteadyStateRun) -> None:
    assert steady_state.negative_signal_absent is True
