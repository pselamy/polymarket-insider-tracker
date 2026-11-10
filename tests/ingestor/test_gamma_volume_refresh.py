"""The Gamma refresh runs on its own cadence, independent of the CLOB crawl (T012b, T012d)."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from redis.typing import EncodableT, ExpiryT, KeyT

from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.detector.volume_state import VolumeVerdict, classify_volume
from polymarket_insider_tracker.ingestor.gamma_client import GammaCrawl
from polymarket_insider_tracker.ingestor.gamma_volume_refresh import GammaVolumeRefresh
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync, MetadataSyncError
from polymarket_insider_tracker.ingestor.models import Market, MarketMetadata, TradeEvent
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


class SimulatedScaledClock:
    """Coupled fake datetime clock with scaled time and manual offset support."""

    def __init__(self, start: datetime, scale: float = 10_000.0) -> None:
        self._start = start
        self._scale = scale
        self._t0 = time.monotonic()
        self._manual_offset: float = 0.0

    def now(self) -> datetime:
        elapsed = time.monotonic() - self._t0
        simulated_seconds = elapsed * self._scale + self._manual_offset
        return self._start + timedelta(seconds=simulated_seconds)

    def advance(self, seconds: float) -> None:
        self._manual_offset += seconds


class ScaledClockGamma(FakeGammaClient):
    """Gamma fake with crawls stamped from a coupled simulated clock."""

    def __init__(self, clock: SimulatedScaledClock) -> None:
        super().__init__({CID: volume_stats(acquired_at=clock.now())})
        self._sim_clock = clock
        self.crawls = 0
        self._failing_error: Exception | None = None

    def set_failing(self, err: Exception) -> None:
        self._failing_error = err

    async def crawl_active_markets(self) -> GammaCrawl:
        self.crawls += 1
        if self._failing_error is not None:
            raise self._failing_error
        now = self._sim_clock.now()
        stats = {CID: volume_stats(acquired_at=now)}
        return GammaCrawl(
            stats=stats,
            pages_ok=1,
            pages_failed=0,
            truncated=False,
            started=now,
            finished=now,
        )


def classify_market_volume(market: MarketMetadata | None, evaluated_at: datetime) -> VolumeVerdict:
    """Classify volume of a fetched market metadata record against evaluated_at."""
    if market is None:
        return classify_volume(None, evaluated_at=evaluated_at)
    return classify_volume(
        market.volume,
        evaluated_at=evaluated_at,
        unavailable_reason=market.volume_unavailable_reason,
    )


def _make_sample_trade(instant: datetime) -> TradeEvent:
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


async def _assert_positive_sample(
    sync: MarketMetadataSync,
    detector: SizeAnomalyDetector,
    clock: SimulatedScaledClock,
) -> None:
    now = clock.now()
    market = await sync.get_market(CID)
    assert market is not None and market.volume is not None
    daily_volume_acquired_at = datetime.fromisoformat(str(market.volume.acquired_at))
    assert 0 <= (now - daily_volume_acquired_at).total_seconds() <= 600

    verdict = classify_market_volume(market, evaluated_at=now)
    assert verdict.state == "available"

    trade = _make_sample_trade(now)
    size_eval = await detector.evaluate(trade, evaluated_at=now)
    assert size_eval.volume.verdict.state == "available"
    assert size_eval.signal is not None
    assert size_eval.signal.volume_impact_measured is not None


async def _assert_negative_sample(
    sync: MarketMetadataSync,
    detector: SizeAnomalyDetector,
    clock: SimulatedScaledClock,
) -> None:
    now = clock.now()
    market = await sync.get_market(CID)
    assert market is not None
    verdict = classify_market_volume(market, evaluated_at=now)
    assert verdict.state == "stale"
    assert verdict.reason in ("older_than_600s", "volume_age_exceeded")

    trade = _make_sample_trade(now)
    size_eval = await detector.evaluate(trade, evaluated_at=now)
    assert size_eval.volume.verdict.state == "stale"
    assert size_eval.volume.volume_impact_measured is None
    assert size_eval.signal is None


@pytest.mark.asyncio
async def test_slow_clob_fake_clock_steady_state_and_negative_control(
    fake_redis: FakeAsyncRedis,
) -> None:
    """T012d: steady-state with slow CLOB (D=204s) and healthy Gamma (S_g=180s) + negative control."""
    start_time = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)
    clock = SimulatedScaledClock(start_time, scale=10_000.0)
    gamma = ScaledClockGamma(clock)
    clob = SlowCountingClob([MARKET], seconds=0.0204)

    sync = MarketMetadataSync(
        redis=fake_redis,
        clob_client=clob,
        gamma_client=gamma,
        sync_interval_seconds=0,
        gamma_refresh_interval_seconds=0.018,
    )
    detector = SizeAnomalyDetector(sync, clock=clock.now)

    await sync.start()
    try:
        for _ in range(4):
            await asyncio.sleep(0.04)
            await _assert_positive_sample(sync, detector, clock)

        assert clob.calls >= 3
        assert gamma.crawls >= 6
        assert (clock.now() - start_time).total_seconds() > 1200

        gamma.set_failing(RuntimeError("gamma degraded"))
        clock.advance(601.0)
        calls_before = clob.calls
        await asyncio.sleep(0.03)
        assert clob.calls > calls_before
        await _assert_negative_sample(sync, detector, clock)
    finally:
        await sync.stop()
