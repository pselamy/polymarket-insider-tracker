"""Delivery-claim races and dedup-state failures around one dispatched channel send.

Every double runs its commands on a real ``fakeredis`` server through ``AlertHistory``; the
doubles only interleave a rival dispatcher process (or an outage) at one point of the claim
lifecycle, so the outcome is read back from the shared Redis state and the channel itself.
"""

from __future__ import annotations

import logging

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.alerter.dispatcher import AlertDispatcher
from polymarket_insider_tracker.alerter.history import AlertHistory
from polymarket_insider_tracker.alerter.models import FormattedAlert
from tests.fakes import resist_cancellation_for
from tests.fakes.alerts import FakeAlertChannel
from tests.fakes.redis_history import DelegatingPipeline, DelegatingRedis

CHANNEL = "discord"
WALLET = "0xWallet"
MARKET = "0xMarket"
RIVAL_TOKEN = "rival-owner"
DISPATCHER_LOGGER = "polymarket_insider_tracker.alerter.dispatcher"


def _alert() -> FormattedAlert:
    return FormattedAlert(
        title="High Risk Insider Alert",
        body="Suspicious trade detected",
        discord_embed={"title": "Alert"},
        telegram_markdown="*Alert*",
        plain_text="Alert",
    )


def _history(redis: DelegatingRedis) -> AlertHistory:
    return AlertHistory(redis, dedup_window_hours=1)


def _claim_key() -> str:
    return _history(DelegatingRedis(FakeAsyncRedis())).get_channel_ambiguous_key(
        CHANNEL, WALLET, MARKET
    )


def _dedup_key() -> str:
    return _history(DelegatingRedis(FakeAsyncRedis())).get_channel_dedup_key(
        CHANNEL, WALLET, MARKET
    )


async def _dispatch(dispatcher: AlertDispatcher) -> str:
    result = await dispatcher.dispatch(_alert(), WALLET, MARKET)
    return result.channel_statuses[CHANNEL]


class RivalClaimsFirstRedis(DelegatingRedis):
    """Another dispatcher process acquires the claim between the suppression check and ours."""

    async def set(
        self, name: str, value: str, *, ex: int | None = None, nx: bool = False
    ) -> object:
        if nx:
            await self._backend.set(name, RIVAL_TOKEN, ex=ex)
        return await self._backend.set(name, value, ex=ex, nx=nx)


class AfterClaimRedis(DelegatingRedis):
    """Runs ``after_claim`` once this dispatch's ``SET NX`` claim command has completed."""

    async def set(
        self, name: str, value: str, *, ex: int | None = None, nx: bool = False
    ) -> object:
        acquired = await self._backend.set(name, value, ex=ex, nx=nx)
        if nx:
            await self.after_claim()
        return acquired

    async def after_claim(self) -> None:
        """Nothing happens after the claim unless a subclass says so."""


class RivalDeliversUnderClaimRedis(AfterClaimRedis):
    """Another dispatcher confirms delivery right after this dispatch acquires its claim."""

    async def after_claim(self) -> None:
        await self._backend.set(_dedup_key(), "rival-delivered", ex=3600)


class RecheckFailsRedis(DelegatingRedis):
    """Redis becomes unreachable for reads once this dispatch holds its claim."""

    claimed = False

    async def set(
        self, name: str, value: str, *, ex: int | None = None, nx: bool = False
    ) -> object:
        self.claimed = self.claimed or nx
        return await self._backend.set(name, value, ex=ex, nx=nx)

    async def exists(self, *names: str) -> object:
        if self.claimed:
            raise ConnectionError("redis unavailable")
        return await self._backend.exists(*names)


class ClaimFailsRedis(DelegatingRedis):
    """The atomic claim command fails while plain writes still succeed."""

    async def set(
        self, name: str, value: str, *, ex: int | None = None, nx: bool = False
    ) -> object:
        if nx:
            raise ConnectionError("redis unavailable")
        return await self._backend.set(name, value, ex=ex, nx=nx)


class TransactionsFailRedis(DelegatingRedis):
    """The compare-and-delete/expire transactions behind claim release and renewal fail."""

    def pipeline(self, transaction: bool = True) -> DelegatingPipeline:
        raise ConnectionError(f"redis unavailable (transaction={transaction})")


class DetachOnClaimRedis(AfterClaimRedis):
    """The operator detaches dedup state from the dispatcher while its claim is in flight."""

    dispatcher: AlertDispatcher | None = None

    async def after_claim(self) -> None:
        if self.dispatcher is not None:
            self.dispatcher.history = None


class TakeoverChannel:
    """A send during which a rival overwrites the claim, optionally resisting cancellation."""

    def __init__(self, backend: FakeAsyncRedis, *, resist_seconds: float = 0.0) -> None:
        self.name = CHANNEL
        self.backend = backend
        self.resist_seconds = resist_seconds
        self.sends = 0

    async def send(self, alert: FormattedAlert) -> bool:
        _ = alert
        self.sends += 1
        await self.backend.set(_claim_key(), RIVAL_TOKEN, ex=60)
        await resist_cancellation_for(self.resist_seconds)
        return True


class FailingChannel:
    """A send that raises a transport error after resisting cancellation for a while."""

    def __init__(self, *, resist_seconds: float = 0.0) -> None:
        self.name = CHANNEL
        self.resist_seconds = resist_seconds
        self.sends = 0

    async def send(self, alert: FormattedAlert) -> bool:
        _ = alert
        self.sends += 1
        await resist_cancellation_for(self.resist_seconds)
        raise ConnectionError("webhook connection reset")


def _guarded_dispatcher(
    channel: TakeoverChannel | FailingChannel, redis: DelegatingRedis
) -> AlertDispatcher:
    """A 1-second lease (renewed every 0.25 s) with a deadline that abandons slow sends."""
    return AlertDispatcher(
        [channel], history=_history(redis), claim_ttl_seconds=1, send_deadline_seconds=0.05
    )


@pytest.mark.asyncio
async def test_rival_claim_after_the_check_suppresses_the_send() -> None:
    backend = FakeAsyncRedis()
    channel = FakeAlertChannel(CHANNEL)
    dispatcher = AlertDispatcher([channel], history=_history(RivalClaimsFirstRedis(backend)))

    assert await _dispatch(dispatcher) == "ambiguous_timeout"
    assert channel.deliveries == []
    assert await backend.get(_claim_key()) == RIVAL_TOKEN.encode()


@pytest.mark.asyncio
async def test_rival_delivery_under_the_claim_is_a_duplicate_and_releases_it() -> None:
    backend = FakeAsyncRedis()
    channel = FakeAlertChannel(CHANNEL)
    dispatcher = AlertDispatcher([channel], history=_history(RivalDeliversUnderClaimRedis(backend)))

    result = await dispatcher.dispatch(_alert(), WALLET, MARKET)

    assert result.channel_statuses == {CHANNEL: "duplicate"}
    assert result.disposition == "duplicate"
    assert channel.deliveries == []
    assert await backend.exists(_claim_key()) == 0
    assert await backend.exists(_dedup_key()) == 1


@pytest.mark.asyncio
async def test_failed_recheck_under_the_claim_favors_delivery() -> None:
    channel = FakeAlertChannel(CHANNEL)
    dispatcher = AlertDispatcher([channel], history=_history(RecheckFailsRedis(FakeAsyncRedis())))

    assert await _dispatch(dispatcher) == "delivered"
    assert len(channel.deliveries) == 1


@pytest.mark.asyncio
async def test_unverified_claim_still_records_the_confirmed_delivery() -> None:
    backend = FakeAsyncRedis()
    channel = FakeAlertChannel(CHANNEL)
    dispatcher = AlertDispatcher([channel], history=_history(ClaimFailsRedis(backend)))

    assert await _dispatch(dispatcher) == "delivered"
    assert len(channel.deliveries) == 1
    assert await backend.exists(_dedup_key()) == 1
    assert await backend.exists(_claim_key()) == 0


@pytest.mark.asyncio
async def test_claim_taken_over_during_the_send_is_left_to_its_new_owner(
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = FakeAsyncRedis()
    channel = TakeoverChannel(backend)
    dispatcher = AlertDispatcher([channel], history=_history(DelegatingRedis(backend)))

    with caplog.at_level(logging.WARNING, logger=DISPATCHER_LOGGER):
        assert await _dispatch(dispatcher) == "delivered"

    assert await backend.get(_claim_key()) == RIVAL_TOKEN.encode()
    assert await backend.exists(_dedup_key()) == 1
    assert "expired or changed owner before release" in caplog.text


@pytest.mark.asyncio
async def test_detached_history_mid_claim_still_sends_and_leaves_the_lease() -> None:
    backend = FakeAsyncRedis()
    redis = DetachOnClaimRedis(backend)
    channel = FakeAlertChannel(CHANNEL)
    dispatcher = AlertDispatcher([channel], history=_history(redis))
    redis.dispatcher = dispatcher

    assert await _dispatch(dispatcher) == "delivered"
    assert dispatcher.history is None
    assert len(channel.deliveries) == 1
    assert await backend.exists(_dedup_key()) == 0
    assert 0 < await backend.ttl(_claim_key()) <= 60


@pytest.mark.asyncio
async def test_raising_channel_is_a_confirmed_failure(caplog: pytest.LogCaptureFixture) -> None:
    dispatcher = AlertDispatcher([FailingChannel()])

    with caplog.at_level(logging.ERROR, logger=DISPATCHER_LOGGER):
        result = await dispatcher.dispatch(_alert())

    assert result.channel_statuses == {CHANNEL: "failed"}
    assert result.disposition == "failed"
    assert result.failure_count == 1
    assert dispatcher.get_circuit_status()[CHANNEL]["failure_count"] == 1
    assert "Error sending to discord: webhook connection reset" in caplog.text


@pytest.mark.asyncio
async def test_failed_half_open_attempt_is_counted_and_keeps_the_circuit_open() -> None:
    channel = FakeAlertChannel(CHANNEL, accepting=False)
    dispatcher = AlertDispatcher([channel], failure_threshold=1, recovery_timeout_seconds=0)

    first = await dispatcher.dispatch(_alert())
    second = await dispatcher.dispatch(_alert())

    assert (first.channel_statuses[CHANNEL], second.channel_statuses[CHANNEL]) == (
        "failed",
        "failed",
    )
    assert len(channel.deliveries) == 2
    circuit = dispatcher.get_circuit_status()[CHANNEL]
    assert circuit["is_open"] is True
    assert circuit["half_open_attempts"] == 1


@pytest.mark.asyncio
async def test_abandoned_send_that_raises_under_an_unverified_claim_keeps_the_window(
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = FakeAsyncRedis()
    channel = FailingChannel(resist_seconds=0.4)
    dispatcher = _guarded_dispatcher(channel, ClaimFailsRedis(backend))

    with caplog.at_level(logging.DEBUG, logger=DISPATCHER_LOGGER):
        assert await _dispatch(dispatcher) == "ambiguous"
        await dispatcher.join_abandoned_sends()

    assert channel.sends == 1
    assert await backend.exists(_claim_key()) == 1
    assert await backend.exists(_dedup_key()) == 0
    assert "Abandoned send to discord terminated with ConnectionError" in caplog.text


@pytest.mark.asyncio
async def test_failed_claim_renewal_is_logged_and_the_window_still_recorded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = FakeAsyncRedis()
    channel = TakeoverChannel(FakeAsyncRedis(), resist_seconds=0.4)
    dispatcher = _guarded_dispatcher(channel, TransactionsFailRedis(backend))

    with caplog.at_level(logging.WARNING, logger=DISPATCHER_LOGGER):
        assert await _dispatch(dispatcher) == "ambiguous"
        await dispatcher.join_abandoned_sends()

    assert await backend.exists(_claim_key()) == 1
    assert "Failed to renew delivery claim for discord" in caplog.text


@pytest.mark.asyncio
async def test_claim_lost_while_the_abandoned_send_runs_warns_of_a_duplicate(
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = FakeAsyncRedis()
    channel = TakeoverChannel(backend, resist_seconds=0.4)
    dispatcher = _guarded_dispatcher(channel, DelegatingRedis(backend))

    with caplog.at_level(logging.WARNING, logger=DISPATCHER_LOGGER):
        assert await _dispatch(dispatcher) == "ambiguous"
        await dispatcher.join_abandoned_sends()

    assert channel.sends == 1
    assert await backend.get(_claim_key()) != RIVAL_TOKEN.encode()
    assert "changed owner while its send was still running" in caplog.text


@pytest.mark.asyncio
async def test_failed_claim_release_after_delivery_leaves_a_bounded_lease(
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = FakeAsyncRedis()
    channel = FakeAlertChannel(CHANNEL)
    dispatcher = AlertDispatcher([channel], history=_history(TransactionsFailRedis(backend)))

    with caplog.at_level(logging.WARNING, logger=DISPATCHER_LOGGER):
        assert await _dispatch(dispatcher) == "delivered"

    assert await backend.exists(_dedup_key()) == 1
    assert 0 < await backend.ttl(_claim_key()) <= 60
    assert "Failed to release delivery claim for discord" in caplog.text
