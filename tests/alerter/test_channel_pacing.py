"""Characterization tests for the channels' rate-limit wait and retry backoff.

These tests pin the delivery-pacing behaviour both channel implementations
express today — the sliding one-minute send window and the exponential backoff
between retries — for BOTH call sites, so the knowledge can move to its single
home without any timing change. Transport-failure redaction is already pinned
behaviourally for both channels by ``TestChannelErrorRedaction`` in
``tests/alerter/test_dispatcher.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.channels.telegram import TelegramChannel
from polymarket_insider_tracker.alerter.models import FormattedAlert
from tests.fakes.alerts import FakeWebhookServer, discord_webhook, telegram_bot_api

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/123/abc"


class _PacingClock:
    """A controllable stand-in for the running loop's ``time()``.

    The channels read elapsed time only through
    ``asyncio.get_event_loop().time()``, which inside the test coroutine is the
    running loop itself, so the ``pacing_clock`` fixture swaps that one method
    for this clock and a test advances the sliding window without sleeping.
    """

    def __init__(self) -> None:
        self._now = 1000.0

    def time(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


class ChannelHarness:
    """Builds one real channel with its matching fake server for a pacing test."""

    def __init__(self, kind: str) -> None:
        self._kind = kind

    def channel(
        self, *, rate_limit_per_minute: int, max_retries: int, retry_delay: float
    ) -> DiscordChannel | TelegramChannel:
        if self._kind == "discord":
            return DiscordChannel(
                webhook_url=DISCORD_WEBHOOK_URL,
                rate_limit_per_minute=rate_limit_per_minute,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )
        return TelegramChannel(
            "test-bot-token",
            "chat-1",
            rate_limit_per_minute=rate_limit_per_minute,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

    def server(self, *, network_error: Exception | None = None) -> FakeWebhookServer:
        if self._kind == "discord":
            return discord_webhook(network_error=network_error)
        return telegram_bot_api(network_error=network_error)


@pytest.fixture
async def pacing_clock(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_PacingClock]:
    """Route the channels' elapsed-time reads through a controllable clock."""
    clock = _PacingClock()
    monkeypatch.setattr(asyncio.get_running_loop(), "time", clock.time)
    yield clock


@pytest.fixture
def recorded_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Collect every ``asyncio.sleep`` delay without actually sleeping."""
    sleeps: list[float] = []

    async def record(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", record)
    return sleeps


@pytest.fixture
def pacing_alert() -> FormattedAlert:
    """A minimal formatted alert; pacing tests never inspect its content."""
    return FormattedAlert(
        title="Pacing probe",
        body="Pacing body",
        discord_embed={"title": "Pacing probe"},
        telegram_markdown="*Pacing probe*",
        plain_text="PACING PROBE",
        links={"market": "https://polymarket.com/pacing-probe"},
    )


class TestRateLimitWindow:
    """The local sliding window paces each channel's sends before any request."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["discord", "telegram"])
    async def test_full_window_waits_until_the_oldest_request_expires(
        self,
        kind: str,
        pacing_alert: FormattedAlert,
        pacing_clock: _PacingClock,
        recorded_sleeps: list[float],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A full window delays the next send by exactly the oldest windowed
        request's remaining seconds, and the delayed send is still delivered."""
        harness = ChannelHarness(kind)
        channel = harness.channel(rate_limit_per_minute=2, max_retries=1, retry_delay=0.0)
        server = harness.server()
        monkeypatch.setattr(httpx, "AsyncClient", server.client_factory(httpx.AsyncClient))

        assert await channel.send(pacing_alert) is True
        pacing_clock.advance(50.0)
        assert await channel.send(pacing_alert) is True
        pacing_clock.advance(5.0)
        assert await channel.send(pacing_alert) is True

        assert recorded_sleeps == [5.0]
        assert len(server.requests) == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["discord", "telegram"])
    async def test_expired_window_slots_do_not_delay_the_next_send(
        self,
        kind: str,
        pacing_alert: FormattedAlert,
        pacing_clock: _PacingClock,
        recorded_sleeps: list[float],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A window that is full by count but whose requests have all expired
        no longer delays a send: no sleep is recorded and delivery proceeds."""
        harness = ChannelHarness(kind)
        channel = harness.channel(rate_limit_per_minute=2, max_retries=1, retry_delay=0.0)
        server = harness.server()
        monkeypatch.setattr(httpx, "AsyncClient", server.client_factory(httpx.AsyncClient))

        assert await channel.send(pacing_alert) is True
        pacing_clock.advance(30.0)
        assert await channel.send(pacing_alert) is True
        pacing_clock.advance(31.0)
        assert await channel.send(pacing_alert) is True

        assert recorded_sleeps == []
        assert len(server.requests) == 3


class TestRetryBackoff:
    """The backoff between attempts doubles the base delay and never sleeps
    after the final permitted attempt."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["discord", "telegram"])
    async def test_backoff_doubles_between_attempts_and_skips_after_the_last(
        self,
        kind: str,
        pacing_alert: FormattedAlert,
        recorded_sleeps: list[float],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Three permitted attempts with a base delay of 0.5 sleep 0.5 then 1.0
        across three failed attempts, and the final attempt sleeps never."""
        harness = ChannelHarness(kind)
        channel = harness.channel(rate_limit_per_minute=30, max_retries=3, retry_delay=0.5)
        server = harness.server(network_error=httpx.ConnectError("connect failed"))
        monkeypatch.setattr(httpx, "AsyncClient", server.client_factory(httpx.AsyncClient))

        result = await channel.send(pacing_alert)

        assert result is False
        assert recorded_sleeps == [0.5, 1.0]
        assert len(server.requests) == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["discord", "telegram"])
    async def test_single_attempt_never_backs_off(
        self,
        kind: str,
        pacing_alert: FormattedAlert,
        recorded_sleeps: list[float],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """With one permitted attempt there is no retry to space out: no sleep,
        exactly one request, confirmed failure."""
        harness = ChannelHarness(kind)
        channel = harness.channel(rate_limit_per_minute=30, max_retries=1, retry_delay=0.5)
        server = harness.server(network_error=httpx.ConnectError("connect failed"))
        monkeypatch.setattr(httpx, "AsyncClient", server.client_factory(httpx.AsyncClient))

        result = await channel.send(pacing_alert)

        assert result is False
        assert recorded_sleeps == []
        assert len(server.requests) == 1
