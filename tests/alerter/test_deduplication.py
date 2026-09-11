"""Tests for channel-scoped delivery deduplication, dry-run safety, and ambiguity semantics.

Tests cover:
1. FR-009 / G-018: Deduplication evaluated per channel with key pattern alert:dedup:{channel}:{wallet}:{market}
2. FR-010: Confirmed success (HTTP 2xx) writes channel dedup key with configured TTL
3. FR-011 / G-019: Confirmed failure does not write dedup key, leaving channel immediately eligible for retry
4. FR-017 / G-019: Ambiguous timeout writes alert:ambiguous:{channel}:{wallet}:{market} with 60s TTL
5. FR-018 / G-020: Dry-run mode makes zero network calls and writes zero Redis keys
6. SC-003: Dry-run dispatch disposition is 'dry_run'
7. SC-004: Multi-channel partial failure leaves failed channel retryable
8. SC-008: 60-second ambiguity window suppresses double-delivery and allows subsequent attempt
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.dispatcher import AlertDispatcher
from polymarket_insider_tracker.alerter.history import AlertHistory
from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.ingestor.models import TradeEvent
from tests.fakes.alerts import discord_webhook


class FakeChannel:
    """Working fake channel for testing delivery outcomes without unittest.mock."""

    def __init__(
        self,
        name: str,
        *,
        should_fail: bool = False,
        should_timeout: bool = False,
    ) -> None:
        self.name = name
        self.should_fail = should_fail
        self.should_timeout = should_timeout
        self.calls: list[FormattedAlert] = []

    async def send(self, alert: FormattedAlert) -> bool:
        self.calls.append(alert)
        if self.should_timeout:
            raise TimeoutError(f"Connection to {self.name} timed out")
        return not self.should_fail


def _make_sample_alert() -> FormattedAlert:
    return FormattedAlert(
        title="High Risk Insider Alert",
        body="Suspicious trade detected",
        discord_embed={"title": "Alert"},
        telegram_markdown="*Alert*",
        plain_text="Alert",
    )


def _make_sample_assessment(
    wallet: str = "0x1234567890abcdef1234567890abcdef12345678",
    market: str = "0xmarket_condition_id",
) -> RiskAssessment:
    trade = TradeEvent(
        market_id=market,
        trade_id="trade_123",
        wallet_address=wallet,
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.50"),
        size=Decimal("10000"),
        timestamp=datetime.now(UTC),
        asset_id="token_123",
    )
    return RiskAssessment(
        trade_event=trade,
        wallet_address=wallet,
        market_id=market,
        signals_triggered=2,
        weighted_score=0.85,
        should_alert=True,
    )


@pytest.fixture
def fake_redis() -> FakeAsyncRedis:
    return FakeAsyncRedis()


@pytest.fixture
def alert_history(fake_redis: FakeAsyncRedis) -> AlertHistory:
    return AlertHistory(fake_redis, dedup_window_hours=1)


class TestChannelScopedDeduplication:
    """Test suite for channel-scoped deduplication and retry semantics."""

    @pytest.mark.asyncio
    async def test_successful_delivery_writes_channel_scoped_keys(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        """Confirmed success writes separate alert:dedup:{channel}:{wallet}:{market} keys."""
        discord = FakeChannel("discord")
        telegram = FakeChannel("telegram")
        dispatcher = AlertDispatcher([discord, telegram], history=alert_history)

        assessment = _make_sample_assessment()
        alert = _make_sample_alert()

        result = await dispatcher.dispatch(alert, assessment=assessment)

        assert result.all_succeeded is True
        assert len(discord.calls) == 1
        assert len(telegram.calls) == 1

        discord_key = (
            f"alert:dedup:discord:{assessment.wallet_address.lower()}:{assessment.market_id}"
        )
        telegram_key = (
            f"alert:dedup:telegram:{assessment.wallet_address.lower()}:{assessment.market_id}"
        )

        assert await fake_redis.exists(discord_key) == 1
        assert await fake_redis.exists(telegram_key) == 1

        discord_ttl = await fake_redis.ttl(discord_key)
        assert 0 < discord_ttl <= 3600

    @pytest.mark.asyncio
    async def test_second_attempt_is_suppressed_as_duplicate(
        self, alert_history: AlertHistory
    ) -> None:
        """When keys exist for all channels, second dispatch suppresses delivery with duplicate disposition."""
        discord = FakeChannel("discord")
        telegram = FakeChannel("telegram")
        dispatcher = AlertDispatcher([discord, telegram], history=alert_history)

        assessment = _make_sample_assessment()
        alert = _make_sample_alert()

        result1 = await dispatcher.dispatch(alert, assessment=assessment)
        assert result1.disposition == "delivered"
        assert len(discord.calls) == 1
        assert len(telegram.calls) == 1

        result2 = await dispatcher.dispatch(alert, assessment=assessment)
        assert result2.disposition == "duplicate"
        assert len(discord.calls) == 1
        assert len(telegram.calls) == 1
        assert result2.channel_statuses == {"discord": "duplicate", "telegram": "duplicate"}

    @pytest.mark.asyncio
    async def test_partial_failure_leaves_failed_channel_eligible_for_retry(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        """When Discord succeeds and Telegram fails, Discord key is written, Telegram key is not."""
        discord = FakeChannel("discord")
        telegram = FakeChannel("telegram", should_fail=True)
        dispatcher = AlertDispatcher([discord, telegram], history=alert_history)

        assessment = _make_sample_assessment()
        alert = _make_sample_alert()

        result1 = await dispatcher.dispatch(alert, assessment=assessment)
        assert result1.disposition == "partial_failure"
        assert result1.channel_statuses["discord"] == "delivered"
        assert result1.channel_statuses["telegram"] == "failed"

        discord_key = (
            f"alert:dedup:discord:{assessment.wallet_address.lower()}:{assessment.market_id}"
        )
        telegram_key = (
            f"alert:dedup:telegram:{assessment.wallet_address.lower()}:{assessment.market_id}"
        )
        assert await fake_redis.exists(discord_key) == 1
        assert await fake_redis.exists(telegram_key) == 0

        # Now fix telegram channel and dispatch again
        telegram.should_fail = False
        result2 = await dispatcher.dispatch(alert, assessment=assessment)

        # Discord was suppressed (duplicate); Telegram was retried and succeeded
        assert len(discord.calls) == 1
        assert len(telegram.calls) == 2
        assert result2.channel_statuses["discord"] == "duplicate"
        assert result2.channel_statuses["telegram"] == "delivered"
        assert await fake_redis.exists(telegram_key) == 1

    @pytest.mark.asyncio
    async def test_dry_run_makes_zero_calls_and_writes_zero_keys(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        """Dry-run mode performs zero network requests and writes zero keys to Redis."""
        discord = FakeChannel("discord")
        telegram = FakeChannel("telegram")
        dispatcher = AlertDispatcher([discord, telegram], history=alert_history, dry_run=True)

        assessment = _make_sample_assessment()
        alert = _make_sample_alert()

        result = await dispatcher.dispatch(alert, assessment=assessment)

        assert result.dry_run is True
        assert result.disposition == "dry_run"
        assert len(discord.calls) == 0
        assert len(telegram.calls) == 0
        assert await fake_redis.dbsize() == 0

    @pytest.mark.asyncio
    async def test_ambiguity_timeout_sets_60s_key_and_suppresses_then_allows(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        """Ambiguous timeout writes alert:ambiguous with 60s TTL; suppresses retry until expiry."""
        discord = FakeChannel("discord", should_timeout=True)
        dispatcher = AlertDispatcher([discord], history=alert_history)

        assessment = _make_sample_assessment()
        alert = _make_sample_alert()

        # First dispatch times out
        result1 = await dispatcher.dispatch(alert, assessment=assessment)
        assert result1.channel_statuses["discord"] == "ambiguous"

        ambiguous_key = (
            f"alert:ambiguous:discord:{assessment.wallet_address.lower()}:{assessment.market_id}"
        )
        dedup_key = (
            f"alert:dedup:discord:{assessment.wallet_address.lower()}:{assessment.market_id}"
        )

        assert await fake_redis.exists(ambiguous_key) == 1
        assert await fake_redis.exists(dedup_key) == 0
        ttl = await fake_redis.ttl(ambiguous_key)
        assert 0 < ttl <= 60

        # Second immediate dispatch is suppressed while ambiguity key is active
        result2 = await dispatcher.dispatch(alert, assessment=assessment)
        assert len(discord.calls) == 1  # Not called again
        assert result2.channel_statuses["discord"] == "ambiguous_timeout"

        # Simulate expiry of ambiguity key (TTL expires after 60s)
        await fake_redis.delete(ambiguous_key)
        discord.should_timeout = False

        # Third dispatch after expiry is eligible for retry
        result3 = await dispatcher.dispatch(alert, assessment=assessment)
        assert len(discord.calls) == 2
        assert result3.channel_statuses["discord"] == "delivered"
        assert await fake_redis.exists(dedup_key) == 1

    @pytest.mark.asyncio
    async def test_wallet_address_is_normalized_to_lowercase(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        """Mixed-case wallet addresses normalize to lowercase in dedup keys."""
        discord = FakeChannel("discord")
        dispatcher = AlertDispatcher([discord], history=alert_history)

        mixed_wallet = "0xAbCdEf1234567890AbCdEf1234567890AbCdEf12"
        lower_wallet = mixed_wallet.lower()
        market = "0xmarket_cond"

        assessment = _make_sample_assessment(wallet=mixed_wallet, market=market)
        alert = _make_sample_alert()

        await dispatcher.dispatch(alert, assessment=assessment)

        expected_key = f"alert:dedup:discord:{lower_wallet}:{market}"
        assert await fake_redis.exists(expected_key) == 1

    @pytest.mark.asyncio
    async def test_real_channel_read_timeout_is_ambiguous_not_failed(
        self,
        fake_redis: FakeAsyncRedis,
        alert_history: AlertHistory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A production channel read timeout must reach the ambiguous path, not confirmed failure."""
        channel = DiscordChannel(
            webhook_url="https://discord.com/api/webhooks/123/abc",
            max_retries=3,
            retry_delay=0.01,
        )
        server = discord_webhook(network_error=httpx.ReadTimeout("read timed out"))
        monkeypatch.setattr(httpx, "AsyncClient", server.client_factory(httpx.AsyncClient))
        dispatcher = AlertDispatcher([channel], history=alert_history)

        assessment = _make_sample_assessment()
        result = await dispatcher.dispatch(_make_sample_alert(), assessment=assessment)

        # Exactly one HTTP attempt: no channel-internal re-post of a possibly accepted payload
        assert len(server.requests) == 1
        assert result.channel_statuses["discord"] == "ambiguous"
        assert result.disposition == "ambiguous"

        ambiguous_key = (
            f"alert:ambiguous:discord:{assessment.wallet_address.lower()}:{assessment.market_id}"
        )
        dedup_key = (
            f"alert:dedup:discord:{assessment.wallet_address.lower()}:{assessment.market_id}"
        )
        assert await fake_redis.exists(ambiguous_key) == 1
        assert await fake_redis.exists(dedup_key) == 0

    @pytest.mark.asyncio
    async def test_dedup_ttl_honors_configured_window_seconds(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        """The dedup key TTL must match dedup_window_seconds, not an hour-rounded value."""
        history = AlertHistory(fake_redis, dedup_window_seconds=1800)
        await history.record_channel_delivery("discord", "0xAbC", "market1")

        key = history.get_channel_dedup_key("discord", "0xAbC", "market1")
        ttl = await fake_redis.ttl(key)
        assert 0 < ttl <= 1800

    @pytest.mark.asyncio
    async def test_dedup_state_outage_does_not_block_delivery(self) -> None:
        """A Redis outage during dispatch must not raise or block the delivery attempt."""

        class BrokenRedis:
            async def exists(self, _key: str) -> int:
                raise ConnectionError("redis unavailable")

            async def set(self, *_args: object, **_kwargs: object) -> None:
                raise ConnectionError("redis unavailable")

        history = AlertHistory(BrokenRedis(), dedup_window_hours=1)
        discord = FakeChannel("discord")
        dispatcher = AlertDispatcher([discord], history=history)

        result = await dispatcher.dispatch(
            _make_sample_alert(), assessment=_make_sample_assessment()
        )

        assert len(discord.calls) == 1
        assert result.channel_statuses["discord"] == "delivered"
        assert result.disposition == "delivered"


class YieldingChannel:
    """Working fake whose send yields to the event loop, exposing check-then-act races."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.sends = 0

    async def send(self, alert: FormattedAlert) -> bool:
        _ = alert
        await asyncio.sleep(0.01)
        self.sends += 1
        return True


class TestConcurrentDispatchAtomicity:
    """The suppression check and send must be atomic per delivery identity (G-018, FR-010)."""

    @pytest.mark.asyncio
    async def test_concurrent_dispatch_of_same_identity_delivers_exactly_once(
        self, alert_history: AlertHistory
    ) -> None:
        """Two concurrent dispatches for one identity must not both send to a channel."""
        channel = YieldingChannel("discord")
        dispatcher = AlertDispatcher([channel], history=alert_history)
        assessment = _make_sample_assessment()
        alert = _make_sample_alert()

        first, second = await asyncio.gather(
            dispatcher.dispatch(alert, assessment=assessment),
            dispatcher.dispatch(alert, assessment=assessment),
        )

        assert channel.sends == 1
        dispositions = sorted((first.disposition, second.disposition))
        assert dispositions == ["ambiguous", "delivered"]

    @pytest.mark.asyncio
    async def test_confirmed_failure_leaves_no_claim_and_stays_immediately_eligible(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        """A confirmed failure must release every delivery key so retry is immediate."""
        channel = FakeChannel("discord", should_fail=True)
        dispatcher = AlertDispatcher([channel], history=alert_history)
        assessment = _make_sample_assessment()
        alert = _make_sample_alert()

        result = await dispatcher.dispatch(alert, assessment=assessment)

        assert result.channel_statuses["discord"] == "failed"
        assert await fake_redis.dbsize() == 0

        channel.should_fail = False
        retry = await dispatcher.dispatch(alert, assessment=assessment)
        assert retry.channel_statuses["discord"] == "delivered"
        assert len(channel.calls) == 2


class SlowChannel:
    """Working fake whose send takes longer than the configured deadline."""

    def __init__(self, name: str, delay: float) -> None:
        self.name = name
        self.delay = delay
        self.completed_sends = 0

    async def send(self, alert: FormattedAlert) -> bool:
        _ = alert
        await asyncio.sleep(self.delay)
        self.completed_sends += 1
        return True


class TestClaimOwnershipAndLease:
    """The in-flight claim is owned, lease-bounded, and safe across expiry (G-018 residual)."""

    @pytest.mark.asyncio
    async def test_claim_returns_unique_ownership_token(self, alert_history: AlertHistory) -> None:
        token = await alert_history.claim_channel_send("discord", "0xW", "0xM")
        assert token is not None
        assert await alert_history.claim_channel_send("discord", "0xW", "0xM") is None

    @pytest.mark.asyncio
    async def test_stale_owner_cannot_release_a_newer_claim(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        """After the lease expires and another dispatch claims, the stale owner's release
        must leave the new claim untouched (compare-and-delete, not unconditional DEL)."""
        stale_token = await alert_history.claim_channel_send("discord", "0xW", "0xM", ttl=1)
        assert stale_token is not None
        await asyncio.sleep(1.1)

        new_token = await alert_history.claim_channel_send("discord", "0xW", "0xM", ttl=60)
        assert new_token is not None

        released = await alert_history.release_channel_claim("discord", "0xW", "0xM", stale_token)

        assert released is False
        key = alert_history.get_channel_ambiguous_key("discord", "0xW", "0xM")
        assert await fake_redis.get(key) == new_token.encode()

    @pytest.mark.asyncio
    async def test_owner_release_deletes_only_its_own_claim(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        token = await alert_history.claim_channel_send("discord", "0xW", "0xM")
        assert token is not None
        assert await alert_history.release_channel_claim("discord", "0xW", "0xM", token) is True
        assert await fake_redis.dbsize() == 0

    @pytest.mark.asyncio
    async def test_expired_claim_leaves_identity_eligible_again(
        self, alert_history: AlertHistory
    ) -> None:
        first = await alert_history.claim_channel_send("discord", "0xW", "0xM", ttl=1)
        assert first is not None
        await asyncio.sleep(1.1)
        second = await alert_history.claim_channel_send("discord", "0xW", "0xM", ttl=1)
        assert second is not None
        assert second != first

    @pytest.mark.asyncio
    async def test_send_deadline_must_stay_inside_claim_lease(self) -> None:
        with pytest.raises(ValueError):
            AlertDispatcher([], claim_ttl_seconds=1, send_deadline_seconds=1.0)

    @pytest.mark.asyncio
    async def test_send_is_cut_off_before_the_claim_can_expire(
        self, fake_redis: FakeAsyncRedis, alert_history: AlertHistory
    ) -> None:
        """A send slower than the deadline becomes an ambiguous outcome under a live claim,
        so no concurrent dispatch can send the same identity while it is still in flight."""
        channel = SlowChannel("discord", delay=10.0)
        dispatcher = AlertDispatcher(
            [channel],
            history=alert_history,
            claim_ttl_seconds=2,
            send_deadline_seconds=0.1,
        )
        assessment = _make_sample_assessment()

        result = await asyncio.wait_for(
            dispatcher.dispatch(_make_sample_alert(), assessment=assessment), timeout=1.0
        )

        assert result.channel_statuses["discord"] == "ambiguous"
        assert channel.completed_sends == 0
        key = alert_history.get_channel_ambiguous_key(
            "discord", assessment.wallet_address, assessment.market_id
        )
        assert await fake_redis.exists(key) == 1

    @pytest.mark.asyncio
    async def test_slow_send_and_lease_expiry_never_double_deliver(
        self, alert_history: AlertHistory
    ) -> None:
        """Phase-3 reproduction: with a 1-second lease and a send outlasting it, two
        dispatches of one identity used to both deliver. The send deadline now cuts the
        first attempt off inside its lease and the second dispatch stays suppressed."""
        channel = SlowChannel("discord", delay=1.5)
        dispatcher = AlertDispatcher(
            [channel],
            history=alert_history,
            claim_ttl_seconds=1,
            send_deadline_seconds=0.5,
        )
        assessment = _make_sample_assessment()
        alert = _make_sample_alert()

        first_task = asyncio.create_task(dispatcher.dispatch(alert, assessment=assessment))
        await asyncio.sleep(0.2)
        second_task = asyncio.create_task(dispatcher.dispatch(alert, assessment=assessment))
        first, second = await asyncio.gather(first_task, second_task)

        assert channel.completed_sends == 0
        statuses = sorted((first.channel_statuses["discord"], second.channel_statuses["discord"]))
        assert statuses == ["ambiguous", "ambiguous_timeout"]


class TestSuppressedAmbiguousClassification:
    """A suppressed-unknown channel must never count as a confirmed delivery (FR-018)."""

    @pytest.mark.asyncio
    async def test_delivered_with_suppressed_ambiguous_channel_is_partial_failure(
        self, alert_history: AlertHistory
    ) -> None:
        """delivered + ambiguous_timeout is not a full success and not all_succeeded."""
        discord = FakeChannel("discord")
        telegram = FakeChannel("telegram")
        dispatcher = AlertDispatcher([discord, telegram], history=alert_history)
        assessment = _make_sample_assessment()
        await alert_history.record_channel_ambiguous(
            "telegram", assessment.wallet_address, assessment.market_id
        )

        result = await dispatcher.dispatch(_make_sample_alert(), assessment=assessment)

        assert result.channel_statuses == {
            "discord": "delivered",
            "telegram": "ambiguous_timeout",
        }
        assert result.disposition == "partial_failure"
        assert result.all_succeeded is False
        assert result.failure_count == 1

    @pytest.mark.asyncio
    async def test_duplicate_with_suppressed_ambiguous_channel_is_ambiguous(
        self, alert_history: AlertHistory
    ) -> None:
        """duplicate + ambiguous_timeout has no confirmed failure; it must not report failed."""
        discord = FakeChannel("discord")
        telegram = FakeChannel("telegram")
        dispatcher = AlertDispatcher([discord, telegram], history=alert_history)
        assessment = _make_sample_assessment()
        await alert_history.record_channel_delivery(
            "discord", assessment.wallet_address, assessment.market_id
        )
        await alert_history.record_channel_ambiguous(
            "telegram", assessment.wallet_address, assessment.market_id
        )

        result = await dispatcher.dispatch(_make_sample_alert(), assessment=assessment)

        assert result.channel_statuses == {
            "discord": "duplicate",
            "telegram": "ambiguous_timeout",
        }
        assert result.disposition == "ambiguous"
        assert len(discord.calls) == 0
        assert len(telegram.calls) == 0
