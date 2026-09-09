"""Tests for alert dispatcher and channels."""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.channels.telegram import TelegramChannel
from polymarket_insider_tracker.alerter.dispatcher import (
    AlertDispatcher,
    CircuitBreakerState,
    DispatchResult,
)
from polymarket_insider_tracker.alerter.models import FormattedAlert

# ============================================================================
# Fakes
# ============================================================================


class FakeHttpResponse:
    """Fake HTTP response for httpx calls."""

    def __init__(
        self,
        status_code: int = 200,
        json_data: dict[str, Any] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._json_data


class FakeAsyncHttpClient:
    """Fake async HTTP client substituting httpx.AsyncClient."""

    def __init__(
        self,
        responses: list[FakeHttpResponse] | FakeHttpResponse | None = None,
    ) -> None:
        self.responses = (
            [responses] if isinstance(responses, FakeHttpResponse) else list(responses or [])
        )
        self.post_calls: list[dict[str, Any]] = []

    async def __aenter__(self) -> "FakeAsyncHttpClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    async def post(self, url: str, **kwargs: Any) -> FakeHttpResponse:
        self.post_calls.append({"url": url, **kwargs})
        if self.responses:
            return self.responses.pop(0)
        return FakeHttpResponse(200)


class FakeAlertChannel:
    """In-memory fake alert channel for testing AlertDispatcher."""

    def __init__(self, name: str, succeeds: bool = True) -> None:
        self.name = name
        self.succeeds = succeeds
        self.sent_alerts: list[FormattedAlert] = []

    async def send(self, alert: FormattedAlert) -> bool:
        self.sent_alerts.append(alert)
        return self.succeeds


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_alert() -> FormattedAlert:
    """Create a sample formatted alert."""
    return FormattedAlert(
        title="Test Alert",
        body="Test body",
        discord_embed={
            "title": "Test",
            "color": 15158332,
            "fields": [],
        },
        telegram_markdown="*Test Alert*\nTest body",
        plain_text="TEST ALERT\nTest body",
        links={"market": "https://polymarket.com/test"},
    )


@pytest.fixture
def fake_discord_channel() -> FakeAlertChannel:
    """Create a fake Discord channel."""
    return FakeAlertChannel(name="discord")


@pytest.fixture
def fake_telegram_channel() -> FakeAlertChannel:
    """Create a fake Telegram channel."""
    return FakeAlertChannel(name="telegram")


# ============================================================================
# DiscordChannel Tests
# ============================================================================


class TestDiscordChannel:
    """Tests for Discord channel."""

    def test_init(self) -> None:
        """Test channel initialization."""
        channel = DiscordChannel(
            webhook_url="https://discord.com/api/webhooks/123/abc",
            rate_limit_per_minute=30,
        )
        assert channel.webhook_url == "https://discord.com/api/webhooks/123/abc"
        assert channel.rate_limit_per_minute == 30
        assert channel.name == "discord"

    @pytest.mark.asyncio
    async def test_send_success(
        self, sample_alert: FormattedAlert, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test successful Discord message send."""
        channel = DiscordChannel(webhook_url="https://discord.com/api/webhooks/123/abc")
        fake_client = FakeAsyncHttpClient(FakeHttpResponse(204))
        monkeypatch.setattr(httpx, "AsyncClient", lambda *_, **__: fake_client)

        result = await channel.send(sample_alert)

        assert result is True
        assert len(fake_client.post_calls) == 1
        assert fake_client.post_calls[0]["url"] == "https://discord.com/api/webhooks/123/abc"

    @pytest.mark.asyncio
    async def test_send_rate_limited(
        self, sample_alert: FormattedAlert, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test Discord rate limit handling."""
        channel = DiscordChannel(
            webhook_url="https://discord.com/api/webhooks/123/abc",
            max_retries=2,
            retry_delay=0.01,
        )
        fake_client = FakeAsyncHttpClient(
            [
                FakeHttpResponse(429, json_data={"retry_after": 0.01}),
                FakeHttpResponse(204),
            ]
        )
        monkeypatch.setattr(httpx, "AsyncClient", lambda *_, **__: fake_client)

        result = await channel.send(sample_alert)

        assert result is True
        assert len(fake_client.post_calls) == 2

    @pytest.mark.asyncio
    async def test_send_failure(
        self, sample_alert: FormattedAlert, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test Discord send failure after retries."""
        channel = DiscordChannel(
            webhook_url="https://discord.com/api/webhooks/123/abc",
            max_retries=2,
            retry_delay=0.01,
        )
        fake_client = FakeAsyncHttpClient(
            [
                FakeHttpResponse(500, text="Internal Server Error"),
                FakeHttpResponse(500, text="Internal Server Error"),
                FakeHttpResponse(500, text="Internal Server Error"),
            ]
        )
        monkeypatch.setattr(httpx, "AsyncClient", lambda *_, **__: fake_client)

        result = await channel.send(sample_alert)

        assert result is False
        assert len(fake_client.post_calls) == 2


# ============================================================================
# TelegramChannel Tests
# ============================================================================


class TestTelegramChannel:
    """Tests for Telegram channel."""

    def test_init(self) -> None:
        """Test channel initialization."""
        channel = TelegramChannel(
            bot_token="123456:ABC-DEF",
            chat_id="-1001234567890",
            rate_limit_per_minute=20,
        )
        assert channel.bot_token == "123456:ABC-DEF"
        assert channel.chat_id == "-1001234567890"
        assert channel.name == "telegram"

    @pytest.mark.asyncio
    async def test_send_success(
        self, sample_alert: FormattedAlert, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test successful Telegram message send."""
        channel = TelegramChannel(
            bot_token="123456:ABC-DEF",
            chat_id="-1001234567890",
        )
        fake_client = FakeAsyncHttpClient(FakeHttpResponse(200, json_data={"ok": True}))
        monkeypatch.setattr(httpx, "AsyncClient", lambda *_, **__: fake_client)

        result = await channel.send(sample_alert)

        assert result is True
        assert len(fake_client.post_calls) == 1

    @pytest.mark.asyncio
    async def test_send_rate_limited(
        self, sample_alert: FormattedAlert, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test Telegram rate limit handling."""
        channel = TelegramChannel(
            bot_token="123456:ABC-DEF",
            chat_id="-1001234567890",
            max_retries=2,
            retry_delay=0.01,
        )
        fake_client = FakeAsyncHttpClient(
            [
                FakeHttpResponse(
                    429,
                    json_data={
                        "ok": False,
                        "error_code": 429,
                        "parameters": {"retry_after": 0.01},
                    },
                ),
                FakeHttpResponse(200, json_data={"ok": True}),
            ]
        )
        monkeypatch.setattr(httpx, "AsyncClient", lambda *_, **__: fake_client)

        result = await channel.send(sample_alert)

        assert result is True
        assert len(fake_client.post_calls) == 2

    @pytest.mark.asyncio
    async def test_send_failure(
        self, sample_alert: FormattedAlert, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test Telegram send failure."""
        channel = TelegramChannel(
            bot_token="123456:ABC-DEF",
            chat_id="-1001234567890",
            max_retries=2,
            retry_delay=0.01,
        )
        fake_client = FakeAsyncHttpClient(
            [
                FakeHttpResponse(
                    400,
                    json_data={
                        "ok": False,
                        "error_code": 400,
                        "description": "Bad Request",
                    },
                ),
                FakeHttpResponse(
                    400,
                    json_data={
                        "ok": False,
                        "error_code": 400,
                        "description": "Bad Request",
                    },
                ),
                FakeHttpResponse(
                    400,
                    json_data={
                        "ok": False,
                        "error_code": 400,
                        "description": "Bad Request",
                    },
                ),
            ]
        )
        monkeypatch.setattr(httpx, "AsyncClient", lambda *_, **__: fake_client)

        result = await channel.send(sample_alert)

        assert result is False
        assert len(fake_client.post_calls) == 2


# ============================================================================
# CircuitBreakerState Tests
# ============================================================================


class TestCircuitBreakerState:
    """Tests for circuit breaker state."""

    def test_default_state(self) -> None:
        """Test default circuit breaker state."""
        state = CircuitBreakerState()
        assert state.failure_count == 0
        assert state.is_open is False
        assert state.half_open_attempts == 0
        assert state.last_failure_time is None


# ============================================================================
# DispatchResult Tests
# ============================================================================


class TestDispatchResult:
    """Tests for dispatch result."""

    def test_all_succeeded(self) -> None:
        """Test all_succeeded property."""
        result = DispatchResult(
            success_count=2,
            failure_count=0,
            channel_results={"discord": True, "telegram": True},
        )
        assert result.all_succeeded is True

    def test_partial_success(self) -> None:
        """Test partial success."""
        result = DispatchResult(
            success_count=1,
            failure_count=1,
            channel_results={"discord": True, "telegram": False},
        )
        assert result.all_succeeded is False

    def test_empty_channels(self) -> None:
        """Test with no channels."""
        result = DispatchResult(success_count=0, failure_count=0)
        assert result.all_succeeded is False


# ============================================================================
# AlertDispatcher Tests
# ============================================================================


class TestAlertDispatcher:
    """Tests for alert dispatcher."""

    def test_init(
        self,
        fake_discord_channel: FakeAlertChannel,
        fake_telegram_channel: FakeAlertChannel,
    ) -> None:
        """Test dispatcher initialization."""
        dispatcher = AlertDispatcher(channels=[fake_discord_channel, fake_telegram_channel])
        assert len(dispatcher.channels) == 2
        assert "discord" in dispatcher._circuit_state
        assert "telegram" in dispatcher._circuit_state

    @pytest.mark.asyncio
    async def test_dispatch_all_success(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
        fake_telegram_channel: FakeAlertChannel,
    ) -> None:
        """Test successful dispatch to all channels."""
        dispatcher = AlertDispatcher(channels=[fake_discord_channel, fake_telegram_channel])

        result = await dispatcher.dispatch(sample_alert)

        assert result.success_count == 2
        assert result.failure_count == 0
        assert result.all_succeeded is True
        assert len(fake_discord_channel.sent_alerts) == 1
        assert len(fake_telegram_channel.sent_alerts) == 1

    @pytest.mark.asyncio
    async def test_dispatch_partial_failure(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
        fake_telegram_channel: FakeAlertChannel,
    ) -> None:
        """Test dispatch with one channel failing."""
        fake_telegram_channel.succeeds = False

        dispatcher = AlertDispatcher(channels=[fake_discord_channel, fake_telegram_channel])

        result = await dispatcher.dispatch(sample_alert)

        assert result.success_count == 1
        assert result.failure_count == 1
        assert result.channel_results["discord"] is True
        assert result.channel_results["telegram"] is False

    @pytest.mark.asyncio
    async def test_dispatch_no_channels(self, sample_alert: FormattedAlert) -> None:
        """Test dispatch with no channels configured."""
        dispatcher = AlertDispatcher(channels=[])

        result = await dispatcher.dispatch(sample_alert)

        assert result.success_count == 0
        assert result.failure_count == 0

    @pytest.mark.asyncio
    async def test_circuit_opens_after_failures(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
    ) -> None:
        """Test circuit breaker opens after threshold failures."""
        fake_discord_channel.succeeds = False

        dispatcher = AlertDispatcher(
            channels=[fake_discord_channel],
            failure_threshold=3,
        )

        # First 3 failures
        for _ in range(3):
            await dispatcher.dispatch(sample_alert)

        # Circuit should be open now
        assert dispatcher._circuit_state["discord"].is_open is True
        assert dispatcher._circuit_state["discord"].failure_count == 3

    @pytest.mark.asyncio
    async def test_circuit_skips_when_open(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
    ) -> None:
        """Test that open circuit skips delivery."""
        dispatcher = AlertDispatcher(
            channels=[fake_discord_channel],
            failure_threshold=3,
            recovery_timeout_seconds=3600,
        )

        # Manually open the circuit
        dispatcher._circuit_state["discord"].is_open = True
        dispatcher._circuit_state["discord"].last_failure_time = datetime.now(UTC)

        result = await dispatcher.dispatch(sample_alert)

        assert result.channel_results["discord"] is False
        assert len(fake_discord_channel.sent_alerts) == 0

    @pytest.mark.asyncio
    async def test_circuit_closes_on_success(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
    ) -> None:
        """Test circuit closes on successful delivery."""
        fake_discord_channel.succeeds = False

        dispatcher = AlertDispatcher(
            channels=[fake_discord_channel],
            failure_threshold=2,
        )

        # Cause failures to open circuit
        await dispatcher.dispatch(sample_alert)
        await dispatcher.dispatch(sample_alert)
        assert dispatcher._circuit_state["discord"].is_open is True

        # Now succeed
        fake_discord_channel.succeeds = True
        # Force half-open by resetting last_failure to past
        dispatcher._circuit_state["discord"].last_failure_time = datetime(2020, 1, 1, tzinfo=UTC)

        result = await dispatcher.dispatch(sample_alert)

        assert result.channel_results["discord"] is True
        assert dispatcher._circuit_state["discord"].is_open is False

    @pytest.mark.asyncio
    async def test_dispatch_batch(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
    ) -> None:
        """Test batch dispatch."""
        dispatcher = AlertDispatcher(channels=[fake_discord_channel])

        alerts = [sample_alert, sample_alert, sample_alert]
        results = await dispatcher.dispatch_batch(alerts)

        assert len(results) == 3
        assert all(r.success_count == 1 for r in results)
        assert len(fake_discord_channel.sent_alerts) == 3

    def test_get_circuit_status(
        self,
        fake_discord_channel: FakeAlertChannel,
        fake_telegram_channel: FakeAlertChannel,
    ) -> None:
        """Test getting circuit status."""
        dispatcher = AlertDispatcher(channels=[fake_discord_channel, fake_telegram_channel])

        status = dispatcher.get_circuit_status()

        assert "discord" in status
        assert "telegram" in status
        assert status["discord"]["is_open"] is False
        assert status["discord"]["failure_count"] == 0

    def test_reset_circuit(
        self,
        fake_discord_channel: FakeAlertChannel,
    ) -> None:
        """Test manual circuit reset."""
        dispatcher = AlertDispatcher(channels=[fake_discord_channel])

        # Set up failure state
        dispatcher._circuit_state["discord"].failure_count = 5
        dispatcher._circuit_state["discord"].is_open = True

        # Reset
        result = dispatcher.reset_circuit("discord")

        assert result is True
        assert dispatcher._circuit_state["discord"].failure_count == 0
        assert dispatcher._circuit_state["discord"].is_open is False

    def test_reset_circuit_unknown_channel(
        self,
        fake_discord_channel: FakeAlertChannel,
    ) -> None:
        """Test reset with unknown channel name."""
        dispatcher = AlertDispatcher(channels=[fake_discord_channel])

        result = dispatcher.reset_circuit("unknown")

        assert result is False
