"""Tests for alert dispatcher and channels."""

import asyncio
from datetime import UTC, datetime
from typing import Literal

import httpx
import pytest

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.channels.response_values import validated_retry_delay
from polymarket_insider_tracker.alerter.channels.telegram import TelegramChannel
from polymarket_insider_tracker.alerter.dispatcher import (
    AlertDispatcher,
    CircuitBreakerState,
    DispatchResult,
)
from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.config import DiscordSettings
from tests.fakes import FakeAlertChannel
from tests.fakes.alerts import FakeWebhookServer, discord_webhook, telegram_bot_api

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/123/abc"
TELEGRAM_BOT_TOKEN = "123456:ABC-DEF"

ChannelKind = Literal["discord", "telegram"]
CHANNEL_KINDS: tuple[ChannelKind, ...] = ("discord", "telegram")

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
    return FakeAlertChannel("discord")


@pytest.fixture
def fake_telegram_channel() -> FakeAlertChannel:
    """Create a fake Telegram channel."""
    return FakeAlertChannel("telegram")


def wired_channel(
    kind: ChannelKind,
    monkeypatch: pytest.MonkeyPatch,
    *,
    max_retries: int = 3,
    retry_delay: float = 0.0,
    credential: str | None = None,
    chat_id: str = "chat-1",
    rate_limited_requests: int = 0,
    failure: httpx.Response | None = None,
    network_error: Exception | None = None,
) -> tuple[DiscordChannel | TelegramChannel, FakeWebhookServer]:
    """A channel of ``kind`` whose HTTP client talks to its matching fake server.

    The one home for pairing each channel with the server shaped like its provider.
    """
    if kind == "discord":
        server = discord_webhook(
            rate_limited_requests=rate_limited_requests,
            failure=failure,
            network_error=network_error,
        )
        channel: DiscordChannel | TelegramChannel = DiscordChannel(
            webhook_url=DISCORD_WEBHOOK_URL if credential is None else credential,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )
    else:
        server = telegram_bot_api(
            rate_limited_requests=rate_limited_requests,
            failure=failure,
            network_error=network_error,
        )
        channel = TelegramChannel(
            TELEGRAM_BOT_TOKEN if credential is None else credential,
            chat_id,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )
    monkeypatch.setattr(httpx, "AsyncClient", server.client_factory(httpx.AsyncClient))
    return channel, server


async def assert_failure_logged_without(
    channel: DiscordChannel | TelegramChannel,
    server: FakeWebhookServer,
    alert: FormattedAlert,
    caplog: pytest.LogCaptureFixture,
    *,
    logged: tuple[str, ...],
    secrets: tuple[str, ...],
) -> None:
    """One send fails after a single request; the log names the failure but leaks no secret."""
    result = await channel.send(alert)

    assert result is False
    assert len(server.requests) == 1
    for text in logged:
        assert text in caplog.text
    for secret in secrets:
        assert secret not in caplog.text


# ============================================================================
# DiscordChannel Tests
# ============================================================================


class TestChannelErrorRedaction:
    """Channel error logs must never echo the credential-bearing URL or token."""

    @pytest.mark.asyncio
    async def test_discord_network_error_log_redacts_webhook_url(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        channel, server = wired_channel(
            "discord",
            monkeypatch,
            max_retries=1,
            network_error=httpx.ConnectError(f"connection failed for {DISCORD_WEBHOOK_URL}"),
        )

        result = await channel.send(sample_alert)

        assert result is False
        assert "connection failed" in caplog.text
        assert DISCORD_WEBHOOK_URL not in caplog.text

    @pytest.mark.asyncio
    async def test_telegram_network_error_log_redacts_token(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        token = "12345:SECRET-BOT-TOKEN"
        channel, server = wired_channel(
            "telegram",
            monkeypatch,
            max_retries=1,
            credential=token,
            network_error=httpx.ConnectError(
                f"connection failed for https://api.telegram.org/bot{token}/sendMessage"
            ),
        )

        result = await channel.send(sample_alert)

        assert result is False
        assert "connection failed" in caplog.text
        assert token not in caplog.text

    @pytest.mark.asyncio
    async def test_discord_failure_response_body_is_redacted(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Round-18: a non-204 response body is server text and may echo the webhook URL."""
        secret = "WEBHOOK_ECHO_SECRET_R18"
        failure = httpx.Response(
            400,
            text=(
                f"invalid webhook {DISCORD_WEBHOOK_URL} "
                f"(proxied via https://edge.example/hook/{secret})"
            ),
        )
        channel, server = wired_channel("discord", monkeypatch, max_retries=1, failure=failure)

        await assert_failure_logged_without(
            channel,
            server,
            sample_alert,
            caplog,
            logged=("Discord webhook failed", "400"),
            secrets=(DISCORD_WEBHOOK_URL, secret),
        )

    @pytest.mark.asyncio
    async def test_telegram_error_description_is_redacted(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Round-18: the API ``description`` is server text and may echo the token URL."""
        secret = "TELEGRAM_ECHO_SECRET_R18"
        token = "12345:SECRET-BOT-TOKEN"
        failure = httpx.Response(
            200,
            json={
                "ok": False,
                "error_code": 401,
                "description": (
                    f"unauthorized for https://api.telegram.org/bot{token}/sendMessage"
                    f" via https://edge.example/relay?key={secret}"
                ),
            },
        )
        channel, server = wired_channel(
            "telegram", monkeypatch, max_retries=1, credential=token, failure=failure
        )

        await assert_failure_logged_without(
            channel,
            server,
            sample_alert,
            caplog,
            logged=("Telegram API error", "401"),
            secrets=(token, secret),
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("style", ["escaped-slashes", "bare-token", "quoted-path"])
    async def test_discord_response_echo_forms_never_reach_the_log(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        style: str,
    ) -> None:
        """Round-21 N-R21-3: a response may echo the credential in a form the
        URL-shaped scan cannot attribute — a JSON ``\\/``-escaped URL, the bare
        token component alone, or a quote-joined path; component replacement
        plus the central policy masks every form.
        """
        secret = "R21-DISCORD-TOKEN-SECRET"
        webhook = f"https://discord.invalid/api/webhooks/987654/{secret}"
        message = {
            "escaped-slashes": webhook.replace("/", "\\/"),
            "bare-token": f"unknown token {secret}",
            "quoted-path": f"https://rpc.invalid/v2/prefix'{secret}",
        }[style]
        failure = httpx.Response(400, json={"message": message})
        channel, server = wired_channel(
            "discord", monkeypatch, max_retries=1, credential=webhook, failure=failure
        )

        await assert_failure_logged_without(
            channel,
            server,
            sample_alert,
            caplog,
            logged=("Discord webhook failed",),
            secrets=(secret,),
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("style", ["escaped-slashes", "bare-token", "quoted-path"])
    async def test_telegram_response_echo_forms_never_reach_the_log(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        style: str,
    ) -> None:
        """Round-21 N-R21-3: the Telegram ``description`` may echo the token in
        escaped, bare, or quote-joined form; every form is masked.
        """
        secret = "R21-TELEGRAM-TOKEN-SECRET"
        token = f"987654:{secret}"
        api_url = f"https://api.telegram.org/bot{token}/sendMessage"
        message = {
            "escaped-slashes": api_url.replace("/", "\\/"),
            "bare-token": f"unauthorized token {token}",
            "quoted-path": f"https://rpc.invalid/v2/prefix'{token}",
        }[style]
        failure = httpx.Response(200, json={"ok": False, "error_code": 400, "description": message})
        channel, server = wired_channel(
            "telegram", monkeypatch, max_retries=1, credential=token, failure=failure
        )

        await assert_failure_logged_without(
            channel, server, sample_alert, caplog, logged=("Telegram API error",), secrets=(secret,)
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "style", ["encoded-path-decoded-echo", "query-value", "json-unicode-escape"]
    )
    async def test_discord_configured_credential_reencodings_never_reach_the_log(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        style: str,
    ) -> None:
        """Round-23 N-R23-1: an accepted webhook URL and a response echo may spell
        the credential in different reversible encodings — a percent-encoded
        configured path echoed decoded, a query value echoed bare, or a plain
        path token echoed with JSON ``\\uXXXX`` escapes. No replacement list can
        enumerate the encodings, so the server-controlled body never reaches the
        log in any form.
        """
        secret = "R23-DISCORD-PATH-SECRET"
        webhook = f"https://discord.invalid/api/webhooks/987654/{secret}"
        echo = secret
        if style == "encoded-path-decoded-echo":
            webhook = webhook.replace(secret, "%52" + secret[1:])
        if style == "query-value":
            webhook = f"https://discord.invalid/api/webhooks/987654/public?token={secret}"
        if style == "json-unicode-escape":
            echo = "\\u0052" + secret[1:]
        config = DiscordSettings(_env_file=None, DISCORD_WEBHOOK_URL=webhook)
        assert config.enabled
        assert config.webhook_url is not None
        body = ('{"message":"' + echo + '"}').encode()
        channel, server = wired_channel(
            "discord",
            monkeypatch,
            max_retries=1,
            credential=config.webhook_url.get_secret_value(),
            failure=httpx.Response(400, content=body),
        )

        await assert_failure_logged_without(
            channel,
            server,
            sample_alert,
            caplog,
            logged=("Discord webhook failed", "400"),
            secrets=(secret, "\\u0052" + secret[1:]),
        )

    @pytest.mark.asyncio
    async def test_telegram_unicode_escaped_error_code_echo_never_reaches_the_log(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Round-23 N-R23-1 sibling: ``error_code`` and ``description`` are
        server-controlled values that can spell the token with JSON ``\\uXXXX``
        escapes no replacement list can enumerate; only a validated integer
        code survives, and free response text is withheld entirely.
        """
        secret = "R23-TELEGRAM-TOKEN-SECRET"
        token = f"987654:{secret}"
        escaped_token = "987654:" + "\\u0052" + secret[1:]
        failure = httpx.Response(
            200,
            json={
                "ok": False,
                "error_code": f"unauthorized {escaped_token}",
                "description": f"bad bot {escaped_token}",
            },
        )
        channel, server = wired_channel(
            "telegram", monkeypatch, max_retries=1, credential=token, failure=failure
        )

        await assert_failure_logged_without(
            channel,
            server,
            sample_alert,
            caplog,
            logged=("Telegram API error",),
            secrets=(secret, "\\u0052" + secret[1:]),
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("channel_kind", CHANNEL_KINDS)
    @pytest.mark.parametrize("shape", ["bytes", "dict", "list"])
    async def test_channel_transport_error_nonstring_arguments_never_reach_the_log(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        channel_kind: ChannelKind,
        shape: str,
    ) -> None:
        """Round-23 N-R23-2: HTTPX annotates error messages as ``str``, but a
        runtime fault can carry bytes or container arguments (fault injection
        beyond the annotated producer contract, not a shape ordinary providers
        emit); ``str(e)`` would interpolate them verbatim before any text
        replacement could see them. The shared argument-aware renderer runs
        first, so no raw argument is ever rendered.
        """
        secret = "R23-CHANNEL-ARG-SECRET"
        payload = {
            "bytes": secret.encode(),
            "dict": {"secret": secret},
            "list": [secret],
        }[shape]
        error = httpx.ConnectError("")
        error.args = (payload,)
        channel, server = wired_channel(
            channel_kind,
            monkeypatch,
            max_retries=1,
            credential={
                "discord": "https://discord.invalid/api/webhooks/1/different-config-token",
                "telegram": "different-config-token",
            }[channel_kind],
            network_error=error,
        )

        result = await channel.send(sample_alert)

        assert result is False
        assert len(server.requests) == 1
        assert secret not in caplog.text


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
    async def test_connect_timeout_is_confirmed_failure(
        self, sample_alert: FormattedAlert, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A connect timeout means nothing reached the server: confirmed failure, retried."""
        channel, server = wired_channel(
            "discord",
            monkeypatch,
            max_retries=2,
            retry_delay=0.01,
            network_error=httpx.ConnectTimeout("connect timed out"),
        )

        result = await channel.send(sample_alert)

        assert result is False
        assert len(server.requests) == 2


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


class TestChannelContract:
    """Delivery, retry and ambiguity behavior both channels share."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("channel_kind", CHANNEL_KINDS)
    async def test_send_success(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        channel_kind: ChannelKind,
    ) -> None:
        """A success reply is one delivery of the provider-shaped payload."""
        expected_url, expected_payload = {
            "discord": (DISCORD_WEBHOOK_URL, {"embeds": [sample_alert.discord_embed]}),
            "telegram": (
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                {
                    "chat_id": "-1001234567890",
                    "text": sample_alert.telegram_markdown,
                    "parse_mode": "MarkdownV2",
                    "disable_web_page_preview": False,
                },
            ),
        }[channel_kind]
        channel, server = wired_channel(channel_kind, monkeypatch, chat_id="-1001234567890")

        result = await channel.send(sample_alert)

        assert result is True
        assert [str(request.url) for request in server.requests] == [expected_url]
        assert server.payloads() == [expected_payload]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("channel_kind", CHANNEL_KINDS)
    async def test_send_rate_limited(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        channel_kind: ChannelKind,
    ) -> None:
        """A 429 is retried and the second attempt delivers."""
        channel, server = wired_channel(
            channel_kind, monkeypatch, max_retries=2, retry_delay=0.01, rate_limited_requests=1
        )

        result = await channel.send(sample_alert)

        assert result is True
        assert len(server.requests) == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize("channel_kind", CHANNEL_KINDS)
    async def test_send_failure(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        channel_kind: ChannelKind,
    ) -> None:
        """A provider failure exhausts the retries and reports failure."""
        failure = {
            "discord": httpx.Response(500, text="Internal Server Error"),
            "telegram": httpx.Response(
                400, json={"ok": False, "error_code": 400, "description": "Bad Request"}
            ),
        }[channel_kind]
        channel, server = wired_channel(
            channel_kind, monkeypatch, max_retries=2, retry_delay=0.01, failure=failure
        )

        result = await channel.send(sample_alert)

        assert result is False
        assert len(server.requests) == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize("channel_kind", CHANNEL_KINDS)
    async def test_read_timeout_is_ambiguous_and_not_retried(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        channel_kind: ChannelKind,
    ) -> None:
        """A read timeout after the payload was sent is ambiguous: no internal retry, raise."""
        channel, server = wired_channel(
            channel_kind,
            monkeypatch,
            max_retries=3,
            retry_delay=0.01,
            network_error=httpx.ReadTimeout("read timed out"),
        )

        with pytest.raises(TimeoutError):
            await channel.send(sample_alert)

        assert len(server.requests) == 1


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
        assert len(fake_discord_channel.deliveries) == 1
        assert len(fake_telegram_channel.deliveries) == 1

    @pytest.mark.asyncio
    async def test_dispatch_partial_failure(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
        fake_telegram_channel: FakeAlertChannel,
    ) -> None:
        """Test dispatch with one channel failing."""
        fake_telegram_channel.accepting = False

        dispatcher = AlertDispatcher(channels=[fake_discord_channel, fake_telegram_channel])

        result = await dispatcher.dispatch(sample_alert)

        assert result.success_count == 1
        assert result.failure_count == 1
        assert result.channel_results["discord"] is True
        assert result.channel_results["telegram"] is False

    @pytest.mark.asyncio
    async def test_dispatch_no_channels(self, sample_alert: FormattedAlert) -> None:
        """Dispatch with no channels configured must not claim a delivery happened."""
        dispatcher = AlertDispatcher(channels=[])

        result = await dispatcher.dispatch(sample_alert)

        assert result.success_count == 0
        assert result.failure_count == 0
        assert result.all_succeeded is False
        assert result.disposition == "no_channels"

    @pytest.mark.asyncio
    async def test_circuit_opens_after_failures(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
    ) -> None:
        """Test circuit breaker opens after threshold failures."""
        fake_discord_channel.accepting = False

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
        assert len(fake_discord_channel.deliveries) == 0

    @pytest.mark.asyncio
    async def test_circuit_closes_on_success(
        self,
        sample_alert: FormattedAlert,
        fake_discord_channel: FakeAlertChannel,
    ) -> None:
        """Test circuit closes on successful delivery."""
        fake_discord_channel.accepting = False

        dispatcher = AlertDispatcher(
            channels=[fake_discord_channel],
            failure_threshold=2,
        )

        # Cause failures to open circuit
        await dispatcher.dispatch(sample_alert)
        await dispatcher.dispatch(sample_alert)
        assert dispatcher._circuit_state["discord"].is_open is True

        # Now succeed
        fake_discord_channel.accepting = True
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
        assert len(fake_discord_channel.deliveries) == 3

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


# ============================================================================
# Retry-delay response boundary (Round-25 N-R25-2)
# ============================================================================


class TestRetryDelayResponseBoundary:
    """Round-25 N-R25-2: numeric ``retry_after`` values the float type cannot
    represent must take the documented fixed 1.0s fallback.

    JSON integers carry no size bound, so a 400-digit value parses normally
    and ``float()`` raised ``OverflowError`` out of both channels' HTTP-error
    handling — aborting the failed-attempt path both inside the request
    method and through public ``send`` — instead of completing the attempt
    with the fallback delay.
    """

    @pytest.fixture
    def recorded_sleeps(self, monkeypatch: pytest.MonkeyPatch) -> list[float]:
        """Record every requested delay instead of really sleeping.

        Narrow boundary injection: the requested delay is the observable
        contract of the 429 path, and recording it keeps the huge-value and
        control cases from stalling the suite.
        """
        sleeps: list[float] = []

        async def record(delay: float) -> None:
            sleeps.append(delay)

        monkeypatch.setattr(asyncio, "sleep", record)
        return sleeps

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (10**400, 1.0),
            (-(10**400), 1.0),
            (float("inf"), 1.0),
            (float("nan"), 1.0),
            (True, 1.0),
            ("2", 1.0),
            (None, 1.0),
            (-1, 1.0),
            (0, 0.0),
            (3, 3.0),
            (2.5, 2.5),
        ],
    )
    def test_validated_retry_delay_input_classes(self, value: object, expected: float) -> None:
        """Every non-representable, non-finite, negative, or non-numeric shape
        collapses to the fixed default; ordinary finite delays survive."""
        assert validated_retry_delay(value) == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize("channel_kind", CHANNEL_KINDS)
    @pytest.mark.parametrize("sign", [1, -1])
    async def test_unrepresentable_retry_after_takes_the_fixed_fallback(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        recorded_sleeps: list[float],
        channel_kind: ChannelKind,
        sign: int,
    ) -> None:
        huge = sign * 10**400
        failure = {
            "discord": httpx.Response(429, json={"retry_after": huge}),
            "telegram": httpx.Response(
                429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": huge}}
            ),
        }[channel_kind]
        channel, server = wired_channel(channel_kind, monkeypatch, max_retries=1, failure=failure)

        result = await channel.send(sample_alert)

        assert result is False
        assert len(server.requests) == 1
        assert recorded_sleeps == [1.0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("channel_kind", CHANNEL_KINDS)
    async def test_ordinary_rate_limit_still_delays_then_delivers(
        self,
        sample_alert: FormattedAlert,
        monkeypatch: pytest.MonkeyPatch,
        recorded_sleeps: list[float],
        channel_kind: ChannelKind,
    ) -> None:
        """Control: a finite server delay is honored as requested and the
        retried attempt still delivers."""
        channel, server = wired_channel(
            channel_kind, monkeypatch, max_retries=2, rate_limited_requests=1
        )

        result = await channel.send(sample_alert)

        assert result is True
        assert len(server.requests) == 2
        assert recorded_sleeps == [0.01]
