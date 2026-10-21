"""Channel behaviour for the less common server response and transport shapes."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.channels.response_values import (
    DEFAULT_RETRY_AFTER_SECONDS,
    response_json_object,
)
from polymarket_insider_tracker.alerter.channels.telegram import TelegramChannel
from polymarket_insider_tracker.alerter.models import FormattedAlert
from tests.fakes.alerts import FakeWebhookServer, discord_webhook, telegram_bot_api

WEBHOOK_URL = "https://discord.com/api/webhooks/123/abc"


@pytest.fixture
def alert() -> FormattedAlert:
    return FormattedAlert(
        title="Shape probe",
        body="Shape body",
        discord_embed={"title": "Shape probe"},
        telegram_markdown="*Shape probe*",
        plain_text="SHAPE PROBE",
        links={"market": "https://polymarket.com/shape-probe"},
    )


def _channel(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    server: FakeWebhookServer,
    max_retries: int = 1,
) -> DiscordChannel | TelegramChannel:
    channel: DiscordChannel | TelegramChannel
    if kind == "discord":
        channel = DiscordChannel(webhook_url=WEBHOOK_URL, max_retries=max_retries, retry_delay=0.0)
    else:
        channel = TelegramChannel("bot-token", "chat-1", max_retries=max_retries, retry_delay=0.0)
    monkeypatch.setattr(httpx, "AsyncClient", server.client_factory(httpx.AsyncClient))
    return channel


@pytest.mark.asyncio
async def test_discord_rejection_logs_only_the_integer_error_code(
    alert: FormattedAlert,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    body = {"code": 50027, "message": f"bad hook {WEBHOOK_URL}"}
    server = discord_webhook(failure=httpx.Response(401, json=body))
    channel = _channel("discord", monkeypatch, server=server)

    assert await channel.send(alert) is False

    assert "error code 50027" in caplog.text
    assert WEBHOOK_URL not in caplog.text
    assert "bad hook" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["discord", "telegram"])
async def test_non_object_json_rejection_is_retried_then_fails_without_body_echo(
    kind: str,
    alert: FormattedAlert,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    failure = httpx.Response(400, json=["leaky-body-marker"])
    server = (discord_webhook if kind == "discord" else telegram_bot_api)(failure=failure)
    channel = _channel(kind, monkeypatch, server=server, max_retries=3)

    assert await channel.send(alert) is False

    assert len(server.requests) == 3
    assert "leaky-body-marker" not in caplog.text


@pytest.mark.asyncio
async def test_telegram_rate_limit_without_a_parameters_object_waits_the_default_delay(
    alert: FormattedAlert,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    async def record(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", record)
    limited = httpx.Response(429, json={"ok": False, "error_code": 429, "parameters": "soon"})
    server = telegram_bot_api(failure=limited)
    channel = _channel("telegram", monkeypatch, server=server)

    assert await channel.send(alert) is False

    assert sleeps == [DEFAULT_RETRY_AFTER_SECONDS]


@pytest.mark.asyncio
async def test_telegram_connect_timeout_is_retried_because_nothing_was_sent(
    alert: FormattedAlert,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = telegram_bot_api(network_error=httpx.ConnectTimeout("no route"))
    channel = _channel("telegram", monkeypatch, server=server, max_retries=2)

    assert await channel.send(alert) is False

    assert len(server.requests) == 2
    assert "connect timeout" in caplog.text


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b'{"code": 7, "retry_after": 1.5}', {"code": 7, "retry_after": 1.5}),
        (b"{}", {}),
        (b"[1, 2]", {}),
        (b'"text"', {}),
        (b"null", {}),
        (b"not json", {}),
    ],
)
def test_response_json_object_returns_only_a_json_object(
    content: bytes, expected: dict[str, object]
) -> None:
    assert response_json_object(httpx.Response(200, content=content)) == expected
