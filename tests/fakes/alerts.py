"""Fakes for the alert-delivery boundary: channels and the HTTP servers behind them."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx

from polymarket_insider_tracker.alerter.models import FormattedAlert


class FakeAlertChannel:
    """An ``AlertChannel`` that keeps every delivered payload instead of sending it.

    Delivery is the product's external effect, so the recorded payloads are the observable
    contract: tests read ``deliveries`` to prove what would have reached Discord or Telegram.
    """

    def __init__(self, name: str, *, accepting: bool = True) -> None:
        self.name = name
        self.accepting = accepting
        self.deliveries: list[FormattedAlert] = []

    async def send(self, alert: FormattedAlert) -> bool:
        self.deliveries.append(alert)
        return self.accepting


class FakeWebhookServer:
    """An ``httpx.MockTransport`` handler that behaves like a webhook or bot API server.

    The server answers through a real ``httpx.AsyncClient`` and real ``httpx.Response`` objects.
    It rate-limits the first ``rate_limited_requests`` requests, then either accepts every request
    or, when ``failure`` is given, rejects every request with that response.
    """

    def __init__(
        self,
        *,
        success: httpx.Response,
        rate_limit: httpx.Response,
        rate_limited_requests: int = 0,
        failure: httpx.Response | None = None,
        network_error: Exception | None = None,
    ) -> None:
        self._success = success
        self._rate_limit = rate_limit
        self._rate_limited_requests = rate_limited_requests
        self._failure = failure
        self._network_error = network_error
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self._network_error is not None:
            raise self._network_error
        if self._failure is not None:
            return self._failure
        if len(self.requests) <= self._rate_limited_requests:
            return self._rate_limit
        return self._success

    def payloads(self) -> list[dict[str, object]]:
        """Return every JSON body the client posted, in order."""
        return [json.loads(request.content) for request in self.requests]

    def client_factory(self, original: type[httpx.AsyncClient]) -> Callable[..., httpx.AsyncClient]:
        """Return a replacement for ``httpx.AsyncClient`` routed through this server."""
        transport = httpx.MockTransport(self)

        def build(**kwargs: Any) -> httpx.AsyncClient:
            return original(transport=transport, **kwargs)

        return build


def discord_webhook(
    *,
    rate_limited_requests: int = 0,
    failure: httpx.Response | None = None,
    network_error: Exception | None = None,
) -> FakeWebhookServer:
    """A Discord-shaped webhook server: 204 on success, 429 JSON with ``retry_after``."""
    return FakeWebhookServer(
        success=httpx.Response(204),
        rate_limit=httpx.Response(429, json={"retry_after": 0.01}),
        rate_limited_requests=rate_limited_requests,
        failure=failure,
        network_error=network_error,
    )


def telegram_bot_api(
    *,
    rate_limited_requests: int = 0,
    failure: httpx.Response | None = None,
    network_error: Exception | None = None,
) -> FakeWebhookServer:
    """A Telegram-shaped Bot API server: ``ok`` envelopes with error codes."""
    return FakeWebhookServer(
        success=httpx.Response(200, json={"ok": True}),
        rate_limit=httpx.Response(
            429,
            json={"ok": False, "error_code": 429, "parameters": {"retry_after": 0.01}},
        ),
        rate_limited_requests=rate_limited_requests,
        failure=failure,
        network_error=network_error,
    )
