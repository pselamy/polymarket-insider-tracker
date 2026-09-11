"""Discord webhook channel implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import httpx

from polymarket_insider_tracker.alerter.channels.response_values import (
    response_json_object,
    validated_integer,
    validated_retry_delay,
)
from polymarket_insider_tracker.redaction import (
    redact_failure_with_secrets,
    url_credential_components,
)

if TYPE_CHECKING:
    from polymarket_insider_tracker.alerter.models import FormattedAlert

logger = logging.getLogger(__name__)


def _rejection_label(response: httpx.Response) -> str:
    """A fail-closed label for a rejected response; no body byte is emitted.

    The body can echo the credential-bearing webhook URL in reversible
    encodings (percent, JSON ``\\uXXXX``, bare or decoded components) that
    no replacement list can enumerate, so it is withheld entirely; only
    Discord's integer ``code`` field survives validation.
    """
    code = validated_integer(response_json_object(response).get("code"))
    size = len(response.content)
    if code is None:
        return f"({size}-byte body withheld)"
    return f"(error code {code}, {size}-byte body withheld)"


class DiscordChannel:
    """Discord webhook channel for sending alerts.

    Sends formatted alerts to Discord via webhook URL with rate limiting
    and retry support.
    """

    def __init__(
        self,
        webhook_url: str,
        *,
        rate_limit_per_minute: int = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 10.0,
    ) -> None:
        """Initialize Discord channel.

        Args:
            webhook_url: Discord webhook URL.
            rate_limit_per_minute: Maximum messages per minute (Discord limit is 30).
            max_retries: Maximum retry attempts on failure.
            retry_delay: Base delay between retries (exponential backoff).
            timeout: HTTP request timeout in seconds.
        """
        self.webhook_url = webhook_url
        self.rate_limit_per_minute = rate_limit_per_minute
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.name = "discord"
        # The webhook URL's path is the credential; a server response may echo
        # it whole, escaped, encoded, or as a bare component, so every derived
        # spelling is precomputed for the diagnostic scrub.
        self._secret_components = url_credential_components(webhook_url)

        # Rate limiting state
        self._request_times: list[float] = []
        self._lock = asyncio.Lock()

    async def _wait_for_rate_limit(self) -> None:
        """Wait if rate limit is exceeded."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            # Remove requests older than 1 minute
            self._request_times = [t for t in self._request_times if now - t < 60]

            if len(self._request_times) >= self.rate_limit_per_minute:
                # Wait until the oldest request expires
                wait_time = 60 - (now - self._request_times[0])
                if wait_time > 0:
                    logger.debug(f"Discord rate limit hit, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)

            self._request_times.append(now)

    async def _post_payload(
        self,
        payload: dict[str, object],
        attempt: int,
    ) -> bool | None:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                )

                if response.status_code == 204:
                    logger.info("Discord alert delivered successfully")
                    return True

                if response.status_code == 429:
                    # ``retry_after`` is a server-controlled value driving a log
                    # line and a sleep; only a validated finite delay survives.
                    retry_after = validated_retry_delay(
                        response_json_object(response).get("retry_after")
                    )
                    logger.warning("Discord rate limited, retry after %ss", retry_after)
                    await asyncio.sleep(retry_after)
                    return False

                # The response body is server-controlled text that can echo the
                # credential-bearing request URL in re-encoded spellings no
                # replacement list can enumerate; it never reaches a log at all.
                logger.error(
                    "Discord webhook failed: %s %s",
                    response.status_code,
                    _rejection_label(response),
                )

        except (httpx.ConnectTimeout, httpx.PoolTimeout):
            # The request never reached Discord, so retrying cannot duplicate a delivery.
            logger.warning(f"Discord webhook connect timeout (attempt {attempt + 1})")
        except httpx.TimeoutException as e:
            # The payload may have been accepted before the timeout: the outcome is
            # ambiguous, so never re-post it here; the dispatcher owns the 60s window.
            logger.warning(
                "Discord webhook timed out after the payload was sent; outcome ambiguous, "
                "not retrying (a duplicate is possible if the message was accepted)"
            )
            raise TimeoutError("Discord webhook response timed out") from e
        except httpx.HTTPError as e:
            # The webhook URL is the credential; an httpx message may embed it.
            logger.error("Discord webhook error: %s", self._redact_failure(e))

        return None

    def _redact_failure(self, error: BaseException) -> str:
        """Render a transport failure without re-emitting the webhook credential.

        HTTPX annotates error messages as ``str``, but a runtime fault can
        carry bytes or container arguments; ``str(error)`` would interpolate
        them verbatim before any replacement could see them. The shared
        argument-aware renderer runs first, then every credential spelling
        derived from the configured URL is replaced and the central policy
        masks remaining URL-shaped text.
        """
        return redact_failure_with_secrets(error, self._secret_components)

    async def _backoff(self, attempt: int) -> None:
        if attempt < self.max_retries - 1:
            delay = self.retry_delay * (2**attempt)
            await asyncio.sleep(delay)

    async def send(self, alert: FormattedAlert) -> bool:
        """Send alert to Discord webhook.

        Args:
            alert: Formatted alert with discord_embed.

        Returns:
            True if delivery succeeded, False on confirmed failure.

        Raises:
            TimeoutError: The outcome is ambiguous; Discord may have accepted the payload.
        """
        await self._wait_for_rate_limit()

        payload: dict[str, object] = {
            "embeds": [alert.discord_embed],
        }

        for attempt in range(self.max_retries):
            result = await self._post_payload(payload, attempt)
            if result is True:
                return True
            if result is False:
                continue
            await self._backoff(attempt)

        logger.error("Discord delivery failed after all retries")
        return False
