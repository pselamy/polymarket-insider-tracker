"""Discord webhook channel implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import httpx

from polymarket_insider_tracker.redaction import redact_text

if TYPE_CHECKING:
    from polymarket_insider_tracker.alerter.models import FormattedAlert

logger = logging.getLogger(__name__)


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
                    retry_after = response.json().get("retry_after", 1.0)
                    logger.warning(
                        "Discord rate limited, retry after %ss", self._redact(str(retry_after))
                    )
                    await asyncio.sleep(retry_after)
                    return False

                # The response body is server-controlled text and may echo the
                # credential-bearing request URL; it never reaches a log raw.
                logger.error(
                    "Discord webhook failed: %s %s",
                    response.status_code,
                    self._redact(response.text),
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
            logger.error("Discord webhook error: %s", self._redact(str(e)))

        return None

    def _redact(self, text: str) -> str:
        """Hide the credential-bearing webhook URL inside diagnostic text.

        The exact configured value is replaced first, then the central policy
        masks every remaining URL-shaped substring, so a respelled or partial
        form of the webhook URL cannot survive either.
        """
        return redact_text(text.replace(self.webhook_url, "<redacted webhook url>"))

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
