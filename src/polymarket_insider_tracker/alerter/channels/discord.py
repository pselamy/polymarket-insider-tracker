"""Discord webhook channel implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import httpx

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

    async def send(self, alert: FormattedAlert) -> bool:
        """Send alert to Discord webhook.

        Args:
            alert: Formatted alert with discord_embed.

        Returns:
            True if delivery succeeded, False otherwise.
        """
        await self._wait_for_rate_limit()

        payload = {
            "embeds": [alert.discord_embed],
        }

        for attempt in range(self.max_retries):
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
                        # Rate limited by Discord
                        retry_after = response.json().get("retry_after", 1.0)
                        logger.warning(f"Discord rate limited, retry after {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    logger.error(
                        f"Discord webhook failed: {response.status_code} {response.text}"
                    )

            except httpx.TimeoutException:
                logger.warning(f"Discord webhook timeout (attempt {attempt + 1})")
            except httpx.HTTPError as e:
                logger.error(f"Discord webhook error: {e}")

            # Exponential backoff
            if attempt < self.max_retries - 1:
                delay = self.retry_delay * (2**attempt)
                await asyncio.sleep(delay)

        logger.error("Discord delivery failed after all retries")
        return False
