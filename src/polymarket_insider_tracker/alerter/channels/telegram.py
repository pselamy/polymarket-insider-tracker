"""Telegram Bot API channel implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from polymarket_insider_tracker.alerter.models import FormattedAlert

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramChannel:
    """Telegram Bot API channel for sending alerts.

    Sends formatted alerts to Telegram via Bot API with rate limiting
    and retry support.
    """

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        *,
        rate_limit_per_minute: int = 20,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 10.0,
    ) -> None:
        """Initialize Telegram channel.

        Args:
            bot_token: Telegram bot token.
            chat_id: Target chat/channel ID.
            rate_limit_per_minute: Maximum messages per minute.
            max_retries: Maximum retry attempts on failure.
            retry_delay: Base delay between retries (exponential backoff).
            timeout: HTTP request timeout in seconds.
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.rate_limit_per_minute = rate_limit_per_minute
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.name = "telegram"

        self._api_url = TELEGRAM_API_BASE.format(token=bot_token)

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
                    logger.debug(f"Telegram rate limit hit, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)

            self._request_times.append(now)

    async def send(self, alert: FormattedAlert) -> bool:
        """Send alert to Telegram channel.

        Args:
            alert: Formatted alert with telegram_markdown.

        Returns:
            True if delivery succeeded, False otherwise.
        """
        await self._wait_for_rate_limit()

        payload = {
            "chat_id": self.chat_id,
            "text": alert.telegram_markdown,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": False,
        }

        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(
                        self._api_url,
                        json=payload,
                    )

                    result = response.json()

                    if result.get("ok"):
                        logger.info("Telegram alert delivered successfully")
                        return True

                    error_code = result.get("error_code", 0)
                    description = result.get("description", "Unknown error")

                    if error_code == 429:
                        # Rate limited
                        retry_after = result.get("parameters", {}).get("retry_after", 1)
                        logger.warning(f"Telegram rate limited, retry after {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    logger.error(f"Telegram API error: {error_code} - {description}")

            except httpx.TimeoutException:
                logger.warning(f"Telegram API timeout (attempt {attempt + 1})")
            except httpx.HTTPError as e:
                logger.error(f"Telegram API error: {e}")

            # Exponential backoff
            if attempt < self.max_retries - 1:
                delay = self.retry_delay * (2**attempt)
                await asyncio.sleep(delay)

        logger.error("Telegram delivery failed after all retries")
        return False
