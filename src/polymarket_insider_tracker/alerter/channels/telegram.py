"""Telegram Bot API channel implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx

from polymarket_insider_tracker.redaction import redact_text_with_secrets, url_credential_components

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
        # The bot token itself is a component the URL derivation cannot see
        # bare (the path segment carries a ``bot`` prefix), so it is added
        # explicitly — raw and percent-encoded — alongside every spelling
        # derived from the API URL.
        self._secret_components = tuple(
            sorted(
                {*url_credential_components(self._api_url), bot_token, quote(bot_token, safe="")},
                key=len,
                reverse=True,
            )
        )

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

    async def _post_payload(
        self,
        payload: dict[str, object],
        attempt: int,
    ) -> bool | None:
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
                    retry_after = result.get("parameters", {}).get("retry_after", 1)
                    logger.warning(
                        "Telegram rate limited, retry after %ss", self._redact(str(retry_after))
                    )
                    await asyncio.sleep(retry_after)
                    return False

                # error_code and description are server-controlled response
                # values and may echo the token-bearing URL; never log them raw.
                logger.error(
                    "Telegram API error: %s - %s",
                    self._redact(str(error_code)),
                    self._redact(str(description)),
                )

        except (httpx.ConnectTimeout, httpx.PoolTimeout):
            # The request never reached Telegram, so retrying cannot duplicate a delivery.
            logger.warning(f"Telegram API connect timeout (attempt {attempt + 1})")
        except httpx.TimeoutException as e:
            # The payload may have been accepted before the timeout: the outcome is
            # ambiguous, so never re-post it here; the dispatcher owns the 60s window.
            logger.warning(
                "Telegram API timed out after the payload was sent; outcome ambiguous, "
                "not retrying (a duplicate is possible if the message was accepted)"
            )
            raise TimeoutError("Telegram API response timed out") from e
        except httpx.HTTPError as e:
            # The bot token rides in the API URL; an httpx message may embed it.
            logger.error("Telegram API error: %s", self._redact(str(e)))

        return None

    def _redact(self, text: str) -> str:
        """Hide the bot token and the token-bearing API URL inside diagnostic text.

        The configured URL, the bare token, and every credential-bearing
        component derived from the URL (path segments — each also in its
        JSON-escaped and percent-encoded spelling) are replaced first,
        because a server response may echo the credential in a form the
        URL-shaped scan cannot attribute; the central policy then masks
        every remaining URL-shaped substring.
        """
        return redact_text_with_secrets(text, self._secret_components)

    async def _backoff(self, attempt: int) -> None:
        if attempt < self.max_retries - 1:
            delay = self.retry_delay * (2**attempt)
            await asyncio.sleep(delay)

    async def send(self, alert: FormattedAlert) -> bool:
        """Send alert to Telegram channel.

        Args:
            alert: Formatted alert with telegram_markdown.

        Returns:
            True if delivery succeeded, False on confirmed failure.

        Raises:
            TimeoutError: The outcome is ambiguous; Telegram may have accepted the payload.
        """
        await self._wait_for_rate_limit()

        payload: dict[str, object] = {
            "chat_id": self.chat_id,
            "text": alert.telegram_markdown,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": False,
        }

        for attempt in range(self.max_retries):
            result = await self._post_payload(payload, attempt)
            if result is True:
                return True
            if result is False:
                continue
            await self._backoff(attempt)

        logger.error("Telegram delivery failed after all retries")
        return False
