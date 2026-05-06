"""Wrapper around py-clob-client with rate limiting and retry logic."""

import asyncio
import json
import logging
import os
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import httpx
from py_clob_client.client import ClobClient as BaseClobClient
from py_clob_client.clob_types import BookParams

from polymarket_insider_tracker.ingestor.models import Market, Orderbook

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")

# Constants
DEFAULT_HOST = "https://clob.polymarket.com"
MAX_REQUESTS_PER_SECOND = 10
MIN_REQUEST_INTERVAL = 1.0 / MAX_REQUESTS_PER_SECOND  # 0.1 seconds

DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_DELAY = 1.0
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)


def _gamma_to_clob_market(g: dict[str, object]) -> dict[str, object]:
    """Adapt a Gamma API market dict to the CLOB simplified-markets schema
    expected by Market.from_dict."""
    outcomes_raw = g.get("outcomes", "[]")
    token_ids_raw = g.get("clobTokenIds", "[]")
    prices_raw = g.get("outcomePrices", "[]")

    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
    token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else (token_ids_raw or [])
    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])

    tokens = []
    for i, outcome in enumerate(outcomes):
        if i >= len(token_ids):
            break
        token = {"token_id": str(token_ids[i]), "outcome": str(outcome)}
        if i < len(prices):
            token["price"] = str(prices[i])
        tokens.append(token)

    return {
        "condition_id": str(g.get("conditionId", "")),
        "question": g.get("question", ""),
        "description": g.get("description", ""),
        "tokens": tokens,
        "end_date_iso": g.get("endDate"),
        "active": g.get("active", True),
        "closed": g.get("closed", False),
    }


class RateLimiter:
    """Token bucket rate limiter for API requests."""

    def __init__(self, max_requests_per_second: float = MAX_REQUESTS_PER_SECOND) -> None:
        """Initialize the rate limiter.

        Args:
            max_requests_per_second: Maximum requests allowed per second.
        """
        self._min_interval = 1.0 / max_requests_per_second
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request slot is available."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                wait_time = self._min_interval - elapsed
                await asyncio.sleep(wait_time)
            self._last_request_time = time.monotonic()

    def acquire_sync(self) -> None:
        """Synchronous version of acquire for sync operations."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            wait_time = self._min_interval - elapsed
            time.sleep(wait_time)
        self._last_request_time = time.monotonic()


class RetryError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, message: str, last_exception: Exception | None = None) -> None:
        super().__init__(message)
        self.last_exception = last_exception


def with_retry(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_RETRY_BASE_DELAY,
    retry_on: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator for adding retry logic with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds (doubles with each retry).
        retry_on: Tuple of exception types to retry on.

    Returns:
        Decorated function with retry logic.
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    last_exception = e
                    if attempt == max_retries:
                        break

                    delay = base_delay * (2**attempt)
                    logger.warning(
                        "Attempt %d/%d failed: %s. Retrying in %.1f seconds...",
                        attempt + 1,
                        max_retries + 1,
                        str(e),
                        delay,
                    )
                    time.sleep(delay)

            raise RetryError(
                f"All {max_retries + 1} attempts failed for {func.__name__}",
                last_exception=last_exception,
            )

        return wrapper

    return decorator


class ClobClientError(Exception):
    """Base exception for ClobClient errors."""


class ClobClient:
    """Wrapper around py-clob-client with rate limiting and retry logic.

    This client provides a clean interface for querying Polymarket CLOB data
    with built-in rate limiting (10 requests/second) and automatic retry
    with exponential backoff on transient errors.

    Example:
        >>> client = ClobClient()  # Uses POLYMARKET_API_KEY env var
        >>> markets = client.get_markets()
        >>> orderbook = client.get_orderbook("token_id_here")
    """

    def __init__(
        self,
        api_key: str | None = None,
        host: str = DEFAULT_HOST,
        max_retries: int = DEFAULT_MAX_RETRIES,
        requests_per_second: float = MAX_REQUESTS_PER_SECOND,
    ) -> None:
        """Initialize the CLOB client.

        Args:
            api_key: Polymarket API key. If not provided, reads from
                POLYMARKET_API_KEY environment variable.
            host: CLOB API endpoint URL.
            max_retries: Maximum retry attempts for failed requests.
            requests_per_second: Rate limit for API requests.
        """
        self._api_key = api_key or os.environ.get("POLYMARKET_API_KEY")
        self._host = host
        self._max_retries = max_retries
        self._rate_limiter = RateLimiter(requests_per_second)

        # Initialize the underlying client (read-only, no auth needed for queries)
        self._client = BaseClobClient(host)

        logger.info(
            "Initialized ClobClient with host=%s, rate_limit=%.1f req/s",
            host,
            requests_per_second,
        )

    def _with_rate_limit(self, func: Callable[P, T]) -> Callable[P, T]:
        """Wrap a function with rate limiting."""

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            self._rate_limiter.acquire_sync()
            return func(*args, **kwargs)

        return wrapper

    @with_retry()
    def get_markets(self, active_only: bool = True) -> list[Market]:
        """Fetch all markets from Polymarket Gamma API.

        Uses Gamma API instead of CLOB simplified-markets because CLOB returns
        every market ever created (~350K+) with no server-side filter, making
        client-side `active_only` filtering O(N) over the entire history.
        Gamma supports active/closed filters server-side.

        Args:
            active_only: If True, only return active and non-closed markets.

        Returns:
            List of Market objects.
        """
        gamma_url = "https://gamma-api.polymarket.com/markets"
        page_limit = 500
        offset = 0
        all_markets: list[Market] = []

        params_base: dict[str, str | int] = {"limit": page_limit}
        if active_only:
            params_base["active"] = "true"
            params_base["closed"] = "false"

        while True:
            self._rate_limiter.acquire_sync()
            params = {**params_base, "offset": offset}
            response = httpx.get(gamma_url, params=params, timeout=30.0)
            response.raise_for_status()
            page = response.json()
            if not isinstance(page, list) or not page:
                break
            for market_data in page:
                try:
                    all_markets.append(Market.from_dict(_gamma_to_clob_market(market_data)))
                except (KeyError, ValueError, TypeError) as e:
                    logger.debug("skip market %s: %s", market_data.get("conditionId"), e)
            if len(page) < page_limit:
                break
            offset += page_limit

        logger.debug("Fetched %d markets", len(all_markets))
        return all_markets

    @with_retry()
    def get_market(self, condition_id: str) -> Market:
        """Fetch a specific market by its condition ID.

        Args:
            condition_id: The market's condition ID.

        Returns:
            Market object.

        Raises:
            ClobClientError: If the market is not found.
        """
        self._rate_limiter.acquire_sync()

        try:
            response = self._client.get_market(condition_id)
            return Market.from_dict(response)
        except Exception as e:
            raise ClobClientError(f"Failed to fetch market {condition_id}: {e}") from e

    @with_retry()
    def get_orderbook(self, token_id: str) -> Orderbook:
        """Fetch the orderbook for a specific token.

        Args:
            token_id: The token ID to fetch the orderbook for.

        Returns:
            Orderbook object with bids, asks, and spread information.
        """
        self._rate_limiter.acquire_sync()

        try:
            orderbook = self._client.get_order_book(token_id)
            return Orderbook.from_clob_orderbook(orderbook)
        except Exception as e:
            raise ClobClientError(f"Failed to fetch orderbook for {token_id}: {e}") from e

    @with_retry()
    def get_orderbooks(self, token_ids: list[str]) -> list[Orderbook]:
        """Fetch orderbooks for multiple tokens in a single request.

        Args:
            token_ids: List of token IDs to fetch orderbooks for.

        Returns:
            List of Orderbook objects.
        """
        self._rate_limiter.acquire_sync()

        params = [BookParams(token_id=tid) for tid in token_ids]

        try:
            orderbooks = self._client.get_order_books(params)
            return [Orderbook.from_clob_orderbook(ob) for ob in orderbooks]
        except Exception as e:
            raise ClobClientError(f"Failed to fetch orderbooks: {e}") from e

    @with_retry()
    def get_midpoint(self, token_id: str) -> str | None:
        """Fetch the midpoint price for a token.

        Args:
            token_id: The token ID.

        Returns:
            Midpoint price as a string, or None if unavailable.
        """
        self._rate_limiter.acquire_sync()

        try:
            response = self._client.get_midpoint(token_id)
            mid = response.get("mid")
            return str(mid) if mid is not None else None
        except Exception as e:
            logger.warning("Failed to get midpoint for %s: %s", token_id, e)
            return None

    @with_retry()
    def get_price(self, token_id: str, side: str = "BUY") -> str | None:
        """Fetch the best price for a token on a given side.

        Args:
            token_id: The token ID.
            side: Either "BUY" or "SELL".

        Returns:
            Best price as a string, or None if unavailable.
        """
        self._rate_limiter.acquire_sync()

        try:
            response = self._client.get_price(token_id, side=side)
            price = response.get("price")
            return str(price) if price is not None else None
        except Exception as e:
            logger.warning("Failed to get %s price for %s: %s", side, token_id, e)
            return None

    def health_check(self) -> bool:
        """Check if the CLOB API is reachable.

        Returns:
            True if the API responds with "OK", False otherwise.
        """
        try:
            self._rate_limiter.acquire_sync()
            result = self._client.get_ok()
            return str(result) == "OK"
        except Exception as e:
            logger.error("Health check failed: %s", e)
            return False

    def get_server_time(self) -> int | None:
        """Get the server timestamp.

        Returns:
            Server timestamp in milliseconds, or None on error.
        """
        try:
            self._rate_limiter.acquire_sync()
            result = self._client.get_server_time()
            return int(result) if result is not None else None
        except Exception as e:
            logger.error("Failed to get server time: %s", e)
            return None
