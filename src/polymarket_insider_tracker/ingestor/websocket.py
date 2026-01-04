"""WebSocket client for streaming Polymarket trade events."""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection

from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)

# Constants
DEFAULT_WS_HOST = "wss://ws-live-data.polymarket.com"
DEFAULT_PING_INTERVAL = 30  # seconds
DEFAULT_MAX_RECONNECT_DELAY = 30  # seconds
DEFAULT_INITIAL_RECONNECT_DELAY = 1  # seconds


class ConnectionState(Enum):
    """WebSocket connection states."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"


@dataclass
class StreamStats:
    """Statistics about the trade stream."""

    trades_received: int = 0
    reconnect_count: int = 0
    last_trade_time: float | None = None
    connected_since: float | None = None
    last_error: str | None = None


class TradeStreamError(Exception):
    """Base exception for trade stream errors."""


class ConnectionError(TradeStreamError):
    """Raised when connection to WebSocket fails."""


TradeCallback = Callable[[TradeEvent], Awaitable[None]]
StateCallback = Callable[[ConnectionState], Awaitable[None]]


class TradeStreamHandler:
    """WebSocket client for streaming Polymarket trade events.

    This handler maintains a persistent connection to Polymarket's real-time
    trade feed, automatically reconnecting on disconnection with exponential
    backoff.

    Example:
        >>> async def on_trade(trade: TradeEvent):
        ...     print(f"Trade: {trade.side} {trade.size} @ {trade.price}")
        ...
        >>> handler = TradeStreamHandler(on_trade=on_trade)
        >>> await handler.start()  # Blocks until stop() is called

    Attributes:
        state: Current connection state.
        stats: Statistics about the stream (trades received, reconnects, etc.).
    """

    def __init__(
        self,
        on_trade: TradeCallback,
        *,
        host: str = DEFAULT_WS_HOST,
        on_state_change: StateCallback | None = None,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        max_reconnect_delay: int = DEFAULT_MAX_RECONNECT_DELAY,
        initial_reconnect_delay: int = DEFAULT_INITIAL_RECONNECT_DELAY,
        event_filter: str | None = None,
        market_filter: str | None = None,
    ) -> None:
        """Initialize the trade stream handler.

        Args:
            on_trade: Async callback invoked for each trade event.
            host: WebSocket endpoint URL.
            on_state_change: Optional callback for connection state changes.
            ping_interval: Seconds between heartbeat pings.
            max_reconnect_delay: Maximum delay between reconnection attempts.
            initial_reconnect_delay: Initial delay for reconnection backoff.
            event_filter: Optional event slug to filter trades by event.
            market_filter: Optional market slug to filter trades by market.
        """
        self._on_trade = on_trade
        self._on_state_change = on_state_change
        self._host = host
        self._ping_interval = ping_interval
        self._max_reconnect_delay = max_reconnect_delay
        self._initial_reconnect_delay = initial_reconnect_delay
        self._event_filter = event_filter
        self._market_filter = market_filter

        self._state = ConnectionState.DISCONNECTED
        self._stats = StreamStats()
        self._ws: ClientConnection | None = None
        self._running = False
        self._stop_event: asyncio.Event | None = None

    @property
    def state(self) -> ConnectionState:
        """Current connection state."""
        return self._state

    @property
    def stats(self) -> StreamStats:
        """Stream statistics."""
        return self._stats

    async def _set_state(self, new_state: ConnectionState) -> None:
        """Update state and notify callback."""
        if self._state != new_state:
            old_state = self._state
            self._state = new_state
            logger.info("Connection state: %s -> %s", old_state.value, new_state.value)

            if self._on_state_change:
                try:
                    await self._on_state_change(new_state)
                except Exception as e:
                    logger.error("Error in state change callback: %s", e)

    def _build_subscription_message(self) -> dict[str, Any]:
        """Build the WebSocket subscription message."""
        subscription: dict[str, Any] = {
            "topic": "activity",
            "type": "trades",
        }

        # Add filters if specified
        if self._event_filter:
            subscription["filters"] = json.dumps({"event_slug": self._event_filter})
        elif self._market_filter:
            subscription["filters"] = json.dumps({"market_slug": self._market_filter})

        return {"subscriptions": [subscription]}

    async def _connect(self) -> ClientConnection:
        """Establish WebSocket connection."""
        await self._set_state(ConnectionState.CONNECTING)

        try:
            ws = await websockets.connect(
                self._host,
                ping_interval=self._ping_interval,
                ping_timeout=self._ping_interval * 2,
            )

            # Send subscription message
            subscribe_msg = self._build_subscription_message()
            await ws.send(json.dumps(subscribe_msg))

            logger.info("Connected to %s and subscribed to trades", self._host)
            await self._set_state(ConnectionState.CONNECTED)
            self._stats.connected_since = time.time()

            return ws

        except Exception as e:
            logger.error("Failed to connect: %s", e)
            self._stats.last_error = str(e)
            raise ConnectionError(f"Failed to connect to {self._host}: {e}") from e

    async def _handle_message(self, message: str) -> None:
        """Parse and process an incoming WebSocket message."""
        try:
            data = json.loads(message)

            # Check if this is a trade message
            topic = data.get("topic")
            msg_type = data.get("type")

            if topic == "activity" and msg_type == "trades":
                payload = data.get("payload", {})
                trade = TradeEvent.from_websocket_message(payload)

                self._stats.trades_received += 1
                self._stats.last_trade_time = time.time()

                logger.debug(
                    "Trade: %s %s @ %s on %s",
                    trade.side,
                    trade.size,
                    trade.price,
                    trade.market_slug,
                )

                try:
                    await self._on_trade(trade)
                except Exception as e:
                    logger.error("Error in trade callback: %s", e)

            else:
                # Log other message types for debugging
                logger.debug("Received message: topic=%s type=%s", topic, msg_type)

        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON message: %s", e)
        except Exception as e:
            logger.error("Error processing message: %s", e)

    async def _listen(self, ws: ClientConnection) -> None:
        """Listen for messages on the WebSocket."""
        try:
            async for message in ws:
                if not self._running:
                    break

                if isinstance(message, str):
                    await self._handle_message(message)
                else:
                    logger.debug("Received binary message (%d bytes)", len(message))

        except websockets.ConnectionClosed as e:
            logger.warning("Connection closed: %s", e)
            raise
        except Exception as e:
            logger.error("Error in message loop: %s", e)
            raise

    async def _reconnect_loop(self) -> None:
        """Handle reconnection with exponential backoff."""
        delay = self._initial_reconnect_delay

        while self._running:
            try:
                await self._set_state(ConnectionState.RECONNECTING)

                logger.info("Reconnecting in %.1f seconds...", delay)
                await asyncio.sleep(delay)

                if not self._running:
                    break

                self._ws = await self._connect()
                self._stats.reconnect_count += 1
                delay = self._initial_reconnect_delay  # Reset delay on success
                return

            except Exception as e:
                logger.error("Reconnection failed: %s", e)
                self._stats.last_error = str(e)

                # Exponential backoff with jitter
                delay = min(delay * 2, self._max_reconnect_delay)

    async def start(self) -> None:
        """Connect and begin streaming trades.

        This method blocks until stop() is called. It automatically
        handles reconnection on disconnection.

        Raises:
            ConnectionError: If initial connection fails.
        """
        if self._running:
            logger.warning("Handler already running")
            return

        self._running = True
        self._stop_event = asyncio.Event()

        try:
            # Initial connection
            self._ws = await self._connect()

            # Main loop
            while self._running:
                try:
                    await self._listen(self._ws)
                except (websockets.ConnectionClosed, Exception) as e:
                    if not self._running:
                        break

                    logger.warning("Connection lost: %s", e)
                    await self._set_state(ConnectionState.DISCONNECTED)

                    # Attempt reconnection
                    await self._reconnect_loop()

                    if not self._running or self._ws is None:
                        break

        finally:
            await self._cleanup()

    async def stop(self) -> None:
        """Gracefully disconnect from the WebSocket.

        This signals the handler to stop and cleanly close the connection.
        """
        if not self._running:
            return

        logger.info("Stopping trade stream handler...")
        self._running = False

        if self._stop_event:
            self._stop_event.set()

        await self._cleanup()

    async def _cleanup(self) -> None:
        """Clean up resources."""
        if self._ws:
            try:
                await self._ws.close()
            except Exception as e:
                logger.debug("Error closing WebSocket: %s", e)
            finally:
                self._ws = None

        await self._set_state(ConnectionState.DISCONNECTED)
        logger.info("Trade stream handler stopped")

    async def __aenter__(self) -> "TradeStreamHandler":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.stop()
