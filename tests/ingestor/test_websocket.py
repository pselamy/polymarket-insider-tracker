"""Tests for WebSocket trade stream handler."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any, cast

import pytest

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.websocket import (
    ConnectionState,
    StreamStats,
    TradeStreamHandler,
)


class RecordingTradeCallback:
    """Fake callback for trade events."""

    def __init__(self, raise_error: Exception | None = None) -> None:
        self.trades: list[TradeEvent] = []
        self.called: bool = False
        self._raise_error = raise_error

    async def __call__(self, trade: TradeEvent) -> None:
        self.called = True
        self.trades.append(trade)
        if self._raise_error is not None:
            raise self._raise_error


class RecordingStateCallback:
    """Fake callback for connection state changes."""

    def __init__(self) -> None:
        self.states: list[ConnectionState] = []
        self.called: bool = False

    async def __call__(self, state: ConnectionState) -> None:
        self.called = True
        self.states.append(state)


class FakeWebSocket:
    """Fake WebSocket connection recording sent messages and yielding stream messages."""

    def __init__(self, messages: list[str] | None = None) -> None:
        self.messages = list(messages or [])
        self.sent_messages: list[str] = []
        self.closed: bool = False

    async def send(self, message: str) -> None:
        self.sent_messages.append(message)

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self) -> FakeWebSocket:
        return self

    async def __anext__(self) -> str:
        if self.closed or not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)


class TestStreamStats:
    """Tests for StreamStats."""

    def test_defaults(self) -> None:
        """Test default values."""
        stats = StreamStats()

        assert stats.trades_received == 0
        assert stats.reconnect_count == 0
        assert stats.last_trade_time is None
        assert stats.connected_since is None
        assert stats.last_error is None


class TestTradeStreamHandler:
    """Tests for TradeStreamHandler."""

    @pytest.fixture
    def on_trade_recorder(self) -> RecordingTradeCallback:
        """Create trade callback recorder."""
        return RecordingTradeCallback()

    @pytest.fixture
    def on_state_recorder(self) -> RecordingStateCallback:
        """Create state change callback recorder."""
        return RecordingStateCallback()

    @pytest.fixture
    def handler(
        self,
        on_trade_recorder: RecordingTradeCallback,
        on_state_recorder: RecordingStateCallback,
    ) -> TradeStreamHandler:
        """Create handler with recorders."""
        return TradeStreamHandler(
            on_trade=on_trade_recorder,
            on_state_change=on_state_recorder,
            initial_reconnect_delay=0.01,
            max_reconnect_delay=0.1,
        )

    def test_init_defaults(self, on_trade_recorder: RecordingTradeCallback) -> None:
        """Test handler initialization with defaults."""
        handler = TradeStreamHandler(on_trade=on_trade_recorder)

        assert handler.state == ConnectionState.DISCONNECTED
        assert handler.stats.trades_received == 0
        assert handler._host == "wss://ws-live-data.polymarket.com"

    def test_init_custom_host(self, on_trade_recorder: RecordingTradeCallback) -> None:
        """Test handler with custom host."""
        handler = TradeStreamHandler(
            on_trade=on_trade_recorder,
            host="wss://custom.example.com",
        )

        assert handler._host == "wss://custom.example.com"

    def test_init_with_event_filter(self, on_trade_recorder: RecordingTradeCallback) -> None:
        """Test handler with event filter."""
        handler = TradeStreamHandler(
            on_trade=on_trade_recorder,
            event_filter="presidential-election-2024",
        )

        assert handler._event_filter == "presidential-election-2024"

    def test_build_subscription_message_no_filter(self, handler: TradeStreamHandler) -> None:
        """Test building subscription message without filters."""
        msg = handler._build_subscription_message()

        assert msg == {
            "action": "subscribe",
            "subscriptions": [{"topic": "activity", "type": "trades"}],
        }

    def test_build_subscription_message_with_event_filter(
        self, on_trade_recorder: RecordingTradeCallback
    ) -> None:
        """Test building subscription message with event filter."""
        handler = TradeStreamHandler(
            on_trade=on_trade_recorder,
            event_filter="test-event",
        )
        msg = handler._build_subscription_message()

        assert msg["subscriptions"][0]["filters"] == json.dumps({"event_slug": "test-event"})

    def test_build_subscription_message_with_market_filter(
        self, on_trade_recorder: RecordingTradeCallback
    ) -> None:
        """Test building subscription message with market filter."""
        handler = TradeStreamHandler(
            on_trade=on_trade_recorder,
            market_filter="test-market",
        )
        msg = handler._build_subscription_message()

        assert msg["subscriptions"][0]["filters"] == json.dumps({"market_slug": "test-market"})

    @pytest.mark.asyncio
    async def test_handle_message_trade(
        self, handler: TradeStreamHandler, on_trade_recorder: RecordingTradeCallback
    ) -> None:
        """Test handling a valid trade message (payload-based routing)."""
        message = json.dumps(
            {
                "connection_id": "abc123",
                "payload": {
                    "conditionId": "0xmarket",
                    "transactionHash": "0xtx",
                    "proxyWallet": "0xwallet",
                    "side": "BUY",
                    "outcome": "Yes",
                    "price": 0.65,
                    "size": 100,
                    "timestamp": 1704067200,
                    "asset": "token123",
                },
            }
        )

        await handler._handle_message(message)

        assert on_trade_recorder.called
        assert len(on_trade_recorder.trades) == 1
        trade: TradeEvent = on_trade_recorder.trades[0]
        assert trade.market_id == "0xmarket"
        assert trade.side == "BUY"
        assert trade.price == Decimal("0.65")
        assert handler.stats.trades_received == 1

    @pytest.mark.asyncio
    async def test_handle_message_non_trade(
        self, handler: TradeStreamHandler, on_trade_recorder: RecordingTradeCallback
    ) -> None:
        """Test handling a non-trade message (no transactionHash/proxyWallet)."""
        message = json.dumps(
            {
                "connection_id": "abc123",
                "payload": {"body": "Hello"},
            }
        )

        await handler._handle_message(message)

        assert not on_trade_recorder.called
        assert handler.stats.trades_received == 0

    @pytest.mark.asyncio
    async def test_handle_message_payload_missing_proxy_wallet(
        self, handler: TradeStreamHandler, on_trade_recorder: RecordingTradeCallback
    ) -> None:
        """Ratchet: payload with transactionHash but no proxyWallet is not a trade."""
        message = json.dumps(
            {
                "connection_id": "abc",
                "payload": {"transactionHash": "0xtx", "other": "field"},
            }
        )

        await handler._handle_message(message)

        assert not on_trade_recorder.called
        assert handler.stats.trades_received == 0

    @pytest.mark.asyncio
    async def test_handle_message_no_payload_key(
        self, handler: TradeStreamHandler, on_trade_recorder: RecordingTradeCallback
    ) -> None:
        """Ratchet: message without payload key is ignored."""
        message = json.dumps({"connection_id": "abc", "status": "ok"})

        await handler._handle_message(message)

        assert not on_trade_recorder.called
        assert handler.stats.trades_received == 0

    @pytest.mark.asyncio
    async def test_handle_message_invalid_json(
        self, handler: TradeStreamHandler, on_trade_recorder: RecordingTradeCallback
    ) -> None:
        """Test handling invalid JSON message."""
        await handler._handle_message("not valid json")

        assert not on_trade_recorder.called
        assert handler.stats.trades_received == 0

    @pytest.mark.asyncio
    async def test_handle_message_callback_error(self) -> None:
        """Test that callback errors don't crash the handler."""
        recorder = RecordingTradeCallback(raise_error=ValueError("Callback error"))
        handler = TradeStreamHandler(on_trade=recorder)

        message = json.dumps(
            {
                "connection_id": "abc",
                "payload": {
                    "conditionId": "0x",
                    "transactionHash": "0x",
                    "proxyWallet": "0x",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                },
            }
        )

        # Should not raise
        await handler._handle_message(message)

        # Trade was still counted
        assert recorder.called
        assert handler.stats.trades_received == 1

    @pytest.mark.asyncio
    async def test_set_state_calls_callback(
        self, handler: TradeStreamHandler, on_state_recorder: RecordingStateCallback
    ) -> None:
        """Test that state changes trigger callback."""
        await handler._set_state(ConnectionState.CONNECTING)

        assert on_state_recorder.called
        assert on_state_recorder.states == [ConnectionState.CONNECTING]
        assert handler.state == ConnectionState.CONNECTING

    @pytest.mark.asyncio
    async def test_set_state_same_state_no_callback(
        self, handler: TradeStreamHandler, on_state_recorder: RecordingStateCallback
    ) -> None:
        """Test that same state doesn't trigger callback."""
        handler._state = ConnectionState.CONNECTED

        await handler._set_state(ConnectionState.CONNECTED)

        assert not on_state_recorder.called

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, handler: TradeStreamHandler) -> None:
        """Test stop when handler is not running."""
        # Should not raise
        await handler.stop()

    @pytest.mark.asyncio
    async def test_connect_sends_subscription(
        self,
        handler: TradeStreamHandler,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that connection sends subscription message."""
        fake_ws = FakeWebSocket()

        async def fake_ws_connect(*_args: Any, **_kwargs: Any) -> FakeWebSocket:
            return fake_ws

        monkeypatch.setattr(
            "polymarket_insider_tracker.ingestor.websocket.ws_connect",
            fake_ws_connect,
        )

        ws = await handler._connect()

        assert ws is fake_ws
        assert len(fake_ws.sent_messages) == 1

        # Verify subscription message includes action: subscribe
        sent_msg = json.loads(fake_ws.sent_messages[0])
        assert sent_msg["action"] == "subscribe"
        assert "subscriptions" in sent_msg
        assert sent_msg["subscriptions"][0]["topic"] == "activity"
        assert sent_msg["subscriptions"][0]["type"] == "trades"

    @pytest.mark.asyncio
    async def test_running_reconnect_returns_with_a_live_connection(
        self, handler: TradeStreamHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A reconnect that leaves the handler running must assign its connection."""
        fake_ws = FakeWebSocket()

        async def fake_connect() -> FakeWebSocket:
            return fake_ws

        async def fake_sleep(_delay: float) -> None:
            pass

        handler._running = True
        monkeypatch.setattr(handler, "_connect", fake_connect)
        monkeypatch.setattr(
            "polymarket_insider_tracker.ingestor.websocket.asyncio.sleep", fake_sleep
        )

        await handler._reconnect_loop()

        assert handler._running is True
        assert handler._ws is fake_ws

    @pytest.mark.asyncio
    async def test_cleanup_closes_websocket(self, handler: TradeStreamHandler) -> None:
        """Test that cleanup closes the WebSocket."""
        fake_ws = FakeWebSocket()
        handler._ws = cast(Any, fake_ws)

        await handler._cleanup()

        assert fake_ws.closed is True
        assert handler._ws is None
        assert handler.state == ConnectionState.DISCONNECTED

    @pytest.mark.asyncio
    async def test_context_manager(self, on_trade_recorder: RecordingTradeCallback) -> None:
        """Test async context manager."""
        handler = TradeStreamHandler(on_trade=on_trade_recorder)

        async with handler:
            pass

        # Should be stopped after exiting context
        assert handler._running is False


class TestTradeStreamHandlerIntegration:
    """Integration tests for TradeStreamHandler."""

    @pytest.mark.asyncio
    async def test_start_and_receive_trades(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test starting handler and receiving trades."""
        received_trades: list[TradeEvent] = []

        async def on_trade(trade: TradeEvent) -> None:
            received_trades.append(trade)

        handler = TradeStreamHandler(
            on_trade=on_trade,
            initial_reconnect_delay=0.01,
        )

        trade_message = json.dumps(
            {
                "connection_id": "test-conn",
                "payload": {
                    "conditionId": "0xtest",
                    "transactionHash": "0xtx",
                    "proxyWallet": "0xwallet",
                    "side": "BUY",
                    "outcome": "Yes",
                    "price": 0.75,
                    "size": 50,
                    "timestamp": 1704067200,
                    "asset": "token",
                },
            }
        )

        class StoppingFakeWebSocket(FakeWebSocket):
            """Fake WebSocket that yields one message then stops handler."""

            def __init__(self, target_handler: TradeStreamHandler, msg: str) -> None:
                super().__init__([msg])
                self._handler = target_handler

            async def __anext__(self) -> str:
                if self.messages:
                    return self.messages.pop(0)
                await self._handler.stop()
                raise StopAsyncIteration

        stopping_ws = StoppingFakeWebSocket(handler, trade_message)

        async def fake_ws_connect(*_args: Any, **_kwargs: Any) -> FakeWebSocket:
            return stopping_ws

        monkeypatch.setattr(
            "polymarket_insider_tracker.ingestor.websocket.ws_connect",
            fake_ws_connect,
        )

        # Run with timeout to prevent hanging
        try:
            await asyncio.wait_for(handler.start(), timeout=1.0)
        except TimeoutError:
            await handler.stop()

        # Verify trade was received
        assert len(received_trades) == 1
        assert received_trades[0].market_id == "0xtest"
        assert received_trades[0].side == "BUY"
        assert received_trades[0].price == Decimal("0.75")
