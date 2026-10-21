"""Tests for WebSocket trade stream handler."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

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


MINIMAL_TRADE_MESSAGE = json.dumps(
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


class StoppingWebSocket(FakeWebSocket):
    """Fake WebSocket that yields its messages, then stops the handler streaming from it."""

    def __init__(self, messages: list[str] | None = None) -> None:
        super().__init__(messages)
        self.handler: TradeStreamHandler | None = None

    async def __anext__(self) -> str:
        if self.messages:
            return self.messages.pop(0)
        assert self.handler is not None
        await self.handler.stop()
        raise StopAsyncIteration


class LostWebSocket(StoppingWebSocket):
    """Fake WebSocket whose connection drops before it yields anything."""

    async def __anext__(self) -> str:
        raise RuntimeError("connection lost")


class FakeConnect:
    """Stand-in for the module's ``ws_connect``: hands out scripted sockets, recording hosts."""

    def __init__(self, sockets: list[StoppingWebSocket]) -> None:
        self._sockets = list(sockets)
        self.hosts: list[str] = []

    async def __call__(self, host: str, **_options: object) -> StoppingWebSocket:
        self.hosts.append(host)
        return self._sockets.pop(0)


async def stream(
    handler: TradeStreamHandler, sockets: list[StoppingWebSocket], monkeypatch: pytest.MonkeyPatch
) -> FakeConnect:
    """Run ``handler`` over scripted sockets until the last one drains and stops it."""
    for socket in sockets:
        socket.handler = handler
    connect = FakeConnect(sockets)
    monkeypatch.setattr("polymarket_insider_tracker.ingestor.websocket.ws_connect", connect)
    await asyncio.wait_for(handler.start(), timeout=1.0)
    return connect


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


class TestDeprecation:
    """The WebSocket handler stays importable but is not the supported acquisition path."""

    def test_construction_emits_a_deprecation_warning(self) -> None:
        with pytest.warns(DeprecationWarning, match="TradeStreamHandler is deprecated") as caught:
            TradeStreamHandler(on_trade=RecordingTradeCallback())

        assert "TradePoller" in str(caught[0].message)


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

    @pytest.mark.asyncio
    async def test_connects_to_the_default_host(
        self, on_trade_recorder: RecordingTradeCallback, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = TradeStreamHandler(on_trade=on_trade_recorder)
        connect = await stream(handler, [StoppingWebSocket()], monkeypatch)

        assert connect.hosts == ["wss://ws-live-data.polymarket.com"]

    @pytest.mark.asyncio
    async def test_connects_to_a_custom_host(
        self, on_trade_recorder: RecordingTradeCallback, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handler with custom host."""
        handler = TradeStreamHandler(
            on_trade=on_trade_recorder,
            host="wss://custom.example.com",
        )
        connect = await stream(handler, [StoppingWebSocket()], monkeypatch)

        assert connect.hosts == ["wss://custom.example.com"]

    @pytest.mark.asyncio
    async def test_subscription_message_no_filter(
        self, handler: TradeStreamHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that connection sends a subscription message without filters."""
        ws = StoppingWebSocket()
        await stream(handler, [ws], monkeypatch)

        assert [json.loads(sent) for sent in ws.sent_messages] == [
            {
                "action": "subscribe",
                "subscriptions": [{"topic": "activity", "type": "trades"}],
            }
        ]

    @pytest.mark.asyncio
    async def test_subscription_message_with_event_filter(
        self, on_trade_recorder: RecordingTradeCallback, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test building subscription message with event filter."""
        handler = TradeStreamHandler(
            on_trade=on_trade_recorder,
            event_filter="test-event",
        )
        ws = StoppingWebSocket()
        await stream(handler, [ws], monkeypatch)

        msg = json.loads(ws.sent_messages[0])
        assert msg["subscriptions"][0]["filters"] == json.dumps({"event_slug": "test-event"})

    @pytest.mark.asyncio
    async def test_subscription_message_with_market_filter(
        self, on_trade_recorder: RecordingTradeCallback, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test building subscription message with market filter."""
        handler = TradeStreamHandler(
            on_trade=on_trade_recorder,
            market_filter="test-market",
        )
        ws = StoppingWebSocket()
        await stream(handler, [ws], monkeypatch)

        msg = json.loads(ws.sent_messages[0])
        assert msg["subscriptions"][0]["filters"] == json.dumps({"market_slug": "test-market"})

    @pytest.mark.asyncio
    async def test_handle_message_trade(
        self,
        handler: TradeStreamHandler,
        on_trade_recorder: RecordingTradeCallback,
        monkeypatch: pytest.MonkeyPatch,
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

        await stream(handler, [StoppingWebSocket([message])], monkeypatch)

        assert on_trade_recorder.called
        assert len(on_trade_recorder.trades) == 1
        trade: TradeEvent = on_trade_recorder.trades[0]
        assert trade.market_id == "0xmarket"
        assert trade.side == "BUY"
        assert trade.price == Decimal("0.65")
        assert handler.stats.trades_received == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "message",
        [
            # no transactionHash/proxyWallet
            json.dumps({"connection_id": "abc123", "payload": {"body": "Hello"}}),
            # transactionHash but no proxyWallet is not a trade
            json.dumps(
                {"connection_id": "abc", "payload": {"transactionHash": "0xtx", "other": "x"}}
            ),
            # no payload key is ignored
            json.dumps({"connection_id": "abc", "status": "ok"}),
            "not valid json",
        ],
        ids=["non-trade", "missing-proxy-wallet", "no-payload-key", "invalid-json"],
    )
    async def test_handle_message_ignores_non_trades(
        self,
        handler: TradeStreamHandler,
        on_trade_recorder: RecordingTradeCallback,
        monkeypatch: pytest.MonkeyPatch,
        message: str,
    ) -> None:
        """Messages that are not trades, or not JSON, never reach the trade callback."""
        await stream(handler, [StoppingWebSocket([message])], monkeypatch)

        assert not on_trade_recorder.called
        assert handler.stats.trades_received == 0

    @pytest.mark.asyncio
    async def test_handle_message_callback_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that callback errors don't crash the handler."""
        recorder = RecordingTradeCallback(raise_error=ValueError("Callback error"))
        handler = TradeStreamHandler(on_trade=recorder)

        message = MINIMAL_TRADE_MESSAGE

        # Should not raise
        await stream(handler, [StoppingWebSocket([message])], monkeypatch)

        # Trade was still counted
        assert recorder.called
        assert handler.stats.trades_received == 1

    @pytest.mark.asyncio
    async def test_state_changes_trigger_the_callback_once_each(
        self,
        handler: TradeStreamHandler,
        on_state_recorder: RecordingStateCallback,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Each state change calls back once; re-entering the same state does not."""
        await stream(handler, [StoppingWebSocket()], monkeypatch)

        assert on_state_recorder.states == [
            ConnectionState.CONNECTING,
            ConnectionState.CONNECTED,
            ConnectionState.DISCONNECTED,
        ]
        assert handler.state == ConnectionState.DISCONNECTED

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, handler: TradeStreamHandler) -> None:
        """Test stop when handler is not running."""
        # Should not raise
        await handler.stop()

    @pytest.mark.asyncio
    async def test_a_lost_connection_reconnects_and_keeps_streaming(
        self,
        handler: TradeStreamHandler,
        on_trade_recorder: RecordingTradeCallback,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A reconnect that leaves the handler running must assign its connection."""
        message = MINIMAL_TRADE_MESSAGE
        lost = LostWebSocket()
        revived = StoppingWebSocket([message])

        connect = await stream(handler, [lost, revived], monkeypatch)

        assert len(connect.hosts) == 2
        assert handler.stats.reconnect_count == 1
        assert len(on_trade_recorder.trades) == 1

    @pytest.mark.asyncio
    async def test_stopping_closes_the_websocket(
        self, handler: TradeStreamHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that stopping closes the WebSocket."""
        fake_ws = StoppingWebSocket()

        await stream(handler, [fake_ws], monkeypatch)

        assert fake_ws.closed is True
        assert handler.state == ConnectionState.DISCONNECTED

    @pytest.mark.asyncio
    async def test_context_manager(self, on_trade_recorder: RecordingTradeCallback) -> None:
        """Test async context manager."""
        handler = TradeStreamHandler(on_trade=on_trade_recorder)

        async with handler:
            pass

        # Should be stopped after exiting context
        assert handler.state == ConnectionState.DISCONNECTED


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
