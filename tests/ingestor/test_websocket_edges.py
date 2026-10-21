"""Failure and edge handling of the deprecated WebSocket trade stream handler."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

import pytest
from websockets.exceptions import ConnectionClosed

from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.ingestor.websocket import (
    ConnectionError,
    ConnectionState,
    TradeStreamHandler,
)
from tests.ingestor.test_websocket import MINIMAL_TRADE_MESSAGE

WS_CONNECT = "polymarket_insider_tracker.ingestor.websocket.ws_connect"

Item = str | bytes | Exception


class ScriptedSocket:
    """Yields scripted frames (or raises scripted errors), then stops its handler."""

    def __init__(self, items: list[Item], *, close_error: Exception | None = None) -> None:
        self.items = list(items)
        self.sent: list[str] = []
        self.close_attempts = 0
        self.handler: TradeStreamHandler | None = None
        self._close_error = close_error

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.close_attempts += 1
        if self._close_error is not None:
            raise self._close_error

    def __aiter__(self) -> ScriptedSocket:
        return self

    async def __anext__(self) -> str | bytes:
        if self.items:
            item = self.items.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        assert self.handler is not None
        await self.handler.stop()
        raise StopAsyncIteration


class ScriptedConnect:
    """Stand-in for ``ws_connect``: each call returns the next socket or raises the next error."""

    def __init__(self, script: list[ScriptedSocket | Exception]) -> None:
        self._script = list(script)
        self.attempts = 0

    async def __call__(self, host: str, **_options: object) -> ScriptedSocket:
        _ = host
        self.attempts += 1
        step = self._script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


class Trades:
    def __init__(self) -> None:
        self.seen: list[TradeEvent] = []

    async def __call__(self, trade: TradeEvent) -> None:
        self.seen.append(trade)


async def run(
    handler: TradeStreamHandler,
    script: list[ScriptedSocket | Exception],
    monkeypatch: pytest.MonkeyPatch,
) -> ScriptedConnect:
    """Run ``handler`` against ``script``; sockets stop it once their items run out."""
    for step in script:
        if isinstance(step, ScriptedSocket):
            step.handler = handler
    connect = ScriptedConnect(script)
    monkeypatch.setattr(WS_CONNECT, connect)
    await asyncio.wait_for(handler.start(), timeout=2.0)
    return connect


def make_handler(
    trades: Trades, on_state_change: Callable[[ConnectionState], Awaitable[None]] | None = None
) -> TradeStreamHandler:
    return TradeStreamHandler(
        trades,
        on_state_change=on_state_change,
        initial_reconnect_delay=0.01,
        max_reconnect_delay=0.04,
    )


async def test_failing_state_callback_does_not_break_streaming(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def explode(_state: ConnectionState) -> None:
        raise RuntimeError("state callback boom")

    trades = Trades()
    handler = make_handler(trades, explode)

    with caplog.at_level(logging.ERROR):
        await run(handler, [ScriptedSocket([MINIMAL_TRADE_MESSAGE])], monkeypatch)

    assert len(trades.seen) == 1
    assert "Error in state change callback: state callback boom" in caplog.text


async def test_binary_frames_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    trades = Trades()
    handler = make_handler(trades)

    await run(handler, [ScriptedSocket([b"\x00\x01", MINIMAL_TRADE_MESSAGE])], monkeypatch)

    assert len(trades.seen) == 1


@pytest.mark.parametrize(
    ("frame", "logged"),
    [
        ("{not json", "Invalid JSON message"),
        ("[1, 2]", "Error processing message: WebSocket message is not a JSON object"),
        (
            json.dumps({"payload": {"transactionHash": "0x", "proxyWallet": "0x", "price": "abc"}}),
            "Error processing message",
        ),
    ],
)
async def test_malformed_frames_are_logged_and_streaming_continues(
    frame: str, logged: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    trades = Trades()
    handler = make_handler(trades)

    with caplog.at_level(logging.WARNING):
        await run(handler, [ScriptedSocket([frame, MINIMAL_TRADE_MESSAGE])], monkeypatch)

    assert logged in caplog.text
    assert len(trades.seen) == 1


async def test_initial_connection_failure_is_raised_and_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = make_handler(Trades())
    monkeypatch.setattr(WS_CONNECT, ScriptedConnect([OSError("refused")]))

    with pytest.raises(ConnectionError, match="refused"):
        await handler.start()

    assert handler.stats.last_error == "refused"


async def test_closed_connection_reconnects_and_resumes(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    trades = Trades()
    handler = make_handler(trades)
    closed = ScriptedSocket([ConnectionClosed(None, None)])
    revived = ScriptedSocket([MINIMAL_TRADE_MESSAGE])

    with caplog.at_level(logging.WARNING):
        connect = await run(handler, [closed, revived], monkeypatch)

    assert connect.attempts == 2
    assert handler.stats.reconnect_count == 1
    assert len(trades.seen) == 1
    assert "Connection closed" in caplog.text


async def test_failed_reconnects_back_off_until_one_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trades = Trades()
    handler = make_handler(trades)
    script: list[ScriptedSocket | Exception] = [
        ScriptedSocket([RuntimeError("dropped")]),
        OSError("still down"),
        OSError("still down"),
        ScriptedSocket([MINIMAL_TRADE_MESSAGE]),
    ]

    connect = await run(handler, script, monkeypatch)

    assert connect.attempts == 4
    assert handler.stats.reconnect_count == 1
    assert len(trades.seen) == 1


async def test_stop_during_the_reconnect_delay_abandons_reconnecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = TradeStreamHandler(Trades(), initial_reconnect_delay=0.2)
    dropped = ScriptedSocket([RuntimeError("dropped")])
    dropped.handler = handler
    connect = ScriptedConnect([dropped])
    monkeypatch.setattr(WS_CONNECT, connect)

    runner = asyncio.create_task(handler.start())
    while handler.state != ConnectionState.RECONNECTING:
        await asyncio.sleep(0.01)
    await handler.stop()
    await asyncio.wait_for(runner, timeout=2.0)

    assert connect.attempts == 1
    assert handler.stats.reconnect_count == 0


async def test_stop_while_a_frame_is_pending_ends_the_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trades = Trades()
    handler = make_handler(trades)

    class StopThenDeliver(ScriptedSocket):
        async def __anext__(self) -> str | bytes:
            if self.items:
                await handler.stop()
            return await super().__anext__()

    socket = StopThenDeliver([MINIMAL_TRADE_MESSAGE])
    await run(handler, [socket], monkeypatch)

    assert trades.seen == []


async def test_close_failure_during_cleanup_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = make_handler(Trades())
    socket = ScriptedSocket([], close_error=RuntimeError("close boom"))

    await run(handler, [socket], monkeypatch)

    assert socket.close_attempts >= 1
    assert handler.state == ConnectionState.DISCONNECTED


class StopThenFail(ScriptedSocket):
    """Socket whose connection errors out only after the handler was asked to stop."""

    async def __anext__(self) -> str | bytes:
        assert self.handler is not None
        await self.handler.stop()
        raise RuntimeError("late failure")


async def test_connection_loss_after_stop_does_not_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = make_handler(Trades())
    connect = await run(handler, [StopThenFail([])], monkeypatch)

    assert connect.attempts == 1
    assert handler.stats.reconnect_count == 0


async def test_reconnect_that_fails_while_stopping_gives_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = make_handler(Trades())
    dropped = ScriptedSocket([RuntimeError("dropped")])
    dropped.handler = handler
    attempts = 0

    async def connect(_host: str, **_options: object) -> ScriptedSocket:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return dropped
        await handler.stop()
        raise OSError("down while stopping")

    monkeypatch.setattr(WS_CONNECT, connect)
    await asyncio.wait_for(handler.start(), timeout=2.0)

    assert attempts == 2
    assert (handler.stats.last_error or "").endswith("down while stopping")


class GatedSocket(ScriptedSocket):
    """Socket that delivers nothing until ``release`` is set."""

    def __init__(self) -> None:
        super().__init__([])
        self.release = asyncio.Event()

    async def __anext__(self) -> str | bytes:
        await self.release.wait()
        return await super().__anext__()


async def test_starting_twice_leaves_the_first_run_untouched(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    handler = make_handler(Trades())
    socket = GatedSocket()
    socket.handler = handler
    connect = ScriptedConnect([socket])
    monkeypatch.setattr(WS_CONNECT, connect)

    with caplog.at_level(logging.WARNING):
        first = asyncio.create_task(handler.start())
        while handler.state != ConnectionState.CONNECTED:
            await asyncio.sleep(0.01)
        await handler.start()
        socket.release.set()
        await asyncio.wait_for(first, timeout=2.0)

    assert "Handler already running" in caplog.text
    assert connect.attempts == 1
