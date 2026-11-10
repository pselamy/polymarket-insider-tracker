"""Issue #93 WebSocket host validation and connection-failure behaviour (PR #238).

Kept apart from test_websocket.py, which is already over the 398-line ceiling on main.
"""

from __future__ import annotations

import logging

import pytest

from polymarket_insider_tracker.ingestor.websocket import (
    ConnectionError as WsConnectionError,
)
from polymarket_insider_tracker.ingestor.websocket import (
    ConnectionState,
    StreamConnection,
    TradeStreamHandler,
)
from tests.ingestor.test_websocket import RecordingTradeCallback, StoppingWebSocket, stream


class TestIssue93WebsocketValidationAndConnectionFailure:
    """Verify validation and failure behaviour for issue #93 URL patterns."""

    def test_malformed_nested_scheme_host_rejected_at_initialization(self) -> None:
        """Issue #93 reported failure connecting to wss://https://data-api.polymarket.com.

        The handler must reject malformed nested schemes at initialization.
        """
        recorder = RecordingTradeCallback()
        with pytest.raises(ValueError, match="nested scheme"):
            TradeStreamHandler(on_trade=recorder, host="wss://https://data-api.polymarket.com")

    def test_non_websocket_http_host_rejected_at_initialization(self) -> None:
        """An HTTP(S) REST endpoint must not be accepted as a WebSocket host."""
        recorder = RecordingTradeCallback()
        with pytest.raises(ValueError, match="must start with ws:// or wss://"):
            TradeStreamHandler(on_trade=recorder, host="https://data-api.polymarket.com")

    async def _dns_failure(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> tuple[WsConnectionError, TradeStreamHandler]:
        """Run a getaddrinfo failure through a fake feed; return the raised error and handler."""

        async def fake_failing_connect(
            _host: str, ping_interval: int = 30, ping_timeout: int = 60
        ) -> StreamConnection:
            _ = (ping_interval, ping_timeout)
            raise OSError(11001, "getaddrinfo failed")

        monkeypatch.setattr(
            "polymarket_insider_tracker.ingestor.websocket.ws_connect",
            fake_failing_connect,
        )
        recorder = RecordingTradeCallback()
        handler = TradeStreamHandler(on_trade=recorder, host="wss://ws-live-data.polymarket.com")

        with caplog.at_level(logging.ERROR), pytest.raises(WsConnectionError) as exc_info:
            await handler.start()

        return exc_info.value, handler

    @pytest.mark.asyncio
    async def test_reproduce_issue_93_error_names_the_connection_failure(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Reproduce issue #93 getaddrinfo failure: the error names the connection failure."""
        error, _ = await self._dns_failure(monkeypatch, caplog)

        assert "Failed to connect" in str(error)

    @pytest.mark.asyncio
    async def test_reproduce_issue_93_error_carries_the_os_reason(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The raised error carries the operating-system reason."""
        error, _ = await self._dns_failure(monkeypatch, caplog)

        assert "getaddrinfo failed" in str(error)

    @pytest.mark.asyncio
    async def test_reproduce_issue_93_error_log_names_the_connection_failure(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The error log line names the connection failure."""
        await self._dns_failure(monkeypatch, caplog)

        assert "Failed to connect:" in caplog.text

    @pytest.mark.asyncio
    async def test_reproduce_issue_93_error_log_carries_the_os_reason(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The error log line carries the operating-system reason."""
        await self._dns_failure(monkeypatch, caplog)

        assert "getaddrinfo failed" in caplog.text

    @pytest.mark.asyncio
    async def test_reproduce_issue_93_handler_is_disconnected_after_the_failure(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """After the failure the handler is disconnected."""
        _, handler = await self._dns_failure(monkeypatch, caplog)

        assert handler.state == ConnectionState.DISCONNECTED

    @pytest.mark.asyncio
    async def test_reproduce_issue_93_last_error_carries_the_os_reason(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The handler's last_error records the operating-system reason."""
        _, handler = await self._dns_failure(monkeypatch, caplog)

        assert "getaddrinfo failed" in (handler.stats.last_error or "")

    @pytest.mark.asyncio
    async def test_reproduce_issue_93_error_does_not_chain_the_raw_failure(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Negative control: the raised error must not chain the raw OSError as its cause.

        Fails on 940432d8, where the error is raised ``from e``.
        """
        error, _ = await self._dns_failure(monkeypatch, caplog)

        assert error.__cause__ is None

    async def _connection_error_for_secret_host(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> WsConnectionError:
        """Connect to a credential-bearing host through a connector that always fails."""

        async def failing_connect(
            _host: str, ping_interval: int = 30, ping_timeout: int = 60
        ) -> StreamConnection:
            _ = (ping_interval, ping_timeout)
            raise OSError("refused")

        monkeypatch.setattr(
            "polymarket_insider_tracker.ingestor.websocket.ws_connect",
            failing_connect,
        )
        recorder = RecordingTradeCallback()
        secret_host = "wss://user:SECRET_PASS_123@ws.example.com"
        handler = TradeStreamHandler(on_trade=recorder, host=secret_host)

        with pytest.raises(WsConnectionError) as exc_info:
            await handler.start()

        return exc_info.value

    async def _connected_log_text(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> str:
        """Connect successfully to a secret-bearing host; return the captured INFO log text."""
        secret_host = "wss://user:SECRET_PASS_123@ws.example.com?token=SECRET_TOKEN"
        recorder = RecordingTradeCallback()
        handler = TradeStreamHandler(on_trade=recorder, host=secret_host)
        ws = StoppingWebSocket()

        with caplog.at_level(logging.INFO):
            await stream(handler, [ws], monkeypatch)

        return caplog.text

    @pytest.mark.asyncio
    async def test_connection_error_omits_the_host_password(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The host password never appears in the ConnectionError text."""
        error = await self._connection_error_for_secret_host(monkeypatch)

        assert "SECRET_PASS_123" not in str(error)

    @pytest.mark.asyncio
    async def test_connection_error_masks_the_host_credential(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The host credential is replaced by the *** mask in the ConnectionError text."""
        error = await self._connection_error_for_secret_host(monkeypatch)

        assert "***" in str(error)

    @pytest.mark.asyncio
    async def test_info_log_announces_the_connection(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The successful connection is announced in the INFO log."""
        text = await self._connected_log_text(monkeypatch, caplog)

        assert "Connected to" in text

    @pytest.mark.asyncio
    async def test_info_log_masks_the_userinfo_and_the_query(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The INFO log shows the URL with its password and query value masked."""
        text = await self._connected_log_text(monkeypatch, caplog)

        assert "wss://user:***@ws.example.com?token=***" in text

    @pytest.mark.asyncio
    async def test_info_log_omits_the_password(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The host password never appears in the INFO log."""
        text = await self._connected_log_text(monkeypatch, caplog)

        assert "SECRET_PASS_123" not in text

    @pytest.mark.asyncio
    async def test_info_log_omits_the_query_token(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The query token never appears in the INFO log."""
        text = await self._connected_log_text(monkeypatch, caplog)

        assert "SECRET_TOKEN" not in text
