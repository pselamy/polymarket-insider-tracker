"""Issue #93 host validation and connection-failure chain, property-based (PR #238 fix).

Each test asserts one property over a generated space of URL variants: scheme and host case
mixing, nested-scheme depth and position, fragments, and embedded credentials. Simulation only:
the connector is replaced by a local fake that raises, so no network is contacted.
"""

from __future__ import annotations

import asyncio
import string
import traceback

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from websockets.exceptions import InvalidURI

from polymarket_insider_tracker.config import validate_websocket_url
from polymarket_insider_tracker.ingestor import websocket as ws_module
from polymarket_insider_tracker.ingestor.websocket import ConnectionError as WsConnectionError
from polymarket_insider_tracker.ingestor.websocket import StreamConnection, TradeStreamHandler

settings.register_profile(
    "repo",
    derandomize=True,
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("repo")

NESTED_SCHEME_NAMES = ("http:", "https:", "ws:", "wss:")
FRAGMENT_ALPHABET = string.ascii_letters + string.digits + "-._~!$&'()*+,;=:@/?"


def _rejection(url: str) -> str | None:
    """The validator's rejection message for ``url``, or ``None`` when it is accepted."""
    try:
        validate_websocket_url(url)
    except ValueError as exc:
        return str(exc)
    return None


def _case_variant(token: str) -> st.SearchStrategy[str]:
    """Every case mixing of ``token``: each character independently upper or lower."""
    flags = st.lists(st.booleans(), min_size=len(token), max_size=len(token))
    return flags.map(
        lambda upper: "".join(c.upper() if up else c for c, up in zip(token, upper, strict=True))
    )


def _nested_chain(tokens: list[str]) -> str:
    """``tokens`` as a chain of nested scheme prefixes, e.g. ``HTTPS://WS://``."""
    return "".join(f"{token}//" for token in tokens)


@given(
    scheme=st.sampled_from(NESTED_SCHEME_NAMES).flatmap(_case_variant),
    depth=st.integers(min_value=1, max_value=3),
    in_path=st.booleans(),
)
def test_a_nested_scheme_is_rejected_in_any_case_at_any_depth_and_position(
    scheme: str, depth: int, in_path: bool
) -> None:
    # Arrange
    nested = _nested_chain([scheme] * depth)
    url = (
        f"wss://data-api.polymarket.com/{nested}x"
        if in_path
        else f"wss://{nested}data-api.polymarket.com"
    )

    # Act
    error = _rejection(url)

    # Assert
    assert error is not None and "nested scheme" in error


@pytest.mark.parametrize(
    "url",
    [
        "wss://https://data-api.polymarket.com",
        "wss://HTTPS://data-api.polymarket.com",
        "wss://Https://data-api.polymarket.com",
    ],
    ids=["lower", "upper", "mixed"],
)
def test_the_issue_93_shape_is_rejected_in_every_case_variant(url: str) -> None:
    # Arrange
    # (the URL is the parameter)

    # Act
    error = _rejection(url)

    # Assert
    assert error is not None and "nested scheme" in error


@given(fragment=st.text(alphabet=FRAGMENT_ALPHABET, min_size=0, max_size=20))
def test_a_fragment_is_rejected_at_initialization(fragment: str) -> None:
    # Arrange
    url = f"wss://data-api.polymarket.com/ws#{fragment}"

    # Act
    error = _rejection(url)

    # Assert
    assert error is not None and "fragment" in error


HOST_LABEL = st.from_regex(r"[a-z][a-z0-9]{0,11}", fullmatch=True)


@given(
    labels=st.lists(HOST_LABEL, min_size=1, max_size=3),
    port=st.none() | st.integers(min_value=1, max_value=65535),
)
def test_a_websocket_host_without_a_nested_scheme_is_accepted(
    labels: list[str], port: int | None
) -> None:
    # Arrange
    host = ".".join([*labels, "com"])
    authority = host if port is None else f"{host}:{port}"
    url = f"wss://{authority}/ws"

    # Act
    error = _rejection(url)

    # Assert
    assert error is None


SECRET = st.from_regex(r"PW[A-Za-z0-9]{8,20}", fullmatch=True)


@given(secret=SECRET)
def test_the_connection_failure_chain_never_contains_the_credential(secret: str) -> None:
    # Arrange
    host = f"wss://user:{secret}@ws.example.com/feed"

    async def refused_with_raw_uri(
        uri: str, ping_interval: int = 30, ping_timeout: int = 60
    ) -> StreamConnection:
        _ = (ping_interval, ping_timeout)
        raise InvalidURI(uri, "simulated rejection")

    async def _on_trade(_trade: object) -> None:
        return None

    handler = TradeStreamHandler(on_trade=_on_trade, host=host)
    module_namespace = ws_module.__dict__
    original = module_namespace["ws_connect"]
    module_namespace["ws_connect"] = refused_with_raw_uri
    try:
        # Act
        error = _start_and_capture(handler)
    finally:
        module_namespace["ws_connect"] = original

    # Assert
    assert secret not in "".join(traceback.format_exception(error))


def _start_and_capture(handler: TradeStreamHandler) -> BaseException:
    async def _run() -> BaseException:
        try:
            await handler.start()
        except BaseException as exc:
            return exc
        raise AssertionError("start() unexpectedly succeeded")

    return asyncio.run(_run())


@pytest.mark.asyncio
async def test_a_connection_failure_does_not_chain_the_raw_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    async def unresolvable(
        _uri: str, ping_interval: int = 30, ping_timeout: int = 60
    ) -> StreamConnection:
        _ = (ping_interval, ping_timeout)
        raise OSError(11001, "getaddrinfo failed")

    async def _on_trade(_trade: object) -> None:
        return None

    monkeypatch.setattr(ws_module, "ws_connect", unresolvable)
    handler = TradeStreamHandler(on_trade=_on_trade, host="wss://ws-live-data.polymarket.com")

    # Act
    with pytest.raises(WsConnectionError) as exc_info:
        await handler.start()

    # Assert
    assert exc_info.value.__cause__ is None
