"""Fake for the py-clob-client SDK boundary."""

from __future__ import annotations

import threading
from typing import Any

from py_clob_client.clob_types import BookParams, OrderBookSummary, OrderSummary

TERMINAL_CURSOR = "LTE="


def _default_pages() -> list[dict[str, Any]]:
    return [
        {
            "data": [
                {"condition_id": "0x123", "question": "Test market?", "tokens": [], "closed": False}
            ],
            "next_cursor": TERMINAL_CURSOR,
        }
    ]


def order_summary(price: str, size: str) -> OrderSummary:
    summary = OrderSummary()
    summary.price = price
    summary.size = size
    return summary


def order_book_summary(
    market: str,
    asset_id: str,
    tick_size: str,
    bids: list[OrderSummary] | None,
    asks: list[OrderSummary] | None,
) -> OrderBookSummary:
    """A real SDK ``OrderBookSummary`` (its constructor takes no arguments, so fields are set)."""
    book = OrderBookSummary()
    book.market = market
    book.asset_id = asset_id
    book.tick_size = tick_size
    book.bids = bids
    book.asks = asks
    return book


class FakeBaseClobClient:
    """In-memory stand-in for ``py_clob_client.client.ClobClient``.

    It serves the same response shapes the real SDK returns from the CLOB REST API: cursor-keyed
    simplified-market pages, market dictionaries, and real ``OrderBookSummary`` values. Failures are
    injected per operation with ``*_error`` attributes so tests can exercise the product's retry
    and fallback paths against the SDK's exception surface.
    """

    def __init__(self, pages: list[dict[str, Any]] | None = None) -> None:
        self.pages = pages if pages is not None else _default_pages()
        self.crawl_gate: threading.Event | None = None
        self.health_error: Exception | None = None
        self.market_error: Exception | None = None
        self.markets_error: Exception | None = None
        self.midpoint_error: Exception | None = None
        self.orderbook_error: Exception | None = None
        self.price_error: Exception | None = None
        self.server_time_error: Exception | None = None
        self.page_requests: list[str | None] = []
        self.health_requests = 0

    def get_ok(self) -> str:
        self.health_requests += 1
        if self.health_error is not None:
            raise self.health_error
        return "OK"

    def get_server_time(self) -> int:
        if self.server_time_error is not None:
            raise self.server_time_error
        return 1704067200000

    def get_simplified_markets(self, next_cursor: str | None = None) -> dict[str, Any]:
        """Serve one page; when ``crawl_gate`` is set, the API answers only once it is released."""
        if self.crawl_gate is not None:
            self.crawl_gate.wait()
        if self.markets_error is not None:
            raise self.markets_error
        self.page_requests.append(next_cursor)
        cursors: list[str | None] = [None, *(page["next_cursor"] for page in self.pages[:-1])]
        return self.pages[cursors.index(next_cursor)]

    def get_market(self, condition_id: str) -> dict[str, Any]:
        if self.market_error is not None:
            raise self.market_error
        return {
            "condition_id": condition_id,
            "question": "Will it happen?",
            "tokens": [
                {"token_id": "t1", "outcome": "Yes"},
                {"token_id": "t2", "outcome": "No"},
            ],
        }

    def get_order_book(self, token_id: str) -> OrderBookSummary:
        if self.orderbook_error is not None:
            raise self.orderbook_error
        return order_book_summary(
            "0xmarket",
            token_id,
            "0.01",
            [order_summary("0.50", "100")],
            [order_summary("0.52", "150")],
        )

    def get_order_books(self, params: list[BookParams]) -> list[OrderBookSummary]:
        return [self.get_order_book(param.token_id) for param in params]

    def get_midpoint(self, token_id: str) -> dict[str, str]:
        _ = token_id
        if self.midpoint_error is not None:
            raise self.midpoint_error
        return {"mid": "0.55"}

    def get_price(self, token_id: str, side: str = "BUY") -> dict[str, str]:
        _ = token_id
        if self.price_error is not None:
            raise self.price_error
        return {"price": "0.53" if side == "BUY" else "0.51"}
