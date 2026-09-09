"""Fake for the py-clob-client SDK boundary."""

from __future__ import annotations

from typing import Any

from py_clob_client.clob_types import OrderBookSummary, OrderSummary

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


class FakeBaseClobClient:
    """In-memory stand-in for ``py_clob_client.client.ClobClient``.

    It serves the same response shapes the real SDK returns from the CLOB REST API: cursor-keyed
    simplified-market pages, market dictionaries, and real ``OrderBookSummary`` values. Failures are
    injected per operation with ``*_error`` attributes so tests can exercise the product's retry
    and fallback paths against the SDK's exception surface.
    """

    def __init__(self, pages: list[dict[str, Any]] | None = None) -> None:
        self.pages = pages if pages is not None else _default_pages()
        self.health_error: Exception | None = None
        self.market_error: Exception | None = None
        self.midpoint_error: Exception | None = None
        self.page_requests: list[str | None] = []
        self.health_requests = 0

    def get_ok(self) -> str:
        self.health_requests += 1
        if self.health_error is not None:
            raise self.health_error
        return "OK"

    @staticmethod
    def get_server_time() -> int:
        return 1704067200000

    def get_simplified_markets(self, next_cursor: str | None = None) -> dict[str, Any]:
        self.page_requests.append(next_cursor)
        cursors = [None] + [page["next_cursor"] for page in self.pages[:-1]]
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

    @staticmethod
    def get_order_book(token_id: str) -> OrderBookSummary:
        return OrderBookSummary(
            market="0xmarket",
            asset_id=token_id,
            tick_size="0.01",
            bids=[OrderSummary(price="0.50", size="100")],
            asks=[OrderSummary(price="0.52", size="150")],
        )

    @staticmethod
    def get_order_books(params: list[Any]) -> list[OrderBookSummary]:
        return [
            OrderBookSummary(
                market=f"m{index}", asset_id=str(param), tick_size="0.01", bids=[], asks=[]
            )
            for index, param in enumerate(params, 1)
        ]

    def get_midpoint(self, token_id: str) -> dict[str, str]:
        _ = token_id
        if self.midpoint_error is not None:
            raise self.midpoint_error
        return {"mid": "0.55"}

    @staticmethod
    def get_price(token_id: str, side: str = "BUY") -> dict[str, str]:
        _ = token_id
        return {"price": "0.53" if side == "BUY" else "0.51"}
