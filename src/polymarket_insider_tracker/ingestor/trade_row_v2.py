"""Decode documented Data API v2 trade rows into the existing parser input contract.

Data API v2 returns ``snake_case`` trade rows in a ``{data, pagination}`` envelope; the existing
parser consumes the historical v1 ``camelCase`` row shape. This module renames only the documented
v2 fields onto that input contract and delegates to
:func:`polymarket_insider_tracker.ingestor.trade_rows.parse_trade_row`, so required-field
validation, numeric rules, optional outcome repair, and composite identity stay the single
implementation owned by the v1 parser. No value is manufactured and unknown fields are passed
through unchanged for the parser to ignore, exactly as it treats unknown v1 fields.

Field names are cited from the official documentation:
https://docs.polymarket.com/migrate/data-api-v1-to-v2 and
https://docs.polymarket.com/api-reference/feeds/list-trades.
"""

from __future__ import annotations

from typing import Any, cast

from polymarket_insider_tracker.ingestor.trade_rows import (
    InvalidRow,
    TradeObservation,
    parse_trade_row,
)

V2_TO_V1_FIELDS: dict[str, str] = {
    "proxy_wallet": "proxyWallet",
    "condition_id": "conditionId",
    "token_id": "asset",
    "transaction_hash": "transactionHash",
    "outcome_index": "outcomeIndex",
    "event_slug": "eventSlug",
    "profile_image": "profileImage",
    "profile_image_optimized": "profileImageOptimized",
}


def _translate_row(row: dict[str, Any]) -> dict[str, Any]:
    translated = dict(row)
    for v2_name, v1_name in V2_TO_V1_FIELDS.items():
        if v2_name in row:
            translated[v1_name] = translated.pop(v2_name)
    return translated


def parse_v2_trade_row(raw: object, *, now: int) -> TradeObservation | InvalidRow:
    """Decode one documented v2 row and parse it through the existing v1 contract."""
    if not isinstance(raw, dict):
        return parse_trade_row(raw, now=now)
    return parse_trade_row(_translate_row(cast(dict[str, Any], raw)), now=now)
