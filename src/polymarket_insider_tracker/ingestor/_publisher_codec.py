"""Pure Redis stream event codec for trade events.

This module holds the side-effect-free encode/decode helpers used by
:mod:`polymarket_insider_tracker.ingestor.publisher` to translate between
:class:`~polymarket_insider_tracker.ingestor.models.TradeEvent` instances and
the string key-value maps stored in Redis Streams. It performs no Redis I/O.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from .models import TradeEvent

_DEFAULT_SIDE: Literal["BUY", "SELL"] = "BUY"


def serialize_trade_event(event: TradeEvent) -> dict[str, str]:
    """Serialize a TradeEvent to a dict suitable for Redis Streams.

    Redis Streams require string key-value pairs, so we convert all
    values to strings.

    Args:
        event: The TradeEvent to serialize.

    Returns:
        Dictionary with string keys and values.
    """
    return {
        "market_id": event.market_id,
        "trade_id": event.trade_id,
        "wallet_address": event.wallet_address,
        "side": event.side,
        "outcome": event.outcome,
        "outcome_index": str(event.outcome_index),
        "price": str(event.price),
        "size": str(event.size),
        "timestamp": event.timestamp.isoformat(),
        "asset_id": event.asset_id,
        "market_slug": event.market_slug,
        "event_slug": event.event_slug,
        "event_title": event.event_title,
        "trader_name": event.trader_name,
        "trader_pseudonym": event.trader_pseudonym,
    }


def decode_stream_dict(data: dict[bytes | str, bytes | str]) -> dict[str, str]:
    decoded: dict[str, str] = {}
    for k, v in data.items():
        key = k.decode() if isinstance(k, bytes) else str(k)
        value = v.decode() if isinstance(v, bytes) else str(v)
        decoded[key] = value
    return decoded


def parse_event_timestamp(timestamp_str: str | None) -> datetime:
    if timestamp_str is None:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(timestamp_str)
    except (ValueError, TypeError):
        return datetime.now(UTC)


def decode_entry_id(entry_id: bytes | str) -> str:
    return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)


def deserialize_trade_event(data: dict[bytes | str, bytes | str]) -> TradeEvent:
    """Deserialize a TradeEvent from Redis Stream data.

    Args:
        data: The raw data from Redis Stream (may have bytes keys/values).

    Returns:
        TradeEvent instance.
    """
    decoded = decode_stream_dict(data)
    timestamp = parse_event_timestamp(decoded.get("timestamp"))

    side_raw = decoded.get("side", _DEFAULT_SIDE).upper()
    side: Literal["BUY", "SELL"] = "BUY" if side_raw == "BUY" else "SELL"

    return TradeEvent(
        market_id=decoded.get("market_id", ""),
        trade_id=decoded.get("trade_id", ""),
        wallet_address=decoded.get("wallet_address", ""),
        side=side,
        outcome=decoded.get("outcome", ""),
        outcome_index=int(decoded.get("outcome_index", "0")),
        price=Decimal(decoded.get("price", "0")),
        size=Decimal(decoded.get("size", "0")),
        timestamp=timestamp,
        asset_id=decoded.get("asset_id", ""),
        market_slug=decoded.get("market_slug", ""),
        event_slug=decoded.get("event_slug", ""),
        event_title=decoded.get("event_title", ""),
        trader_name=decoded.get("trader_name", ""),
        trader_pseudonym=decoded.get("trader_pseudonym", ""),
    )
