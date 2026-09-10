"""Strict parsing of public trade rows into observations with a composite identity.

A provider row becomes a ``TradeObservation`` only when every identity-bearing field parses; the
parser never substitutes a default for a missing or malformed identity field. ``outcome`` and
``outcomeIndex`` are optional and repairable from cached market metadata. Every row receives
exactly one ``RowDisposition``; outcome resolution is an independent attribute. Invalid rows are
described only by the SHA-256 of the raw row and the failing field name, never by wallet data.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Container
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Literal, cast

from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent

IDENTITY_FIELDS: tuple[str, ...] = (
    "transactionHash",
    "proxyWallet",
    "conditionId",
    "asset",
    "side",
    "price",
    "size",
    "timestamp",
)
CLOCK_SKEW_ALLOWANCE_SECONDS = 60

Side = Literal["BUY", "SELL"]


class OutcomeResolution(StrEnum):
    """How an observation's outcome was resolved; independent of the row disposition."""

    PROVIDED = "provided"
    REPAIRED = "repaired"
    UNKNOWN = "unknown"


class RowDisposition(StrEnum):
    """The single disposition every parsed row receives."""

    EMITTED = "emitted"
    DUPLICATE = "duplicate"
    PADDING = "padding"
    ANCHOR_HISTORY = "anchor-history"
    DEFERRED_FUTURE_CYCLE = "deferred:future-cycle"
    INVALID_TRANSACTION_HASH = "invalid:transactionHash"
    INVALID_PROXY_WALLET = "invalid:proxyWallet"
    INVALID_CONDITION_ID = "invalid:conditionId"
    INVALID_ASSET = "invalid:asset"
    INVALID_SIDE = "invalid:side"
    INVALID_PRICE = "invalid:price"
    INVALID_SIZE = "invalid:size"
    INVALID_TIMESTAMP = "invalid:timestamp"
    INVALID_FUTURE_TIMESTAMP = "invalid:future-timestamp"
    INVALID_SCHEMA = "invalid:schema"


def zero_disposition_counts() -> dict[RowDisposition, int]:
    """Return a counter with every disposition present at zero."""
    return dict.fromkeys(RowDisposition, 0)


@dataclass(frozen=True)
class InvalidRow:
    """A wallet-free diagnostic for a row that failed strict parsing."""

    disposition: RowDisposition
    field: str
    row_hash: str


@dataclass(frozen=True)
class TradeObservation:
    """One valid provider row normalized for the pipeline, with its stable identity."""

    event: TradeEvent
    identity: str
    provider_timestamp: int
    outcome_resolution: OutcomeResolution

    @property
    def outcome_known(self) -> bool:
        return self.outcome_resolution is not OutcomeResolution.UNKNOWN

    @property
    def repaired(self) -> bool:
        return self.outcome_resolution is OutcomeResolution.REPAIRED


def canonical_decimal(value: Decimal) -> str:
    """Render a decimal without exponent or trailing zeros so ``0.50`` and ``0.5`` agree."""
    return format(value.normalize(), "f")


def row_hash(raw: object) -> str:
    """SHA-256 of the canonical JSON rendering of a raw row."""
    rendered = json.dumps(raw, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(rendered.encode()).hexdigest()


def _parse_text(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _parse_side(value: object) -> Side | None:
    if not isinstance(value, str):
        return None
    upper = value.upper()
    if upper == "BUY":
        return "BUY"
    if upper == "SELL":
        return "SELL"
    return None


def _parse_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _parse_timestamp(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and value.isdigit():
        return int(value)
    if isinstance(value, int) and value >= 0:
        return value
    return None


_IDENTITY_PARSERS: tuple[tuple[str, Callable[[object], object | None]], ...] = (
    ("transactionHash", _parse_text),
    ("proxyWallet", _parse_text),
    ("conditionId", _parse_text),
    ("asset", _parse_text),
    ("side", _parse_side),
    ("price", _parse_decimal),
    ("size", _parse_decimal),
    ("timestamp", _parse_timestamp),
)


@dataclass(frozen=True)
class _IdentityFields:
    transaction_hash: str
    wallet: str
    condition_id: str
    asset: str
    side: Side
    price: Decimal
    size: Decimal
    timestamp: int

    @classmethod
    def from_parsed(cls, parsed: dict[str, object]) -> _IdentityFields:
        return cls(
            transaction_hash=cast(str, parsed["transactionHash"]),
            wallet=cast(str, parsed["proxyWallet"]),
            condition_id=cast(str, parsed["conditionId"]),
            asset=cast(str, parsed["asset"]),
            side=cast(Side, parsed["side"]),
            price=cast(Decimal, parsed["price"]),
            size=cast(Decimal, parsed["size"]),
            timestamp=cast(int, parsed["timestamp"]),
        )


_FIELD_DISPOSITIONS: dict[str, RowDisposition] = {
    "transactionHash": RowDisposition.INVALID_TRANSACTION_HASH,
    "proxyWallet": RowDisposition.INVALID_PROXY_WALLET,
    "conditionId": RowDisposition.INVALID_CONDITION_ID,
    "asset": RowDisposition.INVALID_ASSET,
    "side": RowDisposition.INVALID_SIDE,
    "price": RowDisposition.INVALID_PRICE,
    "size": RowDisposition.INVALID_SIZE,
    "timestamp": RowDisposition.INVALID_TIMESTAMP,
}


def _invalid(raw: object, field: str, disposition: RowDisposition | None = None) -> InvalidRow:
    resolved = disposition or _FIELD_DISPOSITIONS[field]
    return InvalidRow(disposition=resolved, field=field, row_hash=row_hash(raw))


def _parse_identity_fields(raw: dict[str, Any]) -> _IdentityFields | InvalidRow:
    parsed: dict[str, object] = {}
    for name, parser in _IDENTITY_PARSERS:
        value = parser(raw.get(name))
        if value is None:
            return _invalid(raw, name)
        parsed[name] = value
    return _IdentityFields.from_parsed(parsed)


def _optional_text(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    return value if isinstance(value, str) else ""


def _optional_index(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _initial_resolution(outcome: str, outcome_index: int | None) -> OutcomeResolution:
    if outcome or outcome_index is not None:
        return OutcomeResolution.PROVIDED
    return OutcomeResolution.UNKNOWN


def observation_identity(fields: _IdentityFields, outcome_index: int | None) -> str:
    """SHA-256 of the canonical identity tuple defined by the data model."""
    parts = (
        fields.transaction_hash,
        fields.wallet.lower(),
        fields.condition_id,
        fields.asset,
        fields.side,
        "" if outcome_index is None else str(outcome_index),
        canonical_decimal(fields.price),
        canonical_decimal(fields.size),
        str(fields.timestamp),
    )
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _build_observation(raw: dict[str, Any], fields: _IdentityFields) -> TradeObservation:
    outcome = _optional_text(raw, "outcome")
    outcome_index = _optional_index(raw.get("outcomeIndex"))
    event = TradeEvent(
        market_id=fields.condition_id,
        trade_id=fields.transaction_hash,
        wallet_address=fields.wallet,
        side=fields.side,
        outcome=outcome,
        outcome_index=outcome_index if outcome_index is not None else 0,
        price=fields.price,
        size=fields.size,
        timestamp=datetime.fromtimestamp(fields.timestamp, tz=UTC),
        asset_id=fields.asset,
        market_slug=_optional_text(raw, "slug"),
        event_slug=_optional_text(raw, "eventSlug"),
        event_title=_optional_text(raw, "title"),
        trader_name=_optional_text(raw, "name"),
        trader_pseudonym=_optional_text(raw, "pseudonym"),
    )
    return TradeObservation(
        event=event,
        identity=observation_identity(fields, outcome_index),
        provider_timestamp=fields.timestamp,
        outcome_resolution=_initial_resolution(outcome, outcome_index),
    )


def parse_trade_row(raw: object, *, now: int) -> TradeObservation | InvalidRow:
    """Parse one raw provider row strictly; ``now`` is the local clock in epoch seconds."""
    if not isinstance(raw, dict):
        return InvalidRow(RowDisposition.INVALID_SCHEMA, "row", row_hash(raw))
    row = cast(dict[str, Any], raw)
    fields = _parse_identity_fields(row)
    if isinstance(fields, InvalidRow):
        return fields
    if fields.timestamp > now + CLOCK_SKEW_ALLOWANCE_SECONDS:
        return _invalid(row, "timestamp", RowDisposition.INVALID_FUTURE_TIMESTAMP)
    return _build_observation(row, fields)


def _token_outcome(metadata: MarketMetadata, asset_id: str) -> tuple[int, str] | None:
    for index, token in enumerate(metadata.tokens):
        if token.token_id == asset_id:
            return index, token.outcome
    return None


def repair_outcome(
    observation: TradeObservation, metadata: MarketMetadata | None
) -> TradeObservation:
    """Fill an unknown outcome from the cached token whose id equals the asset; never invent."""
    if observation.outcome_known or metadata is None:
        return observation
    match = _token_outcome(metadata, observation.event.asset_id)
    if match is None:
        return observation
    index, outcome = match
    return replace(
        observation,
        event=replace(observation.event, outcome=outcome, outcome_index=index),
        outcome_resolution=OutcomeResolution.REPAIRED,
    )


def classify_observation(
    observation: TradeObservation, *, cutoff: int, floor: int, retained: Container[str]
) -> RowDisposition:
    """Assign the one disposition of a valid row relative to a cycle cutoff and horizon floor."""
    timestamp = observation.provider_timestamp
    if timestamp > cutoff:
        return RowDisposition.DEFERRED_FUTURE_CYCLE
    if timestamp < floor:
        return RowDisposition.PADDING
    if observation.identity in retained:
        return RowDisposition.DUPLICATE
    return RowDisposition.EMITTED
