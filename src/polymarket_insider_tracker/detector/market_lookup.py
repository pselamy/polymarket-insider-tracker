"""The size detector's market lookup: real metadata, or a placeholder when the lookup fails.

``category_source`` records which one was used (``metadata`` or ``minimal``); a placeholder
category is not evidence (data-model section 3, DECISION D3).
"""

import logging
from typing import Protocol

from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.redaction import redact_exception_message

logger = logging.getLogger(__name__)


class MarketMetadataSource(Protocol):
    """Market lookup the detector needs (satisfied structurally by ``MarketMetadataSync``)."""

    async def get_market(self, condition_id: str, /) -> MarketMetadata | None:
        """Look up the metadata of ``condition_id``; ``None`` when the market is unknown."""


async def fetch_market(
    source: MarketMetadataSource, trade: TradeEvent
) -> tuple[MarketMetadata, str]:
    """The market metadata and its category source (``metadata`` or ``minimal``)."""
    try:
        metadata = await source.get_market(trade.market_id)
        if metadata is None:
            logger.warning(
                "No metadata found for market %s, creating minimal metadata",
                trade.market_id,
            )
            return minimal_metadata(trade), "minimal"
        return metadata, "metadata"
    except Exception as e:
        logger.warning(
            "Failed to get metadata for market %s: %s",
            trade.market_id,
            redact_exception_message(e),
        )
        return minimal_metadata(trade), "minimal"


def minimal_metadata(trade: TradeEvent) -> MarketMetadata:
    """Create minimal metadata from trade event."""
    return MarketMetadata(
        condition_id=trade.market_id,
        question=trade.event_title or "Unknown Market",
        description="",
        tokens=(
            Token(
                token_id=trade.asset_id,
                outcome=trade.outcome,
                price=trade.price,
            ),
        ),
        category="other",
    )
