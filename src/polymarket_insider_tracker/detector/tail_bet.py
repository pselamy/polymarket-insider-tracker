"""Tail-bet anomaly detection.

A "tail bet" is a low-price BUY (or near-1.0 SELL of the opposite outcome)
that controls a large potential payout for a small notional. Structurally:

    size >> notional
    potential_payout = size * (1 - price)   # for BUY at price P
    leverage = 1 / price - 1                # multiple of capital risked

The original `SizeAnomalyDetector` measures *capital risk* (notional/volume),
which makes settlement-arbitrage trades (BUY at 0.999 for $20 of upside on
$20,000 notional) score high while genuine tail bets (BUY at 0.005 for $1k
notional but $200k of upside) score zero.

This detector inverts the lens: it scores by *potential payout* relative to
market depth, not capital. It is deliberately exclusive — settlement-arb
trades (price > `max_price`) are ignored, not just down-weighted, because
their information value is structurally zero.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from polymarket_insider_tracker.detector.models import TailBetSignal
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent

logger = logging.getLogger(__name__)

# A trade only qualifies as a tail bet if the BUY price is below this. Above
# this and the asymmetry collapses — at price=0.5 the payout is just 1x the
# notional, which is no longer "asymmetric upside". 0.10 keeps the focus on
# 10x+ leverage trades.
DEFAULT_MAX_PRICE = Decimal("0.10")

# Floor for potential payout in USDC. Below this, even a perfect prediction
# only nets a few hundred dollars and isn't worth the noise. Tuned to filter
# out hobbyist longshots while keeping every meaningful insider candidate.
DEFAULT_MIN_PAYOUT_USDC = Decimal("1000")

# Reference scale for `payout_to_volume_ratio` confidence. A bet whose payout
# would consume 5% of the market's 24h volume is a strong signal; we map that
# to confidence 1.0 and clamp.
DEFAULT_PAYOUT_TO_VOLUME_REF = 0.05

# When the market is niche (volume below threshold), confidence is multiplied
# by this factor — same convention as SizeAnomalyDetector for consistency.
NICHE_MULTIPLIER = 1.5

# When daily volume is unknown, fall back to a flat baseline confidence so the
# signal isn't silently dropped on cold-start markets. Still scaled by price
# extremity below.
DEFAULT_VOLUME_UNKNOWN_BASELINE = 0.4


class TailBetDetector:
    """Detector for asymmetric low-price BUY trades on prediction markets.

    Excludes the settlement-arbitrage shape (price ~ 1.0) by construction;
    only fires when:

      1. side == BUY
      2. price <= max_price (default 0.10)
      3. potential_payout >= min_payout_usdc (default $1,000)

    Confidence is driven by:
      - payout_to_volume_ratio relative to PAYOUT_TO_VOLUME_REF
      - price extremity (lower price -> higher score, since 1/price - 1
        leverage rises hyperbolically as price approaches 0)
      - niche-market multiplier (1.5x) when volume is below threshold
    """

    def __init__(
        self,
        metadata_sync: MarketMetadataSync,
        *,
        max_price: Decimal = DEFAULT_MAX_PRICE,
        min_payout_usdc: Decimal = DEFAULT_MIN_PAYOUT_USDC,
        payout_to_volume_ref: float = DEFAULT_PAYOUT_TO_VOLUME_REF,
        niche_volume_threshold: Decimal = Decimal("50000"),
    ) -> None:
        self._metadata_sync = metadata_sync
        self._max_price = max_price
        self._min_payout_usdc = min_payout_usdc
        self._payout_to_volume_ref = payout_to_volume_ref
        self._niche_volume_threshold = niche_volume_threshold

    async def analyze(
        self,
        trade: TradeEvent,
        *,
        daily_volume: Decimal | None = None,
    ) -> TailBetSignal | None:
        """Analyze a trade for tail-bet shape.

        Returns None if the trade does not qualify (wrong side, price too
        high, payout too small). Returns a TailBetSignal otherwise.
        """
        # Hard structural filters first — these are not "down-weight" cases,
        # they are "not a tail bet by definition" cases.
        if trade.side != "BUY":
            return None
        if trade.price <= 0 or trade.price > self._max_price:
            return None

        potential_payout = trade.size * (Decimal("1") - trade.price)
        if potential_payout < self._min_payout_usdc:
            return None

        # Get metadata for niche/category info; never let metadata failure
        # drop a signal — fall back to a minimal record like SizeAnomaly does.
        try:
            metadata = await self._metadata_sync.get_market(trade.market_id)
            if metadata is None:
                metadata = self._minimal_metadata(trade)
        except Exception as e:
            logger.warning(
                "Tail-bet metadata fetch failed for %s: %s", trade.market_id, e
            )
            metadata = self._minimal_metadata(trade)

        is_niche = self._is_niche(daily_volume)

        payout_to_volume = self._payout_to_volume(potential_payout, daily_volume)
        # leverage = upside / capital_risked; equals 1/price - 1 for a BUY.
        # We use the realized version (payout / notional) so it stays correct
        # even if upstream rounds price to fewer decimals.
        notional = trade.notional_value
        payout_to_notional = (
            float(potential_payout / notional) if notional > 0 else 0.0
        )

        confidence, factors = self._score(
            price=trade.price,
            payout_to_volume_ratio=payout_to_volume,
            volume_known=daily_volume is not None and daily_volume > 0,
            is_niche=is_niche,
        )

        if confidence < 0.1:
            return None

        logger.info(
            "Tail-bet signal: market=%s, price=%s, size=%s, payout=%s, "
            "payout/vol=%.4f, niche=%s, confidence=%.2f",
            trade.market_id[:10] + "...",
            trade.price,
            trade.size,
            potential_payout,
            payout_to_volume,
            is_niche,
            confidence,
        )

        return TailBetSignal(
            trade_event=trade,
            potential_payout_usdc=potential_payout,
            payout_to_volume_ratio=payout_to_volume,
            payout_to_notional_ratio=payout_to_notional,
            is_niche_market=is_niche,
            confidence=confidence,
            factors=factors,
        )

    def _minimal_metadata(self, trade: TradeEvent) -> MarketMetadata:
        from polymarket_insider_tracker.ingestor.models import Token

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

    def _payout_to_volume(
        self, potential_payout: Decimal, daily_volume: Decimal | None
    ) -> float:
        if daily_volume is None or daily_volume <= 0:
            return 0.0
        return float(potential_payout / daily_volume)

    def _is_niche(self, daily_volume: Decimal | None) -> bool:
        if daily_volume is None:
            # Don't claim niche on unknown — be conservative; the volume-
            # baseline path covers cold-start markets separately.
            return False
        return daily_volume < self._niche_volume_threshold

    def _score(
        self,
        *,
        price: Decimal,
        payout_to_volume_ratio: float,
        volume_known: bool,
        is_niche: bool,
    ) -> tuple[float, dict[str, float]]:
        """Compute confidence in [0, 1] from price extremity and impact.

        Two main components, both clamped:
          1. Price extremity: how deep into the tail the BUY is. Linear from
             0 at max_price down to 1.0 at price=0. This is the structural
             "how asymmetric is this bet" axis.
          2. Payout-to-volume impact: how much of the market would get paid
             if it hits. Mapped to 1.0 at PAYOUT_TO_VOLUME_REF (5%) and
             clamped above. Falls back to a flat baseline when volume is
             unknown so cold markets aren't silently dropped.

        Final = 0.5 * extremity + 0.5 * impact, then 1.5x if niche, clamped.
        """
        factors: dict[str, float] = {}

        # Component 1: price extremity. price <= max_price was checked upstream
        # so this is in [0, 1] without clamping if max_price > 0.
        extremity = float(
            max(Decimal("0"), Decimal("1") - price / self._max_price)
        )
        factors["price_extremity"] = extremity

        # Component 2: payout-to-volume impact (or unknown-volume baseline).
        if volume_known:
            impact = min(payout_to_volume_ratio / self._payout_to_volume_ref, 1.0)
            factors["payout_to_volume"] = impact
        else:
            impact = DEFAULT_VOLUME_UNKNOWN_BASELINE
            factors["volume_unknown_baseline"] = impact

        confidence = 0.5 * extremity + 0.5 * impact

        if is_niche:
            factors["niche_multiplier"] = NICHE_MULTIPLIER
            confidence *= NICHE_MULTIPLIER

        confidence = max(0.0, min(1.0, confidence))
        return confidence, factors
