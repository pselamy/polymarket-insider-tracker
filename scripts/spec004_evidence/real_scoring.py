"""Evidence-grid scoring through the product detectors and scorer (T028, FR-022, FR-022a).

Every size signal comes from the real ``SizeAnomalyDetector.evaluate`` (classified volume state,
D3 (d) niche basis, niche-only minimum, 0.1 emission floor), every wallet signal from the real
``FreshWalletDetector.evaluate``, and every default pre-T027 score from ``size_group_score`` -- the
function ``RiskScorer.calculate_weighted_score`` and the runtime alert-path report
(``detector/alert_path.py``) both use. No scoring rule is copied here.

Two scenarios are not wired on main and are composed from the scorer's own primitives
(``DEFAULT_WEIGHTS``, ``quantize_score_value``, ``apply_multi_signal_bonus``): the post-T027 posture,
where the wallet group scores beside the size group (DECISION D1b, T015b), and its unknown-age rule
(confidence exactly ``UNKNOWN_AGE_CONFIDENCE``, proposed FR-008).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal

from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.scorer import (
    DEFAULT_WEIGHTS,
    SCORE_QUANTUM,
    apply_multi_signal_bonus,
    quantize_score_value,
    size_group_score,
)
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile

UNKNOWN_AGE_CONFIDENCE = 0.5  # proposed FR-008 (exactly); not wired on main before T015b
EVALUATED_AT = datetime(2026, 10, 19, tzinfo=UTC)
NICHE_PRONE_CATEGORY, OTHER_CATEGORY = "science", "politics"
MARKET_ID, WALLET_ADDRESS = "0xreachability", "0x" + "ab" * 20
SizeSignal = tuple[float, bool]


@dataclass(frozen=True)
class Wallet:
    name: str
    nonce: int
    age_hours: float | None  # None = unknown


@dataclass(frozen=True)
class Market:
    name: str
    volume: Decimal | None  # usable positive 24h volume, else None
    niche_prone_category: bool  # category in science/tech/finance/other


@dataclass(frozen=True)
class StaticMarket:
    """The one market every grid trade is on."""

    metadata: MarketMetadata

    async def get_market(self, condition_id: str, /) -> MarketMetadata | None:
        return self.metadata if condition_id == self.metadata.condition_id else None


@dataclass(frozen=True)
class StaticProfiler:
    """The one wallet profile every grid trade is from, reported for the requested address."""

    profile: WalletProfile

    async def analyze(self, address: str, /) -> WalletProfile:
        return replace(self.profile, address=address)


def grid_trade(notional: Decimal) -> TradeEvent:
    """A one-share BUY whose price is the notional, so ``notional_value`` is exact."""
    return TradeEvent(
        market_id=MARKET_ID,
        trade_id="0xgrid",
        wallet_address=WALLET_ADDRESS,
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=notional,
        size=Decimal(1),
        timestamp=EVALUATED_AT,
        asset_id="grid-asset",
    )


def grid_metadata(market: Market) -> MarketMetadata:
    category = NICHE_PRONE_CATEGORY if market.niche_prone_category else OTHER_CATEGORY
    return MarketMetadata(
        condition_id=MARKET_ID,
        question=market.name,
        description="",
        tokens=(),
        category=category,
        last_updated=EVALUATED_AT,
    )


def grid_profile(wallet: Wallet) -> WalletProfile:
    return WalletProfile(
        address=WALLET_ADDRESS,
        nonce=wallet.nonce,
        first_seen=None,
        age_hours=wallet.age_hours,
        is_fresh=False,
        total_tx_count=wallet.nonce,
        matic_balance=Decimal(0),
        usdc_balance=Decimal(0),
        analyzed_at=EVALUATED_AT,
    )


def size_signal(market: Market, notional: Decimal, *, volume_passed: bool) -> SizeSignal | None:
    """(confidence, is_niche) of the real detector, or None when it emits no size signal.

    ``volume_passed`` hands the usable volume to the detector as a caller assertion; otherwise the
    market carries no volume value, so the classified state is ``unavailable``.
    """
    detector = SizeAnomalyDetector(StaticMarket(grid_metadata(market)))
    volume = market.volume if volume_passed else None
    evaluation = asyncio.run(
        detector.evaluate(grid_trade(notional), evaluated_at=EVALUATED_AT, daily_volume=volume)
    )
    signal = evaluation.signal
    return None if signal is None else (signal.confidence, signal.is_niche_market)


def detector_wallet_confidence(wallet: Wallet, notional: Decimal) -> float | None:
    """The real fresh-wallet confidence as wired on main (003.1 rules), None without a signal."""
    detector = FreshWalletDetector(StaticProfiler(grid_profile(wallet)))
    evaluation = asyncio.run(detector.evaluate(grid_trade(notional), evaluated_at=EVALUATED_AT))
    signal = evaluation.signal
    return None if signal is None else signal.confidence


def wallet_confidence(wallet: Wallet, notional: Decimal, *, proposed: bool) -> float | None:
    """None = no wallet signal. ``proposed`` applies the 004 unknown-age rule to a real signal."""
    confidence = detector_wallet_confidence(wallet, notional)
    if confidence is None or not (proposed and wallet.age_hours is None):
        return confidence
    return UNKNOWN_AGE_CONFIDENCE


def weighted(confidence: float, name: str) -> Decimal:
    return quantize_score_value(confidence) * Decimal(str(DEFAULT_WEIGHTS[name]))


def size_terms(size: SizeSignal) -> Decimal:
    confidence, niche = size
    return weighted(confidence, "size_anomaly") + (
        weighted(confidence, "niche_market") if niche else Decimal(0)
    )


def two_group_score(wallet_conf: float, size: SizeSignal | None) -> Decimal:
    """Post-T027 scenario: the wallet group scores and counts toward the two-group bonus."""
    total = weighted(wallet_conf, "fresh_wallet")
    count = 1
    if size is not None:
        total += size_terms(size)
        count = 2
    total = apply_multi_signal_bonus(total, count)
    return min(total, Decimal(1)).quantize(SCORE_QUANTUM, rounding=ROUND_HALF_EVEN)


def size_only_score(size: SizeSignal | None) -> Decimal:
    """Default pre-T027 posture: the scorer's own size-group combination (FR-007c)."""
    if size is None:
        return Decimal(0).quantize(SCORE_QUANTUM)
    confidence, niche = size
    return quantize_score_value(size_group_score(DEFAULT_WEIGHTS, confidence, niche=niche)[0])


def score(wallet_conf: float | None, size: SizeSignal | None) -> Decimal:
    if wallet_conf is None:
        return size_only_score(size)
    return two_group_score(wallet_conf, size)
