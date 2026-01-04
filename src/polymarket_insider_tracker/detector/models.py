"""Data models for the detector module."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile


@dataclass(frozen=True)
class FreshWalletSignal:
    """Signal emitted when a fresh wallet makes a suspicious trade.

    This signal combines trade event data with wallet profile analysis
    to produce a confidence score indicating the likelihood of suspicious
    activity.

    Attributes:
        trade_event: The original trade event that triggered this signal.
        wallet_profile: Analyzed profile of the trader's wallet.
        confidence: Overall confidence score (0.0 to 1.0).
        factors: Individual factor scores contributing to confidence.
        timestamp: When this signal was generated.
    """

    trade_event: TradeEvent
    wallet_profile: WalletProfile
    confidence: float
    factors: dict[str, float]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def wallet_address(self) -> str:
        """Return the wallet address from the trade event."""
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        """Return the market ID from the trade event."""
        return self.trade_event.market_id

    @property
    def trade_size_usdc(self) -> Decimal:
        """Return the trade size in USDC (notional value)."""
        return self.trade_event.notional_value

    @property
    def is_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.7."""
        return self.confidence >= 0.7

    @property
    def is_very_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.85."""
        return self.confidence >= 0.85

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for Redis stream publishing."""
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "trade_size": str(self.trade_size_usdc),
            "trade_side": self.trade_event.side,
            "trade_price": str(self.trade_event.price),
            "wallet_nonce": self.wallet_profile.nonce,
            "wallet_age_hours": self.wallet_profile.age_hours,
            "wallet_is_fresh": self.wallet_profile.is_fresh,
            "confidence": self.confidence,
            "factors": self.factors,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class SizeAnomalySignal:
    """Signal emitted when a trade has unusually large position size.

    This signal is generated when a trade's size significantly impacts
    the market volume or order book depth, indicating potential informed
    trading activity.

    Attributes:
        trade_event: The original trade event that triggered this signal.
        market_metadata: Metadata about the market being traded.
        volume_impact: Trade size as fraction of 24h volume (0.0 if unknown).
        book_impact: Trade size as fraction of order book depth (0.0 if unknown).
        is_niche_market: Whether the market is considered niche/low-volume.
        confidence: Overall confidence score (0.0 to 1.0).
        factors: Individual factor scores contributing to confidence.
        timestamp: When this signal was generated.
    """

    trade_event: TradeEvent
    market_metadata: MarketMetadata
    volume_impact: float
    book_impact: float
    is_niche_market: bool
    confidence: float
    factors: dict[str, float]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def wallet_address(self) -> str:
        """Return the wallet address from the trade event."""
        return self.trade_event.wallet_address

    @property
    def market_id(self) -> str:
        """Return the market ID from the trade event."""
        return self.trade_event.market_id

    @property
    def trade_size_usdc(self) -> Decimal:
        """Return the trade size in USDC (notional value)."""
        return self.trade_event.notional_value

    @property
    def is_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.7."""
        return self.confidence >= 0.7

    @property
    def is_very_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.85."""
        return self.confidence >= 0.85

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for Redis stream publishing."""
        return {
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "trade_size": str(self.trade_size_usdc),
            "trade_side": self.trade_event.side,
            "trade_price": str(self.trade_event.price),
            "market_category": self.market_metadata.category,
            "volume_impact": self.volume_impact,
            "book_impact": self.book_impact,
            "is_niche_market": self.is_niche_market,
            "confidence": self.confidence,
            "factors": self.factors,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class SniperClusterSignal:
    """Signal emitted when a wallet is identified as part of a sniper cluster.

    Sniper clusters are groups of wallets that consistently enter markets
    within minutes of their creation, suggesting coordinated insider activity.

    Attributes:
        wallet_address: The wallet identified as a sniper.
        cluster_id: Unique identifier for this cluster.
        cluster_size: Number of wallets in the cluster.
        avg_entry_delta_seconds: Average time (seconds) from market creation to entry.
        markets_in_common: Number of markets where cluster members overlap.
        confidence: Confidence score (0.0 to 1.0) based on clustering strength.
        timestamp: When this signal was generated.
    """

    wallet_address: str
    cluster_id: str
    cluster_size: int
    avg_entry_delta_seconds: float
    markets_in_common: int
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.7."""
        return self.confidence >= 0.7

    @property
    def is_very_high_confidence(self) -> bool:
        """Return True if confidence exceeds 0.85."""
        return self.confidence >= 0.85

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for Redis stream publishing."""
        return {
            "wallet_address": self.wallet_address,
            "cluster_id": self.cluster_id,
            "cluster_size": self.cluster_size,
            "avg_entry_delta_seconds": self.avg_entry_delta_seconds,
            "markets_in_common": self.markets_in_common,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class RiskAssessment:
    """Combined risk assessment aggregating all signal types.

    This represents the final scoring output that determines whether
    a trade should trigger an alert, combining signals from multiple
    detectors with configurable weights.

    Attributes:
        trade_event: The original trade event being assessed.
        wallet_address: The trader's wallet address.
        market_id: The market condition ID.
        fresh_wallet_signal: Signal from fresh wallet detector, if triggered.
        size_anomaly_signal: Signal from size anomaly detector, if triggered.
        signals_triggered: Count of how many signal types fired.
        weighted_score: Final weighted combination of all signals (0.0 to 1.0).
        should_alert: Whether this assessment meets alert threshold.
        assessment_id: Unique identifier for this assessment.
        timestamp: When this assessment was generated.
    """

    trade_event: TradeEvent
    wallet_address: str
    market_id: str

    # Individual signals (None if not triggered)
    fresh_wallet_signal: FreshWalletSignal | None
    size_anomaly_signal: SizeAnomalySignal | None

    # Combined scoring
    signals_triggered: int
    weighted_score: float
    should_alert: bool

    # Metadata
    assessment_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_high_risk(self) -> bool:
        """Return True if weighted score exceeds 0.7."""
        return self.weighted_score >= 0.7

    @property
    def is_very_high_risk(self) -> bool:
        """Return True if weighted score exceeds 0.85."""
        return self.weighted_score >= 0.85

    @property
    def trade_size_usdc(self) -> Decimal:
        """Return the trade size in USDC (notional value)."""
        return self.trade_event.notional_value

    def to_dict(self) -> dict[str, object]:
        """Serialize to dictionary for Redis stream publishing."""
        return {
            "assessment_id": self.assessment_id,
            "wallet_address": self.wallet_address,
            "market_id": self.market_id,
            "trade_id": self.trade_event.trade_id,
            "trade_size": str(self.trade_size_usdc),
            "trade_side": self.trade_event.side,
            "trade_price": str(self.trade_event.price),
            "signals_triggered": self.signals_triggered,
            "weighted_score": self.weighted_score,
            "should_alert": self.should_alert,
            "has_fresh_wallet_signal": self.fresh_wallet_signal is not None,
            "has_size_anomaly_signal": self.size_anomaly_signal is not None,
            "fresh_wallet_confidence": (
                self.fresh_wallet_signal.confidence if self.fresh_wallet_signal else None
            ),
            "size_anomaly_confidence": (
                self.size_anomaly_signal.confidence if self.size_anomaly_signal else None
            ),
            "timestamp": self.timestamp.isoformat(),
        }
