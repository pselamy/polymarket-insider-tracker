"""SQLAlchemy models for persistent storage.

This module defines the database schema for storing wallet profiles,
funding transfers, and wallet relationships.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

if TYPE_CHECKING:
    pass


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    pass


class WalletProfileModel(Base):
    """SQLAlchemy model for wallet profiles.

    Stores analyzed wallet information including age, transaction count,
    balances, and freshness classification.
    """

    __tablename__ = "wallet_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(String(42), unique=True, nullable=False)
    nonce: Mapped[int] = mapped_column(Integer, nullable=False)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_fresh: Mapped[bool] = mapped_column(Boolean, nullable=False)
    matic_balance: Mapped[Decimal | None] = mapped_column(Numeric(30, 0), nullable=True)
    usdc_balance: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    analyzed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (Index("idx_wallet_profiles_address", "address"),)


class FundingTransferModel(Base):
    """SQLAlchemy model for funding transfers.

    Stores ERC20 transfer events to track wallet funding sources.
    """

    __tablename__ = "funding_transfers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    from_address: Mapped[str] = mapped_column(String(42), nullable=False)
    to_address: Mapped[str] = mapped_column(String(42), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(30, 6), nullable=False)
    token: Mapped[str] = mapped_column(String(10), nullable=False)
    tx_hash: Mapped[str] = mapped_column(String(66), unique=True, nullable=False)
    block_number: Mapped[int] = mapped_column(Integer, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_funding_transfers_to", "to_address"),
        Index("idx_funding_transfers_from", "from_address"),
        Index("idx_funding_transfers_block", "block_number"),
    )


class WalletRelationshipModel(Base):
    """SQLAlchemy model for wallet relationships.

    Stores graph edges between wallets representing funding relationships
    or entity linkages.
    """

    __tablename__ = "wallet_relationships"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wallet_a: Mapped[str] = mapped_column(String(42), nullable=False)
    wallet_b: Mapped[str] = mapped_column(String(42), nullable=False)
    relationship_type: Mapped[str] = mapped_column(String(20), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "wallet_a", "wallet_b", "relationship_type", name="uq_wallet_relationship"
        ),
        Index("idx_wallet_relationships_a", "wallet_a"),
        Index("idx_wallet_relationships_b", "wallet_b"),
    )


class RiskAssessmentModel(Base):
    """SQLAlchemy model for risk assessments.

    One row per signal-bearing trade (i.e. trades that triggered at least one
    detector). Captures everything a future backtest needs without going back
    to the public API: trade identity, score, per-signal confidences, and
    whether the alert was actually delivered (could be False due to dedup or
    threshold).
    """

    __tablename__ = "risk_assessments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    assessment_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)

    # Trade identity
    trade_id: Mapped[str] = mapped_column(String(80), nullable=False)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False)
    market_id: Mapped[str] = mapped_column(String(80), nullable=False)
    asset_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    outcome: Mapped[str | None] = mapped_column(String(120), nullable=True)
    outcome_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    notional_usdc: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    trade_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Scoring
    weighted_score: Mapped[Decimal] = mapped_column(Numeric(4, 3), nullable=False)
    signals_triggered: Mapped[int] = mapped_column(Integer, nullable=False)
    fresh_wallet_confidence: Mapped[Decimal | None] = mapped_column(Numeric(4, 3), nullable=True)
    size_anomaly_confidence: Mapped[Decimal | None] = mapped_column(Numeric(4, 3), nullable=True)
    tail_bet_confidence: Mapped[Decimal | None] = mapped_column(Numeric(4, 3), nullable=True)
    is_niche_market: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    volume_impact: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    book_impact: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    wallet_age_hours: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    potential_payout_usdc: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    payout_to_volume_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    payout_to_notional_ratio: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)

    # Decision
    should_alert: Mapped[bool] = mapped_column(Boolean, nullable=False)
    threshold_at_eval: Mapped[Decimal] = mapped_column(Numeric(4, 3), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("idx_risk_assessments_wallet", "wallet_address"),
        Index("idx_risk_assessments_market", "market_id"),
        Index("idx_risk_assessments_trade_ts", "trade_timestamp"),
        Index("idx_risk_assessments_score", "weighted_score"),
    )
