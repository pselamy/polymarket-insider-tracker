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
