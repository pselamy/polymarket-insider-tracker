"""Repository pattern implementations for data access.

This module provides clean data access abstractions for wallet profiles,
funding transfers, and wallet relationships.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from polymarket_insider_tracker.storage.models import (
    FundingTransferModel,
    WalletProfileModel,
    WalletRelationshipModel,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


@dataclass
class WalletProfileDTO:
    """Data transfer object for wallet profiles."""

    address: str
    nonce: int
    first_seen_at: datetime | None
    is_fresh: bool
    matic_balance: Decimal | None
    usdc_balance: Decimal | None
    analyzed_at: datetime
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: WalletProfileModel) -> WalletProfileDTO:
        """Create DTO from SQLAlchemy model."""
        return cls(
            address=model.address,
            nonce=model.nonce,
            first_seen_at=model.first_seen_at,
            is_fresh=model.is_fresh,
            matic_balance=model.matic_balance,
            usdc_balance=model.usdc_balance,
            analyzed_at=model.analyzed_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@dataclass
class FundingTransferDTO:
    """Data transfer object for funding transfers."""

    from_address: str
    to_address: str
    amount: Decimal
    token: str
    tx_hash: str
    block_number: int
    timestamp: datetime
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: FundingTransferModel) -> FundingTransferDTO:
        """Create DTO from SQLAlchemy model."""
        return cls(
            from_address=model.from_address,
            to_address=model.to_address,
            amount=model.amount,
            token=model.token,
            tx_hash=model.tx_hash,
            block_number=model.block_number,
            timestamp=model.timestamp,
            created_at=model.created_at,
        )


@dataclass
class WalletRelationshipDTO:
    """Data transfer object for wallet relationships."""

    wallet_a: str
    wallet_b: str
    relationship_type: str
    confidence: Decimal
    created_at: datetime | None = None

    @classmethod
    def from_model(cls, model: WalletRelationshipModel) -> WalletRelationshipDTO:
        """Create DTO from SQLAlchemy model."""
        return cls(
            wallet_a=model.wallet_a,
            wallet_b=model.wallet_b,
            relationship_type=model.relationship_type,
            confidence=model.confidence,
            created_at=model.created_at,
        )


class WalletRepository:
    """Repository for wallet profile data access.

    Provides CRUD operations for wallet profiles with async support.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy async session.
        """
        self.session = session

    async def get_by_address(self, address: str) -> WalletProfileDTO | None:
        """Get wallet profile by address.

        Args:
            address: Wallet address (lowercase).

        Returns:
            WalletProfileDTO if found, None otherwise.
        """
        result = await self.session.execute(
            select(WalletProfileModel).where(WalletProfileModel.address == address.lower())
        )
        model = result.scalar_one_or_none()
        return WalletProfileDTO.from_model(model) if model else None

    async def get_many(self, addresses: list[str]) -> list[WalletProfileDTO]:
        """Get multiple wallet profiles by addresses.

        Args:
            addresses: List of wallet addresses.

        Returns:
            List of WalletProfileDTOs for found addresses.
        """
        normalized = [addr.lower() for addr in addresses]
        result = await self.session.execute(
            select(WalletProfileModel).where(WalletProfileModel.address.in_(normalized))
        )
        return [WalletProfileDTO.from_model(m) for m in result.scalars().all()]

    async def get_fresh_wallets(self, limit: int = 100) -> list[WalletProfileDTO]:
        """Get recent fresh wallets.

        Args:
            limit: Maximum number of results.

        Returns:
            List of WalletProfileDTOs marked as fresh.
        """
        result = await self.session.execute(
            select(WalletProfileModel)
            .where(WalletProfileModel.is_fresh.is_(True))
            .order_by(WalletProfileModel.analyzed_at.desc())
            .limit(limit)
        )
        return [WalletProfileDTO.from_model(m) for m in result.scalars().all()]

    async def upsert(self, dto: WalletProfileDTO) -> WalletProfileDTO:
        """Insert or update wallet profile.

        Args:
            dto: Wallet profile data.

        Returns:
            Updated WalletProfileDTO.
        """
        now = datetime.now(UTC)
        values = {
            "address": dto.address.lower(),
            "nonce": dto.nonce,
            "first_seen_at": dto.first_seen_at,
            "is_fresh": dto.is_fresh,
            "matic_balance": dto.matic_balance,
            "usdc_balance": dto.usdc_balance,
            "analyzed_at": dto.analyzed_at,
            "updated_at": now,
        }

        # Try PostgreSQL upsert first, fall back to SQLite for testing
        try:
            stmt = pg_insert(WalletProfileModel).values(**values, created_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["address"],
                set_={
                    "nonce": stmt.excluded.nonce,
                    "first_seen_at": stmt.excluded.first_seen_at,
                    "is_fresh": stmt.excluded.is_fresh,
                    "matic_balance": stmt.excluded.matic_balance,
                    "usdc_balance": stmt.excluded.usdc_balance,
                    "analyzed_at": stmt.excluded.analyzed_at,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.session.execute(stmt)
        except Exception:
            # Fall back to SQLite upsert for testing
            stmt = sqlite_insert(WalletProfileModel).values(**values, created_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["address"],
                set_={
                    "nonce": stmt.excluded.nonce,
                    "first_seen_at": stmt.excluded.first_seen_at,
                    "is_fresh": stmt.excluded.is_fresh,
                    "matic_balance": stmt.excluded.matic_balance,
                    "usdc_balance": stmt.excluded.usdc_balance,
                    "analyzed_at": stmt.excluded.analyzed_at,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.session.execute(stmt)

        await self.session.flush()
        return dto

    async def delete(self, address: str) -> bool:
        """Delete wallet profile by address.

        Args:
            address: Wallet address.

        Returns:
            True if deleted, False if not found.
        """
        result = await self.session.execute(
            delete(WalletProfileModel).where(WalletProfileModel.address == address.lower())
        )
        return result.rowcount > 0

    async def mark_stale(self, address: str) -> bool:
        """Mark a wallet profile as stale (soft delete).

        Sets analyzed_at to a very old date to trigger re-analysis.

        Args:
            address: Wallet address.

        Returns:
            True if updated, False if not found.
        """
        stale_time = datetime(2000, 1, 1, tzinfo=UTC)
        result = await self.session.execute(
            update(WalletProfileModel)
            .where(WalletProfileModel.address == address.lower())
            .values(analyzed_at=stale_time, updated_at=datetime.now(UTC))
        )
        return result.rowcount > 0


class FundingRepository:
    """Repository for funding transfer data access.

    Provides CRUD operations for funding transfers with async support.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy async session.
        """
        self.session = session

    async def get_transfers_to(self, address: str, limit: int = 100) -> list[FundingTransferDTO]:
        """Get transfers to a wallet address.

        Args:
            address: Destination wallet address.
            limit: Maximum number of results.

        Returns:
            List of FundingTransferDTOs ordered by timestamp.
        """
        result = await self.session.execute(
            select(FundingTransferModel)
            .where(FundingTransferModel.to_address == address.lower())
            .order_by(FundingTransferModel.timestamp.asc())
            .limit(limit)
        )
        return [FundingTransferDTO.from_model(m) for m in result.scalars().all()]

    async def get_transfers_from(self, address: str, limit: int = 100) -> list[FundingTransferDTO]:
        """Get transfers from a wallet address.

        Args:
            address: Source wallet address.
            limit: Maximum number of results.

        Returns:
            List of FundingTransferDTOs ordered by timestamp.
        """
        result = await self.session.execute(
            select(FundingTransferModel)
            .where(FundingTransferModel.from_address == address.lower())
            .order_by(FundingTransferModel.timestamp.asc())
            .limit(limit)
        )
        return [FundingTransferDTO.from_model(m) for m in result.scalars().all()]

    async def get_first_transfer_to(self, address: str) -> FundingTransferDTO | None:
        """Get the first transfer to a wallet.

        Args:
            address: Wallet address.

        Returns:
            First FundingTransferDTO if found, None otherwise.
        """
        result = await self.session.execute(
            select(FundingTransferModel)
            .where(FundingTransferModel.to_address == address.lower())
            .order_by(FundingTransferModel.timestamp.asc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        return FundingTransferDTO.from_model(model) if model else None

    async def get_by_tx_hash(self, tx_hash: str) -> FundingTransferDTO | None:
        """Get transfer by transaction hash.

        Args:
            tx_hash: Transaction hash.

        Returns:
            FundingTransferDTO if found, None otherwise.
        """
        result = await self.session.execute(
            select(FundingTransferModel).where(FundingTransferModel.tx_hash == tx_hash.lower())
        )
        model = result.scalar_one_or_none()
        return FundingTransferDTO.from_model(model) if model else None

    async def insert(self, dto: FundingTransferDTO) -> FundingTransferDTO:
        """Insert a new funding transfer.

        Args:
            dto: Funding transfer data.

        Returns:
            Inserted FundingTransferDTO.

        Raises:
            IntegrityError if tx_hash already exists.
        """
        model = FundingTransferModel(
            from_address=dto.from_address.lower(),
            to_address=dto.to_address.lower(),
            amount=dto.amount,
            token=dto.token,
            tx_hash=dto.tx_hash.lower(),
            block_number=dto.block_number,
            timestamp=dto.timestamp,
        )
        self.session.add(model)
        await self.session.flush()
        return dto

    async def insert_many(self, dtos: list[FundingTransferDTO]) -> int:
        """Insert multiple funding transfers.

        Skips duplicates silently.

        Args:
            dtos: List of funding transfer data.

        Returns:
            Number of transfers inserted.
        """
        inserted = 0
        for dto in dtos:
            try:
                await self.insert(dto)
                inserted += 1
            except Exception as e:
                # Skip duplicates
                if "UNIQUE constraint" in str(e) or "duplicate key" in str(e).lower():
                    continue
                raise
        return inserted


class RelationshipRepository:
    """Repository for wallet relationship data access.

    Provides CRUD operations for wallet relationships with async support.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy async session.
        """
        self.session = session

    async def get_relationships(
        self, wallet: str, relationship_type: str | None = None
    ) -> list[WalletRelationshipDTO]:
        """Get relationships for a wallet.

        Args:
            wallet: Wallet address.
            relationship_type: Optional filter by type.

        Returns:
            List of WalletRelationshipDTOs.
        """
        stmt = select(WalletRelationshipModel).where(
            (WalletRelationshipModel.wallet_a == wallet.lower())
            | (WalletRelationshipModel.wallet_b == wallet.lower())
        )
        if relationship_type:
            stmt = stmt.where(WalletRelationshipModel.relationship_type == relationship_type)

        result = await self.session.execute(stmt)
        return [WalletRelationshipDTO.from_model(m) for m in result.scalars().all()]

    async def get_related_wallets(
        self, wallet: str, relationship_type: str | None = None
    ) -> list[str]:
        """Get addresses of related wallets.

        Args:
            wallet: Wallet address.
            relationship_type: Optional filter by type.

        Returns:
            List of related wallet addresses.
        """
        relationships = await self.get_relationships(wallet, relationship_type)
        related = set()
        normalized = wallet.lower()
        for rel in relationships:
            if rel.wallet_a == normalized:
                related.add(rel.wallet_b)
            else:
                related.add(rel.wallet_a)
        return list(related)

    async def upsert(self, dto: WalletRelationshipDTO) -> WalletRelationshipDTO:
        """Insert or update wallet relationship.

        Args:
            dto: Wallet relationship data.

        Returns:
            Updated WalletRelationshipDTO.
        """
        now = datetime.now(UTC)
        values = {
            "wallet_a": dto.wallet_a.lower(),
            "wallet_b": dto.wallet_b.lower(),
            "relationship_type": dto.relationship_type,
            "confidence": dto.confidence,
            "created_at": now,
        }

        # Try PostgreSQL upsert first, fall back to SQLite for testing
        try:
            stmt = pg_insert(WalletRelationshipModel).values(**values)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_wallet_relationship",
                set_={"confidence": stmt.excluded.confidence},
            )
            await self.session.execute(stmt)
        except Exception:
            # Fall back to SQLite upsert for testing
            stmt = sqlite_insert(WalletRelationshipModel).values(**values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["wallet_a", "wallet_b", "relationship_type"],
                set_={"confidence": stmt.excluded.confidence},
            )
            await self.session.execute(stmt)

        await self.session.flush()
        return dto

    async def delete(self, wallet_a: str, wallet_b: str, relationship_type: str) -> bool:
        """Delete a specific relationship.

        Args:
            wallet_a: First wallet address.
            wallet_b: Second wallet address.
            relationship_type: Type of relationship.

        Returns:
            True if deleted, False if not found.
        """
        result = await self.session.execute(
            delete(WalletRelationshipModel).where(
                WalletRelationshipModel.wallet_a == wallet_a.lower(),
                WalletRelationshipModel.wallet_b == wallet_b.lower(),
                WalletRelationshipModel.relationship_type == relationship_type,
            )
        )
        return result.rowcount > 0
