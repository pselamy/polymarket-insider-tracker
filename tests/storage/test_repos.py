"""Tests for storage repositories."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from polymarket_insider_tracker.storage.models import Base
from polymarket_insider_tracker.storage.repos import (
    FundingRepository,
    FundingTransferDTO,
    RelationshipRepository,
    WalletProfileDTO,
    WalletRelationshipDTO,
    WalletRepository,
)

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
async def async_engine():
    """Create an async SQLite engine for testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def async_session(async_engine) -> AsyncSession:
    """Create an async session for testing."""
    session_factory = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest.fixture
def sample_wallet_dto() -> WalletProfileDTO:
    """Create a sample wallet profile DTO."""
    return WalletProfileDTO(
        address="0x1234567890abcdef1234567890abcdef12345678",
        nonce=5,
        first_seen_at=datetime.now(UTC) - timedelta(hours=24),
        is_fresh=True,
        matic_balance=Decimal("1000000000000000000"),
        usdc_balance=Decimal("1000.00"),
        analyzed_at=datetime.now(UTC),
    )


@pytest.fixture
def sample_transfer_dto() -> FundingTransferDTO:
    """Create a sample funding transfer DTO."""
    return FundingTransferDTO(
        from_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        to_address="0x1234567890abcdef1234567890abcdef12345678",
        amount=Decimal("5000.00"),
        token="USDC",
        tx_hash="0x" + "a" * 64,
        block_number=12345678,
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_relationship_dto() -> WalletRelationshipDTO:
    """Create a sample wallet relationship DTO."""
    return WalletRelationshipDTO(
        wallet_a="0x1234567890abcdef1234567890abcdef12345678",
        wallet_b="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        relationship_type="funded_by",
        confidence=Decimal("0.95"),
    )


# ============================================================================
# WalletRepository Tests
# ============================================================================


class TestWalletRepository:
    """Tests for WalletRepository."""

    @pytest.mark.asyncio
    async def test_get_by_address_not_found(self, async_session: AsyncSession) -> None:
        """Test getting a non-existent wallet returns None."""
        repo = WalletRepository(async_session)
        result = await repo.get_by_address("0xnonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_upsert_creates_new(
        self, async_session: AsyncSession, sample_wallet_dto: WalletProfileDTO
    ) -> None:
        """Test upserting a new wallet profile."""
        repo = WalletRepository(async_session)
        await repo.upsert(sample_wallet_dto)
        await async_session.commit()

        result = await repo.get_by_address(sample_wallet_dto.address)
        assert result is not None
        assert result.address == sample_wallet_dto.address.lower()
        assert result.nonce == sample_wallet_dto.nonce
        assert result.is_fresh == sample_wallet_dto.is_fresh

    @pytest.mark.asyncio
    async def test_upsert_updates_existing(
        self, async_session: AsyncSession, sample_wallet_dto: WalletProfileDTO
    ) -> None:
        """Test upserting updates existing profile."""
        repo = WalletRepository(async_session)
        await repo.upsert(sample_wallet_dto)
        await async_session.commit()

        # Update the DTO
        updated_dto = WalletProfileDTO(
            address=sample_wallet_dto.address,
            nonce=10,
            first_seen_at=sample_wallet_dto.first_seen_at,
            is_fresh=False,
            matic_balance=sample_wallet_dto.matic_balance,
            usdc_balance=Decimal("2000.00"),
            analyzed_at=datetime.now(UTC),
        )
        await repo.upsert(updated_dto)
        await async_session.commit()

        result = await repo.get_by_address(sample_wallet_dto.address)
        assert result is not None
        assert result.nonce == 10
        assert result.is_fresh is False
        assert result.usdc_balance == Decimal("2000.00")

    @pytest.mark.asyncio
    async def test_get_many(
        self, async_session: AsyncSession, sample_wallet_dto: WalletProfileDTO
    ) -> None:
        """Test getting multiple wallets."""
        repo = WalletRepository(async_session)

        # Insert two wallets
        dto2 = WalletProfileDTO(
            address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            nonce=3,
            first_seen_at=datetime.now(UTC),
            is_fresh=True,
            matic_balance=None,
            usdc_balance=None,
            analyzed_at=datetime.now(UTC),
        )
        await repo.upsert(sample_wallet_dto)
        await repo.upsert(dto2)
        await async_session.commit()

        results = await repo.get_many([sample_wallet_dto.address, dto2.address])
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_get_fresh_wallets(
        self, async_session: AsyncSession, sample_wallet_dto: WalletProfileDTO
    ) -> None:
        """Test getting fresh wallets."""
        repo = WalletRepository(async_session)

        # Insert fresh and non-fresh wallets
        non_fresh = WalletProfileDTO(
            address="0xcccccccccccccccccccccccccccccccccccccccc",
            nonce=100,
            first_seen_at=datetime.now(UTC) - timedelta(days=30),
            is_fresh=False,
            matic_balance=None,
            usdc_balance=None,
            analyzed_at=datetime.now(UTC),
        )
        await repo.upsert(sample_wallet_dto)
        await repo.upsert(non_fresh)
        await async_session.commit()

        results = await repo.get_fresh_wallets()
        assert len(results) == 1
        assert results[0].is_fresh is True

    @pytest.mark.asyncio
    async def test_delete(
        self, async_session: AsyncSession, sample_wallet_dto: WalletProfileDTO
    ) -> None:
        """Test deleting a wallet profile."""
        repo = WalletRepository(async_session)
        await repo.upsert(sample_wallet_dto)
        await async_session.commit()

        deleted = await repo.delete(sample_wallet_dto.address)
        await async_session.commit()
        assert deleted is True

        result = await repo.get_by_address(sample_wallet_dto.address)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_not_found(self, async_session: AsyncSession) -> None:
        """Test deleting non-existent wallet returns False."""
        repo = WalletRepository(async_session)
        deleted = await repo.delete("0xnonexistent")
        assert deleted is False

    @pytest.mark.asyncio
    async def test_mark_stale(
        self, async_session: AsyncSession, sample_wallet_dto: WalletProfileDTO
    ) -> None:
        """Test marking a wallet as stale."""
        repo = WalletRepository(async_session)
        await repo.upsert(sample_wallet_dto)
        await async_session.commit()

        marked = await repo.mark_stale(sample_wallet_dto.address)
        await async_session.commit()
        assert marked is True

        result = await repo.get_by_address(sample_wallet_dto.address)
        assert result is not None
        assert result.analyzed_at.year == 2000


# ============================================================================
# FundingRepository Tests
# ============================================================================


class TestFundingRepository:
    """Tests for FundingRepository."""

    @pytest.mark.asyncio
    async def test_insert(
        self, async_session: AsyncSession, sample_transfer_dto: FundingTransferDTO
    ) -> None:
        """Test inserting a funding transfer."""
        repo = FundingRepository(async_session)
        await repo.insert(sample_transfer_dto)
        await async_session.commit()

        result = await repo.get_by_tx_hash(sample_transfer_dto.tx_hash)
        assert result is not None
        assert result.amount == sample_transfer_dto.amount

    @pytest.mark.asyncio
    async def test_get_transfers_to(
        self, async_session: AsyncSession, sample_transfer_dto: FundingTransferDTO
    ) -> None:
        """Test getting transfers to an address."""
        repo = FundingRepository(async_session)
        await repo.insert(sample_transfer_dto)
        await async_session.commit()

        results = await repo.get_transfers_to(sample_transfer_dto.to_address)
        assert len(results) == 1
        assert results[0].from_address == sample_transfer_dto.from_address.lower()

    @pytest.mark.asyncio
    async def test_get_transfers_from(
        self, async_session: AsyncSession, sample_transfer_dto: FundingTransferDTO
    ) -> None:
        """Test getting transfers from an address."""
        repo = FundingRepository(async_session)
        await repo.insert(sample_transfer_dto)
        await async_session.commit()

        results = await repo.get_transfers_from(sample_transfer_dto.from_address)
        assert len(results) == 1
        assert results[0].to_address == sample_transfer_dto.to_address.lower()

    @pytest.mark.asyncio
    async def test_get_first_transfer_to(
        self, async_session: AsyncSession, sample_transfer_dto: FundingTransferDTO
    ) -> None:
        """Test getting first transfer to an address."""
        repo = FundingRepository(async_session)

        # Insert multiple transfers with different timestamps
        earlier = FundingTransferDTO(
            from_address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            to_address=sample_transfer_dto.to_address,
            amount=Decimal("100.00"),
            token="USDC",
            tx_hash="0x" + "b" * 64,
            block_number=12345670,
            timestamp=datetime.now(UTC) - timedelta(hours=2),
        )
        await repo.insert(earlier)
        await repo.insert(sample_transfer_dto)
        await async_session.commit()

        result = await repo.get_first_transfer_to(sample_transfer_dto.to_address)
        assert result is not None
        assert result.tx_hash == earlier.tx_hash.lower()

    @pytest.mark.asyncio
    async def test_insert_many(self, async_session: AsyncSession) -> None:
        """Test inserting multiple transfers."""
        repo = FundingRepository(async_session)

        transfers = [
            FundingTransferDTO(
                from_address=f"0x{'a' * 40}",
                to_address=f"0x{'b' * 40}",
                amount=Decimal(f"{i * 100}.00"),
                token="USDC",
                tx_hash=f"0x{str(i) * 64}"[:66],
                block_number=12345678 + i,
                timestamp=datetime.now(UTC),
            )
            for i in range(1, 4)
        ]

        count = await repo.insert_many(transfers)
        await async_session.commit()
        assert count == 3


# ============================================================================
# RelationshipRepository Tests
# ============================================================================


class TestRelationshipRepository:
    """Tests for RelationshipRepository."""

    @pytest.mark.asyncio
    async def test_upsert(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test upserting a relationship."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)
        await async_session.commit()

        results = await repo.get_relationships(sample_relationship_dto.wallet_a)
        assert len(results) == 1
        assert results[0].confidence == sample_relationship_dto.confidence

    @pytest.mark.asyncio
    async def test_get_relationships_filter_type(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test getting relationships with type filter."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)

        same_entity = WalletRelationshipDTO(
            wallet_a=sample_relationship_dto.wallet_a,
            wallet_b="0xdddddddddddddddddddddddddddddddddddddddd",
            relationship_type="same_entity",
            confidence=Decimal("0.80"),
        )
        await repo.upsert(same_entity)
        await async_session.commit()

        funded_results = await repo.get_relationships(
            sample_relationship_dto.wallet_a, relationship_type="funded_by"
        )
        assert len(funded_results) == 1
        assert funded_results[0].relationship_type == "funded_by"

    @pytest.mark.asyncio
    async def test_get_related_wallets(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test getting related wallet addresses."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)
        await async_session.commit()

        related = await repo.get_related_wallets(sample_relationship_dto.wallet_a)
        assert len(related) == 1
        assert sample_relationship_dto.wallet_b.lower() in related

    @pytest.mark.asyncio
    async def test_delete_relationship(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test deleting a relationship."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)
        await async_session.commit()

        deleted = await repo.delete(
            sample_relationship_dto.wallet_a,
            sample_relationship_dto.wallet_b,
            sample_relationship_dto.relationship_type,
        )
        await async_session.commit()
        assert deleted is True

        results = await repo.get_relationships(sample_relationship_dto.wallet_a)
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_upsert_updates_confidence(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test that upserting updates the confidence."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)
        await async_session.commit()

        updated = WalletRelationshipDTO(
            wallet_a=sample_relationship_dto.wallet_a,
            wallet_b=sample_relationship_dto.wallet_b,
            relationship_type=sample_relationship_dto.relationship_type,
            confidence=Decimal("0.99"),
        )
        await repo.upsert(updated)
        await async_session.commit()

        results = await repo.get_relationships(sample_relationship_dto.wallet_a)
        assert len(results) == 1
        assert results[0].confidence == Decimal("0.99")


# ============================================================================
# DTO Tests
# ============================================================================


class TestDTOs:
    """Tests for Data Transfer Objects."""

    def test_wallet_profile_dto_from_model(self) -> None:
        """Test WalletProfileDTO.from_model works correctly."""
        from polymarket_insider_tracker.storage.models import WalletProfileModel

        now = datetime.now(UTC)
        model = WalletProfileModel(
            id=1,
            address="0x1234",
            nonce=5,
            first_seen_at=now,
            is_fresh=True,
            matic_balance=Decimal("100"),
            usdc_balance=Decimal("50.00"),
            analyzed_at=now,
            created_at=now,
            updated_at=now,
        )

        dto = WalletProfileDTO.from_model(model)
        assert dto.address == "0x1234"
        assert dto.nonce == 5
        assert dto.is_fresh is True

    def test_funding_transfer_dto_from_model(self) -> None:
        """Test FundingTransferDTO.from_model works correctly."""
        from polymarket_insider_tracker.storage.models import FundingTransferModel

        now = datetime.now(UTC)
        model = FundingTransferModel(
            id=1,
            from_address="0xaaa",
            to_address="0xbbb",
            amount=Decimal("100.00"),
            token="USDC",
            tx_hash="0x123",
            block_number=12345,
            timestamp=now,
            created_at=now,
        )

        dto = FundingTransferDTO.from_model(model)
        assert dto.from_address == "0xaaa"
        assert dto.amount == Decimal("100.00")

    def test_wallet_relationship_dto_from_model(self) -> None:
        """Test WalletRelationshipDTO.from_model works correctly."""
        from polymarket_insider_tracker.storage.models import WalletRelationshipModel

        now = datetime.now(UTC)
        model = WalletRelationshipModel(
            id=1,
            wallet_a="0xaaa",
            wallet_b="0xbbb",
            relationship_type="funded_by",
            confidence=Decimal("0.95"),
            created_at=now,
        )

        dto = WalletRelationshipDTO.from_model(model)
        assert dto.wallet_a == "0xaaa"
        assert dto.relationship_type == "funded_by"
        assert dto.confidence == Decimal("0.95")


class TestFundingDuplicateDetection:
    """``insert_many`` skips only the unique-violation messages SQLite and PostgreSQL emit."""

    @pytest.mark.parametrize(
        "message",
        [
            "UNIQUE constraint failed: funding_transfers.tx_hash",
            'duplicate key value violates unique constraint "funding_transfers_tx_hash_key"',
            "Duplicate Key value violates a constraint",
        ],
    )
    def test_unique_violations_are_duplicates(self, message: str) -> None:
        assert FundingRepository._is_duplicate_funding_error(RuntimeError(message)) is True

    @pytest.mark.parametrize(
        "message",
        [
            "connection reset by peer",
            "NOT NULL constraint failed: funding_transfers.tx_hash",
            "",
        ],
    )
    def test_other_errors_are_not_duplicates(self, message: str) -> None:
        assert FundingRepository._is_duplicate_funding_error(RuntimeError(message)) is False
