"""Tests that the pipeline persists wallet profiles and funding transfers.

The pipeline runs with its real detectors, analyzer, funding tracer, and repositories; only the
Polygon RPC (``FakeEth``), Redis (``fakeredis``), and the database engine (in-memory SQLite) are
substituted.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.models import FreshWalletSignal
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.profiler.funding import USDC_BRIDGED
from polymarket_insider_tracker.profiler.models import WalletProfile
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import FundingTransferModel, WalletProfileModel
from tests.fakes import FakeEth, TransferLogIndex, make_test_settings, transfer_log, wire_pipeline

WALLET = "0x" + "b" * 40
FUNDER = "0x" + "d" * 40
FUNDING_TX_HASH = "0x" + "e" * 64
CHAIN_HEAD = 12_400_000
UNREACHABLE_DATABASE_URL = "postgresql+psycopg://tracker:unused@127.0.0.1:1/unreachable"


@pytest.fixture
def test_settings() -> Settings:
    """Create concrete test settings."""
    return make_test_settings()


@pytest.fixture
def sample_trade() -> TradeEvent:
    """Create a sample trade event."""
    return TradeEvent(
        trade_id="0x" + "a" * 64,
        wallet_address=WALLET,
        market_id="0x" + "c" * 64,
        asset_id="asset_123",
        side="BUY",
        price=Decimal("0.65"),
        size=Decimal("5000"),
        timestamp=datetime.now(UTC),
        outcome="Yes",
        outcome_index=0,
        event_title="Test Market",
        market_slug="test-market",
    )


@pytest.fixture
def mainstream_market(sample_trade: TradeEvent) -> MarketMetadata:
    return MarketMetadata(
        condition_id=sample_trade.market_id,
        question="Test Market",
        description="",
        tokens=(Token(token_id=sample_trade.asset_id, outcome="Yes", price=Decimal("0.65")),),
        category="politics",
    )


@pytest.fixture
def sample_profile() -> WalletProfile:
    """Create a sample fresh wallet profile."""
    return WalletProfile(
        address=WALLET,
        nonce=2,
        first_seen=datetime(2026, 3, 31, 12, 0, 0, tzinfo=UTC),
        age_hours=1.5,
        is_fresh=True,
        total_tx_count=2,
        matic_balance=Decimal("1000000000000000000"),
        usdc_balance=Decimal("5000000000"),
        fresh_threshold=5,
    )


@pytest.fixture
def funded_chain() -> TransferLogIndex:
    """On-chain history with one USDC transfer from ``FUNDER`` into the traded wallet."""
    index = TransferLogIndex()
    index.add_transfer(
        USDC_BRIDGED,
        transfer_log(
            from_address=FUNDER,
            to_address=WALLET,
            amount=5_000_000_000,
            tx_hash=FUNDING_TX_HASH,
            block_number=12_345_678,
        ),
    )
    return index


def _fresh_wallet_eth(chain: TransferLogIndex | None = None) -> FakeEth:
    """A Polygon RPC that reports a two-transaction wallet, which the analyzer deems fresh."""
    return FakeEth(transaction_count=2, block_number=CHAIN_HEAD, logs=chain)


async def _wallet_rows(engine: AsyncEngine) -> list[WalletProfileModel]:
    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(WalletProfileModel))).scalars().all())


async def _funding_rows(engine: AsyncEngine) -> list[FundingTransferModel]:
    async with async_sessionmaker(bind=engine, expire_on_commit=False)() as session:
        return list((await session.execute(select(FundingTransferModel))).scalars().all())


class TestPipelinePersistence:
    """Tests that the pipeline persists wallet and funding data to the database."""

    async def test_on_trade_persists_wallet_profile(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade: TradeEvent,
        mainstream_market: MarketMetadata,
    ) -> None:
        """When a fresh wallet signal fires, the wallet profile is written to wallet_profiles."""
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=_fresh_wallet_eth(),
            market=mainstream_market,
            db_manager=db_manager,
        )

        await pipeline._on_trade(sample_trade)

        rows = await _wallet_rows(async_engine)
        assert [(row.address, row.nonce, row.is_fresh) for row in rows] == [(WALLET, 2, True)]
        assert pipeline.stats.signals_generated == 1

    async def test_persists_wallet_without_a_funding_tracer(
        self,
        test_settings: Settings,
        db_manager: DatabaseManager,
        sample_trade: TradeEvent,
        sample_profile: WalletProfile,
        async_engine: AsyncEngine,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Guard the explicitly initialized transfer count when no tracer is configured."""
        pipeline = Pipeline(test_settings)
        pipeline._db_manager = db_manager
        pipeline._funding_tracer = None
        fresh_signal = FreshWalletSignal(
            trade_event=sample_trade,
            wallet_profile=sample_profile,
            confidence=0.8,
            factors={"base": 0.5},
        )

        with caplog.at_level(logging.WARNING):
            await pipeline._persist_wallet_and_funding(fresh_signal)

        assert "Failed to persist wallet/funding data" not in caplog.text
        rows = await _wallet_rows(async_engine)
        assert [row.address for row in rows] == [sample_profile.address]
        assert await _funding_rows(async_engine) == []

    async def test_on_trade_persists_funding_transfers(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade: TradeEvent,
        mainstream_market: MarketMetadata,
        funded_chain: TransferLogIndex,
    ) -> None:
        """When a fresh wallet signal fires, traced funding transfers are written."""
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=_fresh_wallet_eth(funded_chain),
            market=mainstream_market,
            db_manager=db_manager,
        )

        await pipeline._on_trade(sample_trade)

        rows = await _funding_rows(async_engine)
        # The tracer stores ``transactionHash.hex()``, which is bare hex without a ``0x`` prefix.
        assert [(row.from_address, row.to_address, row.token, row.tx_hash) for row in rows] == [
            (FUNDER, WALLET, "USDC", FUNDING_TX_HASH.removeprefix("0x"))
        ]
        assert rows[0].block_number == 12_345_678
        assert rows[0].amount == Decimal("5000000000")

    async def test_no_persistence_without_fresh_signal(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade: TradeEvent,
        mainstream_market: MarketMetadata,
        funded_chain: TransferLogIndex,
    ) -> None:
        """No rows are written when the wallet is not fresh."""
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=FakeEth(transaction_count=100, block_number=CHAIN_HEAD, logs=funded_chain),
            market=mainstream_market,
            db_manager=db_manager,
        )

        await pipeline._on_trade(sample_trade)

        assert await _wallet_rows(async_engine) == []
        assert await _funding_rows(async_engine) == []
        assert pipeline.stats.signals_generated == 0

    async def test_persistence_failure_does_not_break_pipeline(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        mainstream_market: MarketMetadata,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An unreachable database is logged and never crashes trade processing."""
        unreachable_db = DatabaseManager(UNREACHABLE_DATABASE_URL, async_mode=True)
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=_fresh_wallet_eth(),
            market=mainstream_market,
            db_manager=unreachable_db,
        )

        try:
            with caplog.at_level(logging.WARNING):
                await pipeline._on_trade(sample_trade)
        finally:
            await unreachable_db.dispose_async()

        assert "Failed to persist wallet/funding data" in caplog.text
        assert pipeline.stats.trades_processed == 1
        assert pipeline.stats.errors == 0

    async def test_duplicate_funding_transfers_are_skipped(
        self,
        test_settings: Settings,
        fake_redis: FakeAsyncRedis,
        db_manager: DatabaseManager,
        async_engine: AsyncEngine,
        sample_trade: TradeEvent,
        mainstream_market: MarketMetadata,
        funded_chain: TransferLogIndex,
    ) -> None:
        """Processing the same trade twice should not duplicate funding transfer rows."""
        pipeline = await wire_pipeline(
            test_settings,
            redis=fake_redis,
            eth=_fresh_wallet_eth(funded_chain),
            market=mainstream_market,
            db_manager=db_manager,
        )

        await pipeline._on_trade(sample_trade)
        await pipeline._on_trade(sample_trade)

        assert len(await _funding_rows(async_engine)) == 1
        assert len(await _wallet_rows(async_engine)) == 1
        assert pipeline.stats.errors == 0
