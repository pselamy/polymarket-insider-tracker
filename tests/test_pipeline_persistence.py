"""Tests verifying wallet and funding data persistence in the pipeline.

These tests confirm that running the live pipeline writes rows into
wallet_profiles and funding_transfers tables when fresh wallets are detected.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.models import FreshWalletSignal
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.pipeline import Pipeline
from polymarket_insider_tracker.profiler.models import FundingChain, FundingTransfer, WalletProfile
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import Base, FundingTransferModel, WalletProfileModel


@pytest.fixture
def mock_settings():
    """Create mock settings for testing."""
    redis = MagicMock()
    redis.url = "redis://localhost:6379"

    database = MagicMock()
    database.url = "sqlite+aiosqlite:///:memory:"

    polygon = MagicMock()
    polygon.rpc_url = "https://polygon-rpc.com"
    polygon.fallback_rpc_url = None

    polymarket = MagicMock()
    polymarket.ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    polymarket.api_key = None

    discord = MagicMock()
    discord.enabled = False
    discord.webhook_url = None

    telegram = MagicMock()
    telegram.enabled = False
    telegram.bot_token = None
    telegram.chat_id = None

    settings = MagicMock(spec=Settings)
    settings.redis = redis
    settings.database = database
    settings.polygon = polygon
    settings.polymarket = polymarket
    settings.discord = discord
    settings.telegram = telegram
    settings.dry_run = True
    return settings


@pytest.fixture
async def async_engine():
    """Create an async SQLite engine for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_manager(async_engine):
    """Create a DatabaseManager backed by the in-memory SQLite engine."""
    manager = DatabaseManager.__new__(DatabaseManager)
    manager.database_url = "sqlite+aiosqlite:///:memory:"
    manager.async_mode = True
    manager._pool_size = 5
    manager._max_overflow = 10
    manager._echo = False
    manager._sync_engine = None
    manager._async_engine = async_engine
    manager._sync_session_factory = None
    manager._async_session_factory = async_sessionmaker(
        bind=async_engine, expire_on_commit=False
    )
    return manager


@pytest.fixture
def sample_trade():
    """Create a sample trade event."""
    return TradeEvent(
        trade_id="0x" + "a" * 64,
        wallet_address="0x" + "b" * 40,
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
def sample_profile():
    """Create a sample fresh wallet profile."""
    return WalletProfile(
        address="0x" + "b" * 40,
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
def sample_funding_chain():
    """Create a sample funding chain with one transfer."""
    return FundingChain(
        target_address="0x" + "b" * 40,
        chain=[
            FundingTransfer(
                from_address="0x" + "d" * 40,
                to_address="0x" + "b" * 40,
                amount=Decimal("5000000000"),
                token="USDC",
                tx_hash="0x" + "e" * 64,
                block_number=12345678,
                timestamp=datetime(2026, 3, 31, 11, 0, 0, tzinfo=UTC),
            ),
        ],
        origin_address="0x" + "d" * 40,
        origin_type="cex_binance",
        hop_count=1,
    )


class TestPipelinePersistence:
    """Tests that the pipeline persists wallet and funding data to Postgres."""

    @pytest.mark.asyncio
    async def test_on_trade_persists_wallet_profile(
        self, mock_settings, db_manager, sample_trade, sample_profile, async_engine
    ):
        """When a fresh wallet signal fires, the wallet profile is written to wallet_profiles."""
        pipeline = Pipeline(mock_settings)
        pipeline._db_manager = db_manager

        fresh_signal = FreshWalletSignal(
            trade_event=sample_trade,
            wallet_profile=sample_profile,
            confidence=0.8,
            factors={"base": 0.5, "brand_new": 0.2},
        )

        pipeline._fresh_wallet_detector = MagicMock()
        pipeline._fresh_wallet_detector.analyze = AsyncMock(return_value=fresh_signal)
        pipeline._size_anomaly_detector = MagicMock()
        pipeline._size_anomaly_detector.analyze = AsyncMock(return_value=None)
        pipeline._funding_tracer = MagicMock()
        pipeline._funding_tracer.trace = AsyncMock(
            return_value=FundingChain(target_address=sample_profile.address)
        )
        pipeline._risk_scorer = MagicMock()
        pipeline._risk_scorer.assess = AsyncMock(
            return_value=MagicMock(should_alert=False, weighted_score=0.3)
        )
        pipeline._alert_formatter = MagicMock()
        pipeline._alert_dispatcher = MagicMock()

        await pipeline._on_trade(sample_trade)

        # Verify wallet_profiles has a row
        async with async_sessionmaker(bind=async_engine, expire_on_commit=False)() as session:
            result = await session.execute(select(WalletProfileModel))
            rows = result.scalars().all()
            assert len(rows) == 1
            assert rows[0].address == sample_profile.address.lower()
            assert rows[0].nonce == sample_profile.nonce
            assert rows[0].is_fresh is True

    @pytest.mark.asyncio
    async def test_on_trade_persists_funding_transfers(
        self,
        mock_settings,
        db_manager,
        sample_trade,
        sample_profile,
        sample_funding_chain,
        async_engine,
    ):
        """When a fresh wallet signal fires, funding transfers are written to funding_transfers."""
        pipeline = Pipeline(mock_settings)
        pipeline._db_manager = db_manager

        fresh_signal = FreshWalletSignal(
            trade_event=sample_trade,
            wallet_profile=sample_profile,
            confidence=0.8,
            factors={"base": 0.5, "brand_new": 0.2},
        )

        pipeline._fresh_wallet_detector = MagicMock()
        pipeline._fresh_wallet_detector.analyze = AsyncMock(return_value=fresh_signal)
        pipeline._size_anomaly_detector = MagicMock()
        pipeline._size_anomaly_detector.analyze = AsyncMock(return_value=None)
        pipeline._funding_tracer = MagicMock()
        pipeline._funding_tracer.trace = AsyncMock(return_value=sample_funding_chain)
        pipeline._risk_scorer = MagicMock()
        pipeline._risk_scorer.assess = AsyncMock(
            return_value=MagicMock(should_alert=False, weighted_score=0.3)
        )
        pipeline._alert_formatter = MagicMock()
        pipeline._alert_dispatcher = MagicMock()

        await pipeline._on_trade(sample_trade)

        # Verify funding_transfers has a row
        async with async_sessionmaker(bind=async_engine, expire_on_commit=False)() as session:
            result = await session.execute(select(FundingTransferModel))
            rows = result.scalars().all()
            assert len(rows) == 1
            assert rows[0].to_address == ("0x" + "b" * 40).lower()
            assert rows[0].from_address == ("0x" + "d" * 40).lower()
            assert rows[0].token == "USDC"
            assert rows[0].tx_hash == ("0x" + "e" * 64).lower()

    @pytest.mark.asyncio
    async def test_no_persistence_without_fresh_signal(
        self, mock_settings, db_manager, sample_trade, async_engine
    ):
        """No rows written when fresh wallet signal is None (wallet not fresh)."""
        pipeline = Pipeline(mock_settings)
        pipeline._db_manager = db_manager

        pipeline._fresh_wallet_detector = MagicMock()
        pipeline._fresh_wallet_detector.analyze = AsyncMock(return_value=None)
        pipeline._size_anomaly_detector = MagicMock()
        pipeline._size_anomaly_detector.analyze = AsyncMock(return_value=None)

        await pipeline._on_trade(sample_trade)

        async with async_sessionmaker(bind=async_engine, expire_on_commit=False)() as session:
            wallets = (await session.execute(select(WalletProfileModel))).scalars().all()
            transfers = (await session.execute(select(FundingTransferModel))).scalars().all()
            assert len(wallets) == 0
            assert len(transfers) == 0

    @pytest.mark.asyncio
    async def test_persistence_failure_does_not_break_pipeline(
        self, mock_settings, sample_trade, sample_profile
    ):
        """Persistence errors are caught and don't crash trade processing."""
        pipeline = Pipeline(mock_settings)

        # Use a broken db_manager that raises on get_async_session
        broken_db = MagicMock()
        broken_db.get_async_session = MagicMock(
            side_effect=Exception("DB connection failed")
        )
        pipeline._db_manager = broken_db

        fresh_signal = FreshWalletSignal(
            trade_event=sample_trade,
            wallet_profile=sample_profile,
            confidence=0.8,
            factors={"base": 0.5},
        )

        pipeline._fresh_wallet_detector = MagicMock()
        pipeline._fresh_wallet_detector.analyze = AsyncMock(return_value=fresh_signal)
        pipeline._size_anomaly_detector = MagicMock()
        pipeline._size_anomaly_detector.analyze = AsyncMock(return_value=None)
        pipeline._funding_tracer = MagicMock()
        pipeline._risk_scorer = MagicMock()
        pipeline._risk_scorer.assess = AsyncMock(
            return_value=MagicMock(should_alert=False, weighted_score=0.3)
        )
        pipeline._alert_formatter = MagicMock()
        pipeline._alert_dispatcher = MagicMock()

        # Should not raise
        await pipeline._on_trade(sample_trade)
        assert pipeline.stats.trades_processed == 1

    @pytest.mark.asyncio
    async def test_duplicate_funding_transfers_are_skipped(
        self,
        mock_settings,
        db_manager,
        sample_trade,
        sample_profile,
        sample_funding_chain,
        async_engine,
    ):
        """Processing the same trade twice should not duplicate funding transfer rows."""
        pipeline = Pipeline(mock_settings)
        pipeline._db_manager = db_manager

        fresh_signal = FreshWalletSignal(
            trade_event=sample_trade,
            wallet_profile=sample_profile,
            confidence=0.8,
            factors={"base": 0.5},
        )

        pipeline._fresh_wallet_detector = MagicMock()
        pipeline._fresh_wallet_detector.analyze = AsyncMock(return_value=fresh_signal)
        pipeline._size_anomaly_detector = MagicMock()
        pipeline._size_anomaly_detector.analyze = AsyncMock(return_value=None)
        pipeline._funding_tracer = MagicMock()
        pipeline._funding_tracer.trace = AsyncMock(return_value=sample_funding_chain)
        pipeline._risk_scorer = MagicMock()
        pipeline._risk_scorer.assess = AsyncMock(
            return_value=MagicMock(should_alert=False, weighted_score=0.3)
        )
        pipeline._alert_formatter = MagicMock()
        pipeline._alert_dispatcher = MagicMock()

        # Process same trade twice
        await pipeline._on_trade(sample_trade)
        await pipeline._on_trade(sample_trade)

        # Should still have only 1 funding transfer (duplicate skipped)
        async with async_sessionmaker(bind=async_engine, expire_on_commit=False)() as session:
            result = await session.execute(select(FundingTransferModel))
            rows = result.scalars().all()
            assert len(rows) == 1
