"""Pytest configuration and fixtures."""

import os
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from polymarket_insider_tracker.detector.models import (
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import Base


@pytest.fixture(scope="session", autouse=True)
def isolate_repository_dotenv(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """Keep unit tests from implicitly loading the contributor's repository `.env`.

    Yields the isolated working directory so a test can prove the harness contract holds.
    """
    original_directory = Path.cwd()
    isolated_directory = tmp_path_factory.mktemp("test-cwd")
    os.chdir(isolated_directory)
    try:
        yield isolated_directory
    finally:
        os.chdir(original_directory)


@pytest.fixture
async def fake_redis() -> AsyncIterator[FakeAsyncRedis]:
    """Isolated in-memory Redis with redis-py semantics.

    ``fakeredis`` is a ``redis.asyncio.Redis`` subclass backed by a private in-memory server, so
    every product Redis call runs unchanged. Its fidelity for the operations this product uses is
    proven by the shared contract suite in ``tests/integration/test_redis_contract.py``, which runs
    the same scenarios against a real loopback Redis in the services profile.
    """
    client = FakeAsyncRedis()
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def async_engine(tmp_path: Path) -> AsyncIterator[AsyncEngine]:
    """Real file-backed SQLite engine with the product schema created.

    The database is a per-test file, not ``:memory:``: an in-memory database lives and dies with
    its single pooled connection, so a task cancelled mid-statement (the pipeline stopping its
    poller) invalidates that connection and the next checkout opens a fresh, empty database
    ("no such table: risk_assessments"). A file survives connection replacement.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tracker.db'}", echo=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def db_manager(async_engine: AsyncEngine) -> DatabaseManager:
    """A real DatabaseManager whose async engine is the file-backed SQLite engine.

    ``DatabaseManager`` only accepts PostgreSQL URLs, so the SQLite engine is attached after
    construction; every session, repository, and transaction path is the production code.
    """
    manager = DatabaseManager(
        "postgresql+psycopg://tracker:unused@127.0.0.1:1/unit-tests", async_mode=True
    )
    manager.attach_async_engine(async_engine)
    return manager


@pytest.fixture
def sample_trade() -> TradeEvent:
    """Create the sample trade event shared by the alert-history and risk-scorer tests."""
    return TradeEvent(
        market_id="market_abc123",
        trade_id="tx_001",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.65"),
        size=Decimal("10000"),
        timestamp=datetime.now(UTC),
        asset_id="token_123",
        event_title="Test Market",
    )


@pytest.fixture
def sample_wallet_profile() -> WalletProfile:
    """Create the fresh wallet profile shared by the alert-history and risk-scorer tests."""
    return WalletProfile(
        address="0x1234567890abcdef1234567890abcdef12345678",
        nonce=2,
        first_seen=datetime.now(UTC),
        age_hours=1.0,
        is_fresh=True,
        total_tx_count=2,
        matic_balance=Decimal("1000000000000000000"),
        usdc_balance=Decimal("1000000"),
    )


@pytest.fixture
def fresh_wallet_signal(
    sample_trade: TradeEvent, sample_wallet_profile: WalletProfile
) -> FreshWalletSignal:
    """Create the fresh wallet signal shared by the formatter, history and scorer tests."""
    return FreshWalletSignal(
        trade_event=sample_trade,
        wallet_profile=sample_wallet_profile,
        confidence=0.8,
        factors={"base": 0.5, "brand_new_bonus": 0.2, "large_trade_bonus": 0.1},
    )


@pytest.fixture
def size_anomaly_signal(
    sample_trade: TradeEvent, sample_metadata: MarketMetadata
) -> SizeAnomalySignal:
    """Create the size anomaly signal shared by the formatter, history and scorer tests.

    The market metadata is each test module's own ``sample_metadata`` fixture.
    """
    return SizeAnomalySignal(
        trade_event=sample_trade,
        market_metadata=sample_metadata,
        volume_impact=0.10,
        book_impact=0.15,
        is_niche_market=True,
        confidence=0.7,
        factors={"volume_impact": 0.4, "book_impact": 0.3},
    )


@pytest.fixture
def high_risk_assessment(
    sample_trade: TradeEvent,
    fresh_wallet_signal: FreshWalletSignal,
    size_anomaly_signal: SizeAnomalySignal,
) -> RiskAssessment:
    """Create the high-risk assessment shared by the formatter and history tests."""
    return RiskAssessment(
        trade_event=sample_trade,
        wallet_address=sample_trade.wallet_address,
        market_id=sample_trade.market_id,
        fresh_wallet_signal=fresh_wallet_signal,
        size_anomaly_signal=size_anomaly_signal,
        signals_triggered=2,
        weighted_score=0.82,
        should_alert=True,
    )
