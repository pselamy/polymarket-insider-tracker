"""Tests for the FundingTracer module.

The tracer runs against a real ``PolygonClient`` whose JSON-RPC surface is ``FakeEth``; transfer
logs come from a ``TransferLogIndex`` that answers ``eth_getLogs`` by token, recipient, and block
window exactly like a node does.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.entities import EntityRegistry
from polymarket_insider_tracker.profiler.entity_data import EntityType
from polymarket_insider_tracker.profiler.funding import (
    DEFAULT_MAX_LOOKBACK_BLOCKS,
    TRANSFER_EVENT_SIGNATURE,
    USDC_BRIDGED,
    USDC_NATIVE,
    FundingTracer,
)
from polymarket_insider_tracker.profiler.models import FundingChain, FundingTransfer
from tests.fakes import FakeAsyncWeb3, FakeEth, TransferLogIndex, transfer_log

# Test addresses
TEST_WALLET = "0x1234567890abcdef1234567890abcdef12345678"
TEST_SOURCE = "0xabcdef1234567890abcdef1234567890abcdef12"
BINANCE_HOT_WALLET = "0x28c6c06298d514db089934071355e5743bf21d60"
CHAIN_HEAD = 50_000_000
PRUNED_HISTORY_ERROR = RuntimeError(
    "{'code': -32701, 'message': 'History has been pruned for this block. To remove "
    "restrictions, order a dedicated full node here: https://www.allnodes.com/pol/host'}"
)


def _polygon_client(eth: FakeEth, *, fallback: FakeEth | None = None) -> PolygonClient:
    """A real PolygonClient with its web3 providers bound to fakes and no retry delays."""
    client = PolygonClient(
        "https://polygon-rpc.com",
        fallback_rpc_url="https://fallback.invalid" if fallback else None,
        max_requests_per_second=10_000.0,
        max_retries=1,
        retry_delay_seconds=0.0,
    )
    client._w3 = FakeAsyncWeb3(eth)
    if fallback is not None:
        client._w3_fallback = FakeAsyncWeb3(fallback)
    return client


def _tracer(logs: TransferLogIndex, *, block_number: int = CHAIN_HEAD) -> FundingTracer:
    return _polygon_tracer(FakeEth(block_number=block_number, logs=logs))


def _polygon_tracer(eth: FakeEth, *, fallback: FakeEth | None = None) -> FundingTracer:
    return FundingTracer(_polygon_client(eth, fallback=fallback))


def _transfer_to(
    to_address: str, from_address: str, *, block_number: int = CHAIN_HEAD, seed: int = 0
) -> dict[str, Any]:
    return transfer_log(
        from_address=from_address,
        to_address=to_address,
        amount=1_000_000,
        tx_hash=f"0x{seed:064x}",
        block_number=block_number,
    )


@pytest.fixture
def entity_registry() -> EntityRegistry:
    """Create an EntityRegistry with default entities."""
    return EntityRegistry()


@pytest.fixture
def polygon_client() -> PolygonClient:
    """A Polygon client over a chain with no transfer history."""
    return _polygon_client(FakeEth(block_number=CHAIN_HEAD))


@pytest.fixture
def funding_tracer(polygon_client: PolygonClient, entity_registry: EntityRegistry) -> FundingTracer:
    """Create a FundingTracer over the empty chain."""
    return FundingTracer(polygon_client=polygon_client, entity_registry=entity_registry, max_hops=3)


class TestFundingTracerInit:
    """Tests for FundingTracer initialization."""

    def test_init_with_defaults(self, polygon_client: PolygonClient) -> None:
        """Test initialization with default parameters."""
        tracer = FundingTracer(polygon_client)

        assert tracer.polygon_client is polygon_client
        assert tracer.max_hops == 3
        assert USDC_BRIDGED.lower() in tracer._usdc_addresses
        assert USDC_NATIVE.lower() in tracer._usdc_addresses

    def test_init_with_custom_max_hops(self, polygon_client: PolygonClient) -> None:
        """Test initialization with custom max_hops."""
        tracer = FundingTracer(polygon_client, max_hops=5)
        assert tracer.max_hops == 5

    def test_init_with_custom_usdc_addresses(self, polygon_client: PolygonClient) -> None:
        """Test initialization with custom USDC addresses."""
        custom_addresses = ["0x1111111111111111111111111111111111111111"]
        tracer = FundingTracer(polygon_client, usdc_addresses=custom_addresses)
        assert tracer._usdc_addresses == [custom_addresses[0].lower()]

    def test_init_with_custom_entity_registry(self, polygon_client: PolygonClient) -> None:
        """Test initialization with custom entity registry."""
        registry = EntityRegistry()
        tracer = FundingTracer(polygon_client, entity_registry=registry)
        assert tracer.entity_registry is registry

    def test_init_creates_default_entity_registry(self, polygon_client: PolygonClient) -> None:
        """Test initialization creates default EntityRegistry if None."""
        tracer = FundingTracer(polygon_client, entity_registry=None)
        assert isinstance(tracer.entity_registry, EntityRegistry)


class TestFundingTracerTrace:
    """Tests for the trace method."""

    async def test_trace_terminates_at_known_cex(self, funding_tracer: FundingTracer) -> None:
        """Test trace terminates when starting at a CEX address."""
        result = await funding_tracer.trace(BINANCE_HOT_WALLET)

        assert result.target_address == BINANCE_HOT_WALLET.lower()
        assert result.origin_address == BINANCE_HOT_WALLET.lower()
        assert result.origin_type == EntityType.CEX_BINANCE.value
        assert result.hop_count == 0
        assert len(result.chain) == 0

    async def test_trace_no_transfers_found(self, funding_tracer: FundingTracer) -> None:
        """Test trace when no USDC transfers are found."""
        result = await funding_tracer.trace(TEST_WALLET)

        assert result.target_address == TEST_WALLET.lower()
        assert result.origin_address == TEST_WALLET.lower()
        assert result.chain == []
        assert result.origin_type == "unknown"
        assert result.hop_count == 0

    async def test_trace_finds_cex_origin(self, entity_registry: EntityRegistry) -> None:
        """Test trace finds CEX as funding origin."""
        logs = TransferLogIndex()
        logs.add_transfer(USDC_BRIDGED, _transfer_to(TEST_WALLET, BINANCE_HOT_WALLET, seed=0xAB))
        tracer = FundingTracer(_polygon_client(FakeEth(logs=logs)), entity_registry=entity_registry)

        result = await tracer.trace(TEST_WALLET)

        assert result.target_address == TEST_WALLET.lower()
        assert result.origin_address == BINANCE_HOT_WALLET.lower()
        assert result.origin_type == EntityType.CEX_BINANCE.value
        assert result.hop_count == 1
        assert result.is_cex_origin is True

    async def test_trace_multiple_hops(self, entity_registry: EntityRegistry) -> None:
        """Test trace follows multiple hops."""
        intermediate_wallet = "0x" + "11" * 20
        logs = TransferLogIndex()
        logs.add_transfer(USDC_BRIDGED, _transfer_to(TEST_WALLET, intermediate_wallet, seed=1))
        logs.add_transfer(
            USDC_BRIDGED, _transfer_to(intermediate_wallet, BINANCE_HOT_WALLET, seed=2)
        )
        tracer = FundingTracer(_polygon_client(FakeEth(logs=logs)), entity_registry=entity_registry)

        result = await tracer.trace(TEST_WALLET)

        assert result.hop_count == 2
        assert result.origin_address == BINANCE_HOT_WALLET.lower()
        assert result.is_cex_origin is True

    async def test_trace_respects_max_hops(self, entity_registry: EntityRegistry) -> None:
        """Test trace stops at max_hops."""
        wallets = [f"0x{i:040x}" for i in range(10)]
        logs = TransferLogIndex()
        for i in range(len(wallets) - 1):
            logs.add_transfer(
                USDC_BRIDGED,
                _transfer_to(wallets[i], wallets[i + 1], block_number=CHAIN_HEAD - i, seed=i),
            )
        tracer = FundingTracer(_polygon_client(FakeEth(logs=logs)), entity_registry=entity_registry)

        result = await tracer.trace(wallets[0], max_hops=3)

        assert result.hop_count == 3
        assert result.origin_type == "unknown"

    async def test_trace_override_max_hops(self, funding_tracer: FundingTracer) -> None:
        """Test trace can override default max_hops."""
        result = await funding_tracer.trace(TEST_WALLET, max_hops=1)

        assert result.hop_count == 0
        assert result.origin_type == "unknown"


class TestGetFirstUsdcTransfer:
    """Tests for get_first_usdc_transfer method."""

    async def test_get_first_usdc_transfer_bridged(self, entity_registry: EntityRegistry) -> None:
        """Test getting first USDC transfer from bridged contract."""
        logs = TransferLogIndex()
        logs.add_transfer(USDC_BRIDGED, _transfer_to(TEST_WALLET, TEST_SOURCE, seed=0xCC))
        tracer = FundingTracer(_polygon_client(FakeEth(logs=logs)), entity_registry=entity_registry)

        result = await tracer.get_first_usdc_transfer(TEST_WALLET)

        assert result is not None
        assert result.from_address == TEST_SOURCE.lower()
        assert result.to_address == TEST_WALLET.lower()
        assert result.amount == Decimal(1_000_000)
        assert result.token == "USDC"

    async def test_get_first_usdc_transfer_native(self, entity_registry: EntityRegistry) -> None:
        """Test fallback to native USDC contract when the bridged contract has no transfer."""
        logs = TransferLogIndex()
        logs.add_transfer(USDC_NATIVE, _transfer_to(TEST_WALLET, TEST_SOURCE, seed=0xDD))
        tracer = FundingTracer(_polygon_client(FakeEth(logs=logs)), entity_registry=entity_registry)

        result = await tracer.get_first_usdc_transfer(TEST_WALLET)

        assert result is not None
        assert result.token == "USDC"
        assert [query["address"].lower() for query in logs.queries][0] == USDC_BRIDGED.lower()
        assert USDC_NATIVE.lower() in {query["address"].lower() for query in logs.queries}

    async def test_get_first_usdc_transfer_none_found(self, funding_tracer: FundingTracer) -> None:
        """Test returns None when no USDC transfers found."""
        result = await funding_tracer.get_first_usdc_transfer(TEST_WALLET)
        assert result is None


class TestGetTransferLogs:
    """Tests for _get_transfer_logs method."""

    async def test_get_transfer_logs_formats_topics_correctly(self) -> None:
        """Test that transfer logs query is formatted correctly."""
        logs = TransferLogIndex()
        tracer = _tracer(logs)

        await tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            from_block=1,
            to_block=8_000,
        )

        assert len(logs.queries) == 1
        call_args = logs.queries[0]
        assert len(call_args["topics"]) == 3
        assert call_args["topics"][0] == "0x" + TRANSFER_EVENT_SIGNATURE.hex().removeprefix("0x")
        assert call_args["topics"][0].startswith("0x")
        assert call_args["topics"][1] is None
        assert call_args["topics"][2].endswith(TEST_WALLET.lower().replace("0x", ""))
        assert call_args["fromBlock"] == 1
        assert call_args["toBlock"] == 8_000

    async def test_get_transfer_logs_respects_limit(self) -> None:
        """Test that limit parameter works correctly."""
        logs = TransferLogIndex()
        for i in range(10):
            logs.add_transfer(
                USDC_BRIDGED, _transfer_to(TEST_WALLET, TEST_SOURCE, block_number=100 + i, seed=i)
            )
        tracer = _tracer(logs)

        result = await tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            limit=3,
            from_block=1,
            to_block=8_000,
        )

        assert len(result) == 3

    async def test_get_transfer_logs_uses_fallback_when_primary_unhealthy(self) -> None:
        """Once the primary RPC has failed over, log scans go to the fallback provider."""
        primary_logs, fallback_logs = TransferLogIndex(), TransferLogIndex()
        primary = FakeEth(always_fail=True, logs=primary_logs)
        fallback = FakeEth(logs=fallback_logs)
        client = _polygon_client(primary, fallback=fallback)
        await client.get_transaction_count(TEST_WALLET)
        tracer = FundingTracer(client)

        await tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            from_block=1,
            to_block=8_000,
        )

        assert primary_logs.queries == []
        assert len(fallback_logs.queries) == 1

    async def test_get_transfer_logs_chunks_large_ranges(self) -> None:
        """Ranges wider than chunk_size are split into multiple eth_getLogs calls."""
        logs = TransferLogIndex()
        tracer = _tracer(logs)

        await tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            from_block=1_000_000,
            to_block=1_025_000,
        )

        windows = [(query["fromBlock"], query["toBlock"]) for query in logs.queries]
        assert windows == [
            (1_000_000, 1_008_999),
            (1_009_000, 1_017_999),
            (1_018_000, 1_025_000),
        ]
        for from_block, to_block in windows:
            assert to_block - from_block + 1 <= 9_000

    async def test_get_transfer_logs_stops_when_limit_hit_mid_walk(self) -> None:
        """Walking should stop as soon as limit matches are gathered."""
        logs = TransferLogIndex()
        for i in range(5):
            logs.add_transfer(
                USDC_BRIDGED,
                _transfer_to(TEST_WALLET, TEST_SOURCE, block_number=1_000_100 + i, seed=i),
            )
        tracer = _tracer(logs)

        result = await tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            limit=2,
            from_block=1_000_000,
            to_block=1_027_000,
        )

        assert len(result) == 2
        assert len(logs.queries) == 1

    async def test_get_transfer_logs_skips_failing_chunk(self) -> None:
        """A flaky chunk must not abort the whole trace; the walk moves on."""
        logs = TransferLogIndex()
        logs.fail_chunk(1_000_000, RuntimeError("RPC hiccup"))
        good_log = _transfer_to(TEST_WALLET, TEST_SOURCE, block_number=1_009_500, seed=0xAA)
        logs.add_transfer(USDC_BRIDGED, good_log)
        tracer = _tracer(logs)

        result = await tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            from_block=1_000_000,
            to_block=1_018_000,
        )

        assert result == [good_log]
        assert len(logs.queries) == 3

    async def test_get_transfer_logs_resolves_latest_via_block_number(self) -> None:
        """to_block='latest' resolves via eth.block_number and from_block=0 is clamped."""
        logs = TransferLogIndex()
        tracer = _tracer(logs, block_number=5_000)

        await tracer._get_transfer_logs(to_address=TEST_WALLET, token_address=USDC_BRIDGED)

        assert [(query["fromBlock"], query["toBlock"]) for query in logs.queries] == [(0, 5_000)]

    async def test_get_transfer_logs_breaks_on_pruned_history(self) -> None:
        """A pruned-history error must short-circuit the whole walk."""
        logs = TransferLogIndex()
        good_log = _transfer_to(TEST_WALLET, TEST_SOURCE, block_number=1_000_500, seed=0xAA)
        logs.add_transfer(USDC_BRIDGED, good_log)
        logs.fail_chunk(1_009_000, PRUNED_HISTORY_ERROR)
        logs.add_transfer(
            USDC_BRIDGED, _transfer_to(TEST_WALLET, TEST_SOURCE, block_number=1_018_500, seed=0xBB)
        )
        tracer = _tracer(logs)

        result = await tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            from_block=1_000_000,
            to_block=1_027_000,
        )

        assert result == [good_log]
        assert len(logs.queries) == 2

    async def test_get_transfer_logs_default_lookback_fits_pruned_horizon(self) -> None:
        """Default max_lookback_blocks must stay inside what public RPCs serve."""
        assert DEFAULT_MAX_LOOKBACK_BLOCKS <= 100_000

    async def test_get_transfer_logs_topic_is_0x_prefixed(self) -> None:
        """The Transfer event topic passed to eth_getLogs must begin with 0x."""
        logs = TransferLogIndex()
        tracer = _tracer(logs)

        await tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            from_block=1,
            to_block=8_000,
        )

        topics = logs.queries[0]["topics"]
        assert topics[0].startswith("0x")
        assert len(topics[0]) == 2 + 64
        assert topics[2].startswith("0x")


class TestLogToFundingTransfer:
    """Tests for _log_to_funding_transfer method."""

    async def test_log_to_funding_transfer_parses_correctly(
        self, funding_tracer: FundingTracer
    ) -> None:
        """Test correct parsing of log to FundingTransfer."""
        log = transfer_log(
            from_address=TEST_SOURCE,
            to_address=TEST_WALLET,
            amount=1500000,
            tx_hash="0x" + "ee" * 32,
            block_number=CHAIN_HEAD,
        )

        result = await funding_tracer._log_to_funding_transfer(log, USDC_BRIDGED)

        assert result.from_address == TEST_SOURCE.lower()
        assert result.to_address == TEST_WALLET.lower()
        assert result.amount == Decimal(1500000)
        assert result.token == "USDC"
        assert result.tx_hash == "ee" * 32
        assert result.block_number == CHAIN_HEAD
        assert result.timestamp == datetime.fromtimestamp(1704369600, tz=UTC)

    async def test_log_to_funding_transfer_handles_block_error(self) -> None:
        """Test graceful handling when block fetch fails."""
        tracer = _polygon_tracer(FakeEth(always_fail=True))
        log = transfer_log(
            from_address=TEST_SOURCE,
            to_address=TEST_WALLET,
            amount=1000000,
            tx_hash="0x" + "ff" * 32,
            block_number=CHAIN_HEAD,
        )

        result = await tracer._log_to_funding_transfer(log, USDC_BRIDGED)

        assert result.from_address == TEST_SOURCE.lower()
        assert result.timestamp is not None


class TestGetFundingChainsBatch:
    """Tests for get_funding_chains_batch method."""

    @pytest.mark.asyncio
    async def test_batch_traces_multiple_addresses(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test batch tracing multiple addresses."""
        addresses = [f"0x{i:040x}" for i in range(3)]

        async def fake_trace(
            addr: str,
            *,
            max_hops: int | None = None,
        ) -> FundingChain:
            _ = max_hops
            return FundingChain(
                target_address=addr.lower(),
                origin_type="unknown",
            )

        funding_tracer.trace = fake_trace

        results = await funding_tracer.get_funding_chains_batch(addresses)

        assert len(results) == 3
        for addr in addresses:
            assert addr.lower() in results

    @pytest.mark.asyncio
    async def test_batch_handles_exceptions(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test batch handles exceptions gracefully."""
        addresses = ["0x" + "11" * 20, "0x" + "22" * 20]
        call_count = 0

        async def fake_trace(
            addr: str,
            *,
            max_hops: int | None = None,
        ) -> FundingChain:
            nonlocal call_count
            _ = max_hops
            call_count += 1
            if call_count == 1:
                raise ValueError("Test error")
            return FundingChain(
                target_address=addr.lower(),
                origin_type="cex_binance",
            )

        funding_tracer.trace = fake_trace

        results = await funding_tracer.get_funding_chains_batch(addresses)

        assert len(results) == 2
        assert results[addresses[0].lower()].origin_type == "error"
        assert results[addresses[1].lower()].origin_type == "cex_binance"

    @pytest.mark.asyncio
    async def test_batch_empty_list(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test batch with empty address list."""
        results = await funding_tracer.get_funding_chains_batch([])
        assert results == {}

    @pytest.mark.asyncio
    async def test_batch_respects_max_hops_override(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test batch passes max_hops to individual traces."""
        addresses = ["0x" + "11" * 20]
        captured_max_hops: list[int | None] = []

        async def fake_trace(addr: str, max_hops: int | None = None) -> FundingChain:
            captured_max_hops.append(max_hops)
            return FundingChain(target_address=addr.lower())

        funding_tracer.trace = fake_trace

        await funding_tracer.get_funding_chains_batch(addresses, max_hops=5)

        assert captured_max_hops == [5]


class TestGetSuspiciousnessScore:
    """Tests for get_suspiciousness_score method."""

    def test_cex_origin_low_score(self, funding_tracer: FundingTracer) -> None:
        """Test CEX origin results in low suspiciousness."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="cex_binance",
            hop_count=1,
        )

        score = funding_tracer.get_suspiciousness_score(chain)

        assert score == 0.1

    def test_bridge_origin_low_score(self, funding_tracer: FundingTracer) -> None:
        """Test bridge origin results in low-medium suspiciousness."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="bridge_polygon",
            hop_count=1,
        )

        score = funding_tracer.get_suspiciousness_score(chain)

        assert score == 0.3

    def test_unknown_no_transfers_high_score(self, funding_tracer: FundingTracer) -> None:
        """Test unknown origin with no transfers is most suspicious."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="unknown",
            hop_count=0,
        )

        score = funding_tracer.get_suspiciousness_score(chain)

        assert score == 1.0

    def test_unknown_max_hops_high_score(self, funding_tracer: FundingTracer) -> None:
        """Test unknown origin at max hops is suspicious."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="unknown",
            hop_count=3,  # Same as max_hops
        )

        score = funding_tracer.get_suspiciousness_score(chain)

        assert score == 0.7

    def test_unknown_partial_hops_medium_score(self, funding_tracer: FundingTracer) -> None:
        """Test unknown origin with partial hops is moderately suspicious."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="unknown",
            hop_count=1,
        )

        score = funding_tracer.get_suspiciousness_score(chain)

        assert 0.5 < score < 0.8


class TestFundingTransferModel:
    """Tests for FundingTransfer dataclass."""

    def test_amount_formatted_usdc(self) -> None:
        """Test formatted amount for USDC (6 decimals)."""
        transfer = FundingTransfer(
            from_address=TEST_SOURCE,
            to_address=TEST_WALLET,
            amount=Decimal("1500000"),  # 1.5 USDC
            token="USDC",
            tx_hash="0x" + "aa" * 32,
            block_number=50000000,
            timestamp=datetime.now(UTC),
        )

        assert transfer.amount_formatted == Decimal("1.5")

    def test_amount_formatted_other(self) -> None:
        """Test formatted amount for other tokens (18 decimals)."""
        transfer = FundingTransfer(
            from_address=TEST_SOURCE,
            to_address=TEST_WALLET,
            amount=Decimal("1500000000000000000"),  # 1.5 MATIC
            token="MATIC",
            tx_hash="0x" + "aa" * 32,
            block_number=50000000,
            timestamp=datetime.now(UTC),
        )

        assert transfer.amount_formatted == Decimal("1.5")


class TestFundingChainModel:
    """Tests for FundingChain dataclass."""

    def test_is_cex_origin(self) -> None:
        """Test is_cex_origin property."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="cex_binance",
        )
        assert chain.is_cex_origin is True

        chain2 = FundingChain(
            target_address=TEST_WALLET,
            origin_type="bridge_polygon",
        )
        assert chain2.is_cex_origin is False

    def test_is_bridge_origin(self) -> None:
        """Test is_bridge_origin property."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="bridge_polygon",
        )
        assert chain.is_bridge_origin is True

        chain2 = FundingChain(
            target_address=TEST_WALLET,
            origin_type="cex_coinbase",
        )
        assert chain2.is_bridge_origin is False

    def test_is_unknown_origin(self) -> None:
        """Test is_unknown_origin property."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="unknown",
        )
        assert chain.is_unknown_origin is True

    def test_total_amount_empty_chain(self) -> None:
        """Test total_amount with empty chain."""
        chain = FundingChain(target_address=TEST_WALLET)
        assert chain.total_amount == Decimal("0")

    def test_total_amount_with_transfers(self) -> None:
        """Test total_amount returns first transfer amount."""
        transfer = FundingTransfer(
            from_address=TEST_SOURCE,
            to_address=TEST_WALLET,
            amount=Decimal("5000000"),
            token="USDC",
            tx_hash="0x" + "aa" * 32,
            block_number=50000000,
            timestamp=datetime.now(UTC),
        )
        chain = FundingChain(
            target_address=TEST_WALLET,
            chain=[transfer],
        )

        assert chain.total_amount == Decimal("5000000")

    def test_funding_depth(self) -> None:
        """Test funding_depth property."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            hop_count=3,
        )
        assert chain.funding_depth == 3

    def test_traced_at_defaults_to_an_aware_utc_timestamp(self) -> None:
        """A trace records when it ran, in UTC, so chains remain comparable across hosts."""
        before = datetime.now(UTC)
        chain = FundingChain(target_address=TEST_WALLET)
        after = datetime.now(UTC)

        assert chain.traced_at.tzinfo is UTC
        assert before <= chain.traced_at <= after


class TestConstants:
    """Tests for module constants."""

    def test_usdc_bridged_address(self) -> None:
        """Test USDC bridged contract address."""
        assert USDC_BRIDGED == "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    def test_usdc_native_address(self) -> None:
        """Test USDC native contract address."""
        assert USDC_NATIVE == "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

    def test_transfer_event_signature(self) -> None:
        """Test Transfer event signature is correct keccak hash."""
        assert TRANSFER_EVENT_SIGNATURE is not None
        assert len(TRANSFER_EVENT_SIGNATURE) == 32
