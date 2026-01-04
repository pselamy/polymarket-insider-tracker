"""Tests for the FundingTracer module."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from polymarket_insider_tracker.profiler.entities import EntityRegistry
from polymarket_insider_tracker.profiler.entity_data import EntityType
from polymarket_insider_tracker.profiler.funding import (
    TRANSFER_EVENT_SIGNATURE,
    USDC_BRIDGED,
    USDC_NATIVE,
    FundingTracer,
)
from polymarket_insider_tracker.profiler.models import FundingChain, FundingTransfer

# Test addresses
TEST_WALLET = "0x1234567890abcdef1234567890abcdef12345678"
TEST_SOURCE = "0xabcdef1234567890abcdef1234567890abcdef12"
BINANCE_HOT_WALLET = "0x28c6c06298d514db089934071355e5743bf21d60"


@pytest.fixture
def mock_polygon_client() -> MagicMock:
    """Create a mock PolygonClient."""
    client = MagicMock()
    client._rate_limiter = MagicMock()
    client._rate_limiter.acquire = AsyncMock()
    client._primary_healthy = True
    client._w3 = MagicMock()
    client._w3_fallback = None
    client.get_block = AsyncMock(return_value={"timestamp": 1704067200})
    return client


@pytest.fixture
def entity_registry() -> EntityRegistry:
    """Create an EntityRegistry with default entities."""
    return EntityRegistry()


@pytest.fixture
def funding_tracer(
    mock_polygon_client: MagicMock,
    entity_registry: EntityRegistry,
) -> FundingTracer:
    """Create a FundingTracer with mocked dependencies."""
    return FundingTracer(
        polygon_client=mock_polygon_client,
        entity_registry=entity_registry,
        max_hops=3,
    )


class TestFundingTracerInit:
    """Tests for FundingTracer initialization."""

    def test_init_with_defaults(self, mock_polygon_client: MagicMock) -> None:
        """Test initialization with default parameters."""
        tracer = FundingTracer(mock_polygon_client)

        assert tracer.polygon_client is mock_polygon_client
        assert tracer.max_hops == 3
        assert USDC_BRIDGED.lower() in tracer._usdc_addresses
        assert USDC_NATIVE.lower() in tracer._usdc_addresses

    def test_init_with_custom_max_hops(self, mock_polygon_client: MagicMock) -> None:
        """Test initialization with custom max_hops."""
        tracer = FundingTracer(mock_polygon_client, max_hops=5)
        assert tracer.max_hops == 5

    def test_init_with_custom_usdc_addresses(
        self, mock_polygon_client: MagicMock
    ) -> None:
        """Test initialization with custom USDC addresses."""
        custom_addresses = ["0x1111111111111111111111111111111111111111"]
        tracer = FundingTracer(
            mock_polygon_client, usdc_addresses=custom_addresses
        )
        assert tracer._usdc_addresses == [custom_addresses[0].lower()]

    def test_init_with_custom_entity_registry(
        self, mock_polygon_client: MagicMock
    ) -> None:
        """Test initialization with custom entity registry."""
        registry = EntityRegistry()
        tracer = FundingTracer(mock_polygon_client, entity_registry=registry)
        assert tracer.entity_registry is registry

    def test_init_creates_default_entity_registry(
        self, mock_polygon_client: MagicMock
    ) -> None:
        """Test initialization creates default EntityRegistry if None."""
        tracer = FundingTracer(mock_polygon_client, entity_registry=None)
        assert isinstance(tracer.entity_registry, EntityRegistry)


class TestFundingTracerTrace:
    """Tests for the trace method."""

    @pytest.mark.asyncio
    async def test_trace_terminates_at_known_cex(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test trace terminates when starting at a CEX address."""
        result = await funding_tracer.trace(BINANCE_HOT_WALLET)

        assert result.target_address == BINANCE_HOT_WALLET.lower()
        assert result.origin_address == BINANCE_HOT_WALLET.lower()
        assert result.origin_type == EntityType.CEX_BINANCE.value
        assert result.hop_count == 0
        assert len(result.chain) == 0

    @pytest.mark.asyncio
    async def test_trace_no_transfers_found(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test trace when no USDC transfers are found."""
        funding_tracer._get_transfer_logs = AsyncMock(return_value=[])

        result = await funding_tracer.trace(TEST_WALLET)

        assert result.target_address == TEST_WALLET.lower()
        assert result.origin_address == TEST_WALLET.lower()
        assert result.origin_type == "unknown"
        assert result.hop_count == 0

    @pytest.mark.asyncio
    async def test_trace_finds_cex_origin(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test trace finds CEX as funding origin."""
        # Mock a transfer from Binance to test wallet
        mock_log = _create_mock_log(
            from_address=BINANCE_HOT_WALLET,
            to_address=TEST_WALLET,
            amount=1000000,  # 1 USDC
            tx_hash="0x" + "ab" * 32,
            block_number=50000000,
        )

        funding_tracer._get_transfer_logs = AsyncMock(return_value=[mock_log])

        result = await funding_tracer.trace(TEST_WALLET)

        assert result.target_address == TEST_WALLET.lower()
        assert result.origin_address == BINANCE_HOT_WALLET.lower()
        assert result.origin_type == EntityType.CEX_BINANCE.value
        assert result.hop_count == 1
        assert len(result.chain) == 1

    @pytest.mark.asyncio
    async def test_trace_multiple_hops(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test trace follows multiple hops."""
        intermediate_wallet = "0x" + "11" * 20

        # First call: TEST_WALLET received from intermediate
        # Second call: intermediate received from Binance
        mock_logs = [
            _create_mock_log(
                from_address=intermediate_wallet,
                to_address=TEST_WALLET,
                amount=1000000,
                tx_hash="0x" + "aa" * 32,
                block_number=50000001,
            ),
            _create_mock_log(
                from_address=BINANCE_HOT_WALLET,
                to_address=intermediate_wallet,
                amount=1000000,
                tx_hash="0x" + "bb" * 32,
                block_number=50000000,
            ),
        ]

        call_count = 0

        async def mock_get_logs(
            *_args: Any, **_kwargs: Any
        ) -> list[dict[str, Any]]:
            nonlocal call_count
            result = [mock_logs[call_count]] if call_count < len(mock_logs) else []
            call_count += 1
            return result

        funding_tracer._get_transfer_logs = mock_get_logs

        result = await funding_tracer.trace(TEST_WALLET)

        assert result.hop_count == 2
        assert result.origin_address == BINANCE_HOT_WALLET.lower()
        assert result.origin_type == EntityType.CEX_BINANCE.value

    @pytest.mark.asyncio
    async def test_trace_respects_max_hops(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test trace stops at max_hops."""
        # Create a chain of unknown wallets
        wallets = [f"0x{i:040x}" for i in range(10)]

        call_count = 0

        async def mock_get_logs(
            *_args: Any, **_kwargs: Any
        ) -> list[dict[str, Any]]:
            nonlocal call_count
            if call_count < len(wallets) - 1:
                log = _create_mock_log(
                    from_address=wallets[call_count + 1],
                    to_address=wallets[call_count],
                    amount=1000000,
                    tx_hash=f"0x{call_count:064x}",
                    block_number=50000000 + call_count,
                )
                call_count += 1
                return [log]
            return []

        funding_tracer._get_transfer_logs = mock_get_logs

        result = await funding_tracer.trace(wallets[0], max_hops=3)

        assert result.hop_count == 3
        assert result.origin_type == "unknown"

    @pytest.mark.asyncio
    async def test_trace_override_max_hops(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test trace can override default max_hops."""
        funding_tracer._get_transfer_logs = AsyncMock(return_value=[])

        # Override to 1 hop
        await funding_tracer.trace(TEST_WALLET, max_hops=1)

        # Verify only 1 iteration (no hops since no transfers found)
        # The trace should have been called once for the target wallet


class TestGetFirstUsdcTransfer:
    """Tests for get_first_usdc_transfer method."""

    @pytest.mark.asyncio
    async def test_get_first_usdc_transfer_bridged(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test getting first USDC transfer from bridged contract."""
        mock_log = _create_mock_log(
            from_address=TEST_SOURCE,
            to_address=TEST_WALLET,
            amount=5000000,
            tx_hash="0x" + "cc" * 32,
            block_number=50000000,
        )

        funding_tracer._get_transfer_logs = AsyncMock(return_value=[mock_log])

        result = await funding_tracer.get_first_usdc_transfer(TEST_WALLET)

        assert result is not None
        assert result.from_address == TEST_SOURCE.lower()
        assert result.to_address == TEST_WALLET.lower()
        assert result.amount == Decimal(5000000)
        assert result.token == "USDC"

    @pytest.mark.asyncio
    async def test_get_first_usdc_transfer_native(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test fallback to native USDC contract."""
        call_count = 0

        async def mock_get_logs(
            *_args: Any, **_kwargs: Any
        ) -> list[dict[str, Any]]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # First call (bridged) returns nothing
                return []
            # Second call (native) returns a transfer
            return [
                _create_mock_log(
                    from_address=TEST_SOURCE,
                    to_address=TEST_WALLET,
                    amount=1000000,
                    tx_hash="0x" + "dd" * 32,
                    block_number=50000000,
                )
            ]

        funding_tracer._get_transfer_logs = mock_get_logs

        result = await funding_tracer.get_first_usdc_transfer(TEST_WALLET)

        assert result is not None
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_get_first_usdc_transfer_none_found(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test returns None when no USDC transfers found."""
        funding_tracer._get_transfer_logs = AsyncMock(return_value=[])

        result = await funding_tracer.get_first_usdc_transfer(TEST_WALLET)

        assert result is None


class TestGetTransferLogs:
    """Tests for _get_transfer_logs method."""

    @pytest.mark.asyncio
    async def test_get_transfer_logs_formats_topics_correctly(
        self,
        funding_tracer: FundingTracer,
        mock_polygon_client: MagicMock,
    ) -> None:
        """Test that transfer logs query is formatted correctly."""
        mock_w3 = MagicMock()
        mock_w3.eth.get_logs = AsyncMock(return_value=[])
        mock_polygon_client._w3 = mock_w3

        await funding_tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
        )

        mock_w3.eth.get_logs.assert_called_once()
        call_args = mock_w3.eth.get_logs.call_args[0][0]

        # Verify topics structure
        assert len(call_args["topics"]) == 3
        assert call_args["topics"][0] == TRANSFER_EVENT_SIGNATURE.hex()
        assert call_args["topics"][1] is None  # from (any)
        # to address should be padded to 32 bytes
        assert call_args["topics"][2].endswith(TEST_WALLET.lower().replace("0x", ""))

    @pytest.mark.asyncio
    async def test_get_transfer_logs_respects_limit(
        self,
        funding_tracer: FundingTracer,
        mock_polygon_client: MagicMock,
    ) -> None:
        """Test that limit parameter works correctly."""
        mock_logs = [MagicMock() for _ in range(10)]
        mock_w3 = MagicMock()
        mock_w3.eth.get_logs = AsyncMock(return_value=mock_logs)
        mock_polygon_client._w3 = mock_w3

        result = await funding_tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
            limit=3,
        )

        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_get_transfer_logs_uses_fallback_when_primary_unhealthy(
        self,
        funding_tracer: FundingTracer,
        mock_polygon_client: MagicMock,
    ) -> None:
        """Test fallback RPC is used when primary is unhealthy."""
        mock_polygon_client._primary_healthy = False
        mock_fallback = MagicMock()
        mock_fallback.eth.get_logs = AsyncMock(return_value=[])
        mock_polygon_client._w3_fallback = mock_fallback

        await funding_tracer._get_transfer_logs(
            to_address=TEST_WALLET,
            token_address=USDC_BRIDGED,
        )

        mock_fallback.eth.get_logs.assert_called_once()


class TestLogToFundingTransfer:
    """Tests for _log_to_funding_transfer method."""

    @pytest.mark.asyncio
    async def test_log_to_funding_transfer_parses_correctly(
        self,
        funding_tracer: FundingTracer,
    ) -> None:
        """Test correct parsing of log to FundingTransfer."""
        mock_log = _create_mock_log(
            from_address=TEST_SOURCE,
            to_address=TEST_WALLET,
            amount=1500000,
            tx_hash="0x" + "ee" * 32,
            block_number=50000000,
        )

        result = await funding_tracer._log_to_funding_transfer(
            mock_log, USDC_BRIDGED
        )

        assert result.from_address == TEST_SOURCE.lower()
        assert result.to_address == TEST_WALLET.lower()
        assert result.amount == Decimal(1500000)
        assert result.token == "USDC"
        assert result.tx_hash == "ee" * 32
        assert result.block_number == 50000000

    @pytest.mark.asyncio
    async def test_log_to_funding_transfer_handles_block_error(
        self,
        funding_tracer: FundingTracer,
        mock_polygon_client: MagicMock,
    ) -> None:
        """Test graceful handling when block fetch fails."""
        mock_polygon_client.get_block = AsyncMock(side_effect=Exception("Block error"))

        mock_log = _create_mock_log(
            from_address=TEST_SOURCE,
            to_address=TEST_WALLET,
            amount=1000000,
            tx_hash="0x" + "ff" * 32,
            block_number=50000000,
        )

        result = await funding_tracer._log_to_funding_transfer(
            mock_log, USDC_BRIDGED
        )

        # Should still return a valid transfer with current timestamp
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

        # Mock trace to return simple chains
        async def mock_trace(
            addr: str, *, max_hops: int | None = None  # noqa: ARG001
        ) -> FundingChain:
            return FundingChain(
                target_address=addr.lower(),
                origin_type="unknown",
            )

        funding_tracer.trace = mock_trace

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

        async def mock_trace(
            addr: str, *, max_hops: int | None = None  # noqa: ARG001
        ) -> FundingChain:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Test error")
            return FundingChain(
                target_address=addr.lower(),
                origin_type="cex_binance",
            )

        funding_tracer.trace = mock_trace

        results = await funding_tracer.get_funding_chains_batch(addresses)

        assert len(results) == 2
        # First address should have error origin type
        assert results[addresses[0].lower()].origin_type == "error"
        # Second address should succeed
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

        async def mock_trace(
            addr: str, max_hops: int | None = None
        ) -> FundingChain:
            captured_max_hops.append(max_hops)
            return FundingChain(target_address=addr.lower())

        funding_tracer.trace = mock_trace

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

    def test_unknown_no_transfers_high_score(
        self, funding_tracer: FundingTracer
    ) -> None:
        """Test unknown origin with no transfers is most suspicious."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="unknown",
            hop_count=0,
        )

        score = funding_tracer.get_suspiciousness_score(chain)

        assert score == 1.0

    def test_unknown_max_hops_high_score(
        self, funding_tracer: FundingTracer
    ) -> None:
        """Test unknown origin at max hops is suspicious."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="unknown",
            hop_count=3,  # Same as max_hops
        )

        score = funding_tracer.get_suspiciousness_score(chain)

        assert score == 0.7

    def test_unknown_partial_hops_medium_score(
        self, funding_tracer: FundingTracer
    ) -> None:
        """Test unknown origin with partial hops is moderately suspicious."""
        chain = FundingChain(
            target_address=TEST_WALLET,
            origin_type="unknown",
            hop_count=1,
        )

        score = funding_tracer.get_suspiciousness_score(chain)

        # 0.5 + (0.3 * (1 - 1/3)) = 0.5 + 0.2 = 0.7
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
        # Transfer(address,address,uint256) hash
        assert TRANSFER_EVENT_SIGNATURE is not None
        assert len(TRANSFER_EVENT_SIGNATURE) == 32


# Helper functions


def _create_mock_log(
    from_address: str,
    to_address: str,
    amount: int,
    tx_hash: str,
    block_number: int,
) -> dict[str, Any]:
    """Create a mock log entry for testing."""
    # Pad addresses to 32 bytes (topics format)
    from_padded = bytes.fromhex(from_address.replace("0x", "").zfill(64))
    to_padded = bytes.fromhex(to_address.replace("0x", "").zfill(64))

    # Amount as 32-byte hex data
    amount_hex = bytes.fromhex(f"{amount:064x}")

    return {
        "topics": [
            TRANSFER_EVENT_SIGNATURE,
            from_padded,
            to_padded,
        ],
        "data": amount_hex,
        "transactionHash": bytes.fromhex(tx_hash.replace("0x", "")),
        "blockNumber": block_number,
    }
