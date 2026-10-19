"""Funding chain tracer for wallet analysis.

This module provides the FundingTracer class for tracing the funding chain
of wallets to identify where their USDC/MATIC originated from.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from polymarket_insider_tracker.profiler.entities import EntityRegistry
from polymarket_insider_tracker.profiler.models import FundingChain, FundingTransfer
from polymarket_insider_tracker.redaction import redact_exception_message

from ._funding_rpc import (
    DEFAULT_CHUNK_SIZE_BLOCKS as DEFAULT_CHUNK_SIZE_BLOCKS,
)
from ._funding_rpc import (
    DEFAULT_MAX_LOOKBACK_BLOCKS as DEFAULT_MAX_LOOKBACK_BLOCKS,
)
from ._funding_rpc import (
    TRANSFER_EVENT_SIGNATURE as TRANSFER_EVENT_SIGNATURE,
)
from ._funding_rpc import (
    TRANSFER_EVENT_TOPIC as TRANSFER_EVENT_TOPIC,
)
from ._funding_rpc import (
    USDC_BRIDGED as USDC_BRIDGED,
)
from ._funding_rpc import (
    USDC_NATIVE as USDC_NATIVE,
)
from ._funding_rpc import (
    TransferLogScanner,
)
from ._funding_rpc import (
    is_pruned_history_error as _is_pruned_history_error,
)

if TYPE_CHECKING:
    from polymarket_insider_tracker.profiler.chain import PolygonClient

logger = logging.getLogger(__name__)

# Compatibility: keep the historical ``funding.<name>`` module identity so
# existing import paths, qualnames and pickle references keep resolving.
_is_pruned_history_error.__module__ = __name__
_is_pruned_history_error.__qualname__ = "_is_pruned_history_error"


class FundingTracer:
    """Traces funding chains to identify wallet funding sources.

    The tracer follows USDC transfers backwards from a target wallet
    to find where the funds originated, stopping at known entities
    (CEX hot wallets, bridges) or reaching the maximum hop count.

    Attributes:
        polygon_client: Client for Polygon blockchain queries.
        entity_registry: Registry of known blockchain entities.
        max_hops: Maximum number of hops to trace (default 3).
    """

    def __init__(
        self,
        polygon_client: PolygonClient,
        entity_registry: EntityRegistry | None = None,
        *,
        max_hops: int = 3,
        usdc_addresses: list[str] | None = None,
        chunk_size_blocks: int = DEFAULT_CHUNK_SIZE_BLOCKS,
        max_lookback_blocks: int = DEFAULT_MAX_LOOKBACK_BLOCKS,
    ) -> None:
        """Initialize the funding tracer.

        Args:
            polygon_client: Polygon blockchain client for queries.
            entity_registry: Registry for entity classification. Creates default if None.
            max_hops: Maximum hops to trace back (default 3).
            usdc_addresses: USDC contract addresses to track. Uses defaults if None.
            chunk_size_blocks: Block window size per eth_getLogs call. Public
                Polygon RPCs cap at 10_000 blocks; default leaves a safety margin.
            max_lookback_blocks: How far back to scan when caller passes
                ``from_block=0``. Default ~44 hours at 2s block time, which
                fits inside the pruned-history horizon of most public RPCs.
        """
        self.polygon_client = polygon_client
        self.entity_registry = entity_registry or EntityRegistry()
        self.max_hops = max_hops
        self._usdc_addresses = [
            addr.lower() for addr in (usdc_addresses or [USDC_BRIDGED, USDC_NATIVE])
        ]
        self._log_scanner = TransferLogScanner(
            polygon_client,
            chunk_size_blocks=chunk_size_blocks,
            max_lookback_blocks=max_lookback_blocks,
        )

    async def _execute_trace_hop(
        self, current_address: str, hop: int, current_origin_type: str
    ) -> tuple[FundingTransfer | None, str, str, bool]:
        """Execute one hop of tracing.

        Returns (transfer, origin_address, origin_type, should_stop).
        """
        if self.entity_registry.is_terminal(current_address):
            origin_type = self.entity_registry.classify(current_address).value
            logger.debug(
                "Trace terminated at known entity: %s (%s)",
                current_address,
                origin_type,
            )
            return None, current_address, origin_type, True

        transfer = await self.get_first_usdc_transfer(current_address)
        if transfer is None:
            logger.debug("No USDC transfer found for %s at hop %d", current_address, hop)
            return None, current_address, current_origin_type, True

        origin_address = transfer.from_address
        if self.entity_registry.is_terminal(origin_address):
            origin_type = self.entity_registry.classify(origin_address).value
            logger.debug("Trace found terminal entity: %s (%s)", origin_address, origin_type)
            return transfer, origin_address, origin_type, True

        return transfer, origin_address, current_origin_type, False

    async def _trace_loop(
        self, start_address: str, max_hops: int
    ) -> tuple[list[FundingTransfer], str, str]:
        chain: list[FundingTransfer] = []
        current = start_address
        origin = start_address
        origin_type = "unknown"

        for hop in range(max_hops):
            transfer, origin, origin_type, stop = await self._execute_trace_hop(
                current, hop, origin_type
            )
            if not transfer:
                break
            chain.append(transfer)
            current = transfer.from_address
            if stop:
                break

        return chain, origin, origin_type

    async def trace(
        self,
        address: str,
        max_hops: int | None = None,
    ) -> FundingChain:
        """Trace the funding chain for a wallet.

        Follows the first USDC transfer into the wallet, then recursively
        traces the source wallet until reaching a known entity or max hops.

        Args:
            address: Target wallet address to trace.
            max_hops: Override default max_hops for this trace.

        Returns:
            FundingChain with the complete trace result.
        """
        effective_max_hops = max_hops if max_hops is not None else self.max_hops
        normalized_address = address.lower()
        chain, origin_address, origin_type = await self._trace_loop(
            normalized_address, effective_max_hops
        )

        return FundingChain(
            target_address=normalized_address,
            chain=chain,
            origin_address=origin_address,
            origin_type=origin_type,
            hop_count=len(chain),
            traced_at=datetime.now(UTC),
        )

    async def get_first_usdc_transfer(
        self,
        address: str,
    ) -> FundingTransfer | None:
        """Get the first USDC transfer into a wallet.

        Queries the blockchain for ERC20 Transfer events to the target
        address for known USDC contracts.

        Args:
            address: Target wallet address.

        Returns:
            First FundingTransfer if found, None otherwise.
        """
        normalized = address.lower()

        # Query transfers for each USDC contract
        for usdc_address in self._usdc_addresses:
            transfer = await self._get_first_token_transfer(
                to_address=normalized,
                token_address=usdc_address,
            )
            if transfer is not None:
                return transfer

        return None

    async def _get_first_token_transfer(
        self,
        to_address: str,
        token_address: str,
    ) -> FundingTransfer | None:
        """Get the first ERC20 transfer to an address for a specific token.

        Args:
            to_address: Recipient wallet address.
            token_address: ERC20 token contract address.

        Returns:
            First FundingTransfer if found, None otherwise.
        """
        try:
            logs = await self._get_transfer_logs(
                to_address=to_address,
                token_address=token_address,
                limit=1,
            )
        except Exception as e:
            logger.warning(
                "Failed to get transfer logs for %s: %s",
                to_address,
                redact_exception_message(e),
            )
            return None

        if not logs:
            return None

        log = logs[0]
        return await self._log_to_funding_transfer(log, token_address)

    async def _get_transfer_logs(
        self,
        to_address: str,
        token_address: str,
        limit: int = 10,
        from_block: int | str = 0,
        to_block: int | str = "latest",
    ) -> list[dict[str, Any]]:
        """Get ERC20 Transfer event logs.

        Delegates to the shared provider-safe chunk walker; see
        :class:`polymarket_insider_tracker.profiler._funding_rpc.TransferLogScanner`.

        Args:
            to_address: Filter by recipient address.
            token_address: ERC20 token contract address.
            limit: Maximum logs to return.
            from_block: Starting block number (0 means
                ``latest - max_lookback_blocks``).
            to_block: Ending block number ("latest" resolves to current head).

        Returns:
            List of log dictionaries, oldest first, capped at ``limit``.
        """
        return await self._log_scanner.get_transfer_logs(
            to_address=to_address,
            token_address=token_address,
            limit=limit,
            from_block=from_block,
            to_block=to_block,
        )

    async def _log_to_funding_transfer(
        self,
        log: dict[str, Any],
        token_address: str,
    ) -> FundingTransfer:
        """Convert a log entry to a FundingTransfer.

        Args:
            log: Log dictionary from get_logs.
            token_address: Token contract address.

        Returns:
            FundingTransfer object.
        """
        # Extract addresses from topics (padded to 32 bytes)
        from_address = "0x" + log["topics"][1].hex()[-40:]
        to_address = "0x" + log["topics"][2].hex()[-40:]

        # Extract amount from data
        amount = int(log["data"].hex(), 16)

        # Get block timestamp
        block_number = log["blockNumber"]
        try:
            block = await self.polygon_client.get_block(block_number)
            timestamp = datetime.fromtimestamp(block["timestamp"], tz=UTC)
        except Exception:
            timestamp = datetime.now(UTC)

        # Determine token symbol
        token = "USDC" if token_address.lower() in self._usdc_addresses else "OTHER"

        return FundingTransfer(
            from_address=from_address.lower(),
            to_address=to_address.lower(),
            amount=Decimal(amount),
            token=token,
            tx_hash=log["transactionHash"].hex(),
            block_number=block_number,
            timestamp=timestamp,
        )

    async def get_funding_chains_batch(
        self,
        addresses: list[str],
        max_hops: int | None = None,
    ) -> dict[str, FundingChain]:
        """Trace funding chains for multiple addresses concurrently.

        Args:
            addresses: List of wallet addresses to trace.
            max_hops: Override default max_hops for all traces.

        Returns:
            Dictionary mapping address to FundingChain.
        """
        tasks = [self.trace(addr, max_hops=max_hops) for addr in addresses]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        chains: dict[str, FundingChain] = {}
        for addr, result in zip(addresses, results, strict=True):
            if isinstance(result, BaseException):
                logger.warning("Failed to trace %s: %s", addr, redact_exception_message(result))
                chains[addr.lower()] = FundingChain(
                    target_address=addr.lower(),
                    origin_type="error",
                )
            else:
                chains[addr.lower()] = result

        return chains

    def get_suspiciousness_score(self, chain: FundingChain) -> float:
        """Calculate a suspiciousness score based on funding chain.

        Higher scores indicate more suspicious funding patterns:
        - CEX origin: Lower suspicion (0.0-0.2)
        - Bridge origin: Low suspicion (0.2-0.4)
        - Unknown origin with few hops: High suspicion (0.8-1.0)
        - Unknown origin with many hops: Medium suspicion (0.5-0.8)

        Args:
            chain: Funding chain to score.

        Returns:
            Suspiciousness score from 0.0 to 1.0.
        """
        if chain.is_cex_origin:
            # CEX origin is least suspicious
            return 0.1

        if chain.is_bridge_origin:
            # Bridge origin is slightly more suspicious
            return 0.3

        # Unknown origin
        if chain.hop_count == 0:
            # No transfers found - very suspicious (possible contract or new wallet)
            return 1.0

        if chain.hop_count >= self.max_hops:
            # Max hops reached without finding known entity
            # More hops = more obfuscation = more suspicious
            return 0.7

        # Some hops but didn't reach max - moderately suspicious
        return 0.5 + (0.3 * (1 - chain.hop_count / self.max_hops))
