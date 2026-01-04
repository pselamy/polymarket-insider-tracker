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

from web3 import AsyncWeb3

from polymarket_insider_tracker.profiler.entities import EntityRegistry
from polymarket_insider_tracker.profiler.models import FundingChain, FundingTransfer

if TYPE_CHECKING:
    from polymarket_insider_tracker.profiler.chain import PolygonClient

logger = logging.getLogger(__name__)

# USDC contract addresses on Polygon
USDC_BRIDGED = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# ERC20 Transfer event signature
TRANSFER_EVENT_SIGNATURE = AsyncWeb3.keccak(text="Transfer(address,address,uint256)")


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
    ) -> None:
        """Initialize the funding tracer.

        Args:
            polygon_client: Polygon blockchain client for queries.
            entity_registry: Registry for entity classification. Creates default if None.
            max_hops: Maximum hops to trace back (default 3).
            usdc_addresses: USDC contract addresses to track. Uses defaults if None.
        """
        self.polygon_client = polygon_client
        self.entity_registry = entity_registry or EntityRegistry()
        self.max_hops = max_hops
        self._usdc_addresses = [
            addr.lower() for addr in (usdc_addresses or [USDC_BRIDGED, USDC_NATIVE])
        ]

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

        chain: list[FundingTransfer] = []
        current_address = normalized_address
        origin_address = normalized_address
        origin_type = "unknown"

        for hop in range(effective_max_hops):
            # Check if current address is a known entity
            if self.entity_registry.is_terminal(current_address):
                origin_address = current_address
                origin_type = self.entity_registry.classify(current_address).value
                logger.debug(
                    "Trace terminated at known entity: %s (%s)",
                    current_address,
                    origin_type,
                )
                break

            # Get first USDC transfer into this address
            transfer = await self.get_first_usdc_transfer(current_address)
            if transfer is None:
                logger.debug(
                    "No USDC transfer found for %s at hop %d",
                    current_address,
                    hop,
                )
                origin_address = current_address
                break

            chain.append(transfer)
            origin_address = transfer.from_address
            current_address = transfer.from_address

            # Check if the source is a known entity
            if self.entity_registry.is_terminal(origin_address):
                origin_type = self.entity_registry.classify(origin_address).value
                logger.debug(
                    "Trace found terminal entity: %s (%s)",
                    origin_address,
                    origin_type,
                )
                break

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
                e,
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

        Args:
            to_address: Filter by recipient address.
            token_address: ERC20 token contract address.
            limit: Maximum logs to return.
            from_block: Starting block number.
            to_block: Ending block number.

        Returns:
            List of log dictionaries.
        """
        # Pad address to 32 bytes for topic filter
        padded_to = "0x" + to_address.lower().replace("0x", "").zfill(64)

        await self.polygon_client._rate_limiter.acquire()

        # Use the web3 instance from polygon client
        w3 = (
            self.polygon_client._w3
            if self.polygon_client._primary_healthy
            else (self.polygon_client._w3_fallback or self.polygon_client._w3)
        )

        # Get logs with Transfer event filtering by recipient
        logs = await w3.eth.get_logs(
            {
                "address": AsyncWeb3.to_checksum_address(token_address),
                "topics": [
                    TRANSFER_EVENT_SIGNATURE.hex(),  # Transfer event
                    None,  # from (any)
                    padded_to,  # to (target address)
                ],
                "fromBlock": from_block,
                "toBlock": to_block,
            }
        )

        # Convert to list of dicts and limit
        result = [dict(log) for log in logs[:limit]]
        return result

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
            if isinstance(result, Exception):
                logger.warning("Failed to trace %s: %s", addr, result)
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
