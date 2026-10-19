"""Provider-safe Polygon log-scanning helpers for the funding tracer.

This module holds the low-level plumbing that turns a requested block range
into chunked ``eth_getLogs`` calls for
:mod:`polymarket_insider_tracker.profiler.funding`: the USDC contract and
Transfer-topic constants, the chunking/lookback defaults, the pruned-history
classifier, and :class:`TransferLogScanner`. It performs RPC I/O only through
the injected :class:`~polymarket_insider_tracker.profiler.chain.PolygonClient`
and holds no per-trace state of its own.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from web3 import AsyncWeb3

from polymarket_insider_tracker.redaction import redact_exception_message

if TYPE_CHECKING:
    from polymarket_insider_tracker.profiler.chain import PolygonClient
    from polymarket_insider_tracker.profiler.rpc_handle import Web3Handle

logger = logging.getLogger(__name__)

# USDC contract addresses on Polygon
USDC_BRIDGED = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# ERC20 Transfer event signature. ``HexBytes.hex()`` returns a *bare* hex
# string without the ``0x`` prefix; publicnode tolerates that, but stricter
# providers (e.g. drpc — which we use as the fallback) reject it with
# ``invalid argument 0: hex string without 0x prefix``. Always pass the
# 0x-prefixed form to ``eth_getLogs``.
TRANSFER_EVENT_SIGNATURE = AsyncWeb3.keccak(text="Transfer(address,address,uint256)")
TRANSFER_EVENT_TOPIC = "0x" + TRANSFER_EVENT_SIGNATURE.hex().removeprefix("0x")

# eth_getLogs block-range chunking. Most public Polygon RPCs (publicnode, ankr,
# llamarpc) cap the range at 10_000 blocks per call; pick a window slightly
# under the cap so off-by-one differences between providers don't trip us up.
DEFAULT_CHUNK_SIZE_BLOCKS = 9_000
# Polygon block time is ~2.0s. publicnode (the most common free RPC) prunes
# log history aggressively — empirically only ~100k blocks (~55 hours) are
# served before requests start returning "History has been pruned". We default
# to 80k blocks (~44 hours), which is more than enough for fresh-wallet
# funding traces (those wallets are by definition new) and fits comfortably
# inside what most public providers retain.
DEFAULT_MAX_LOOKBACK_BLOCKS = 80_000

# Substrings that, when present in an RPC error, indicate the chunk we just
# asked for is outside the provider's archive horizon. Walking further back
# is futile, so we stop the trace early instead of hammering every chunk.
_PRUNED_HISTORY_MARKERS: tuple[str, ...] = (
    "history has been pruned",
    "missing trie node",
    "older than",
)


def is_pruned_history_error(err: BaseException) -> bool:
    """Return True if the RPC error indicates pruned history.

    Public Polygon nodes only retain a recent slice of log history. When we
    walk back through that slice in chunks and hit the cutoff, every further
    chunk will fail with the same message — so we stop early instead of
    burning quota on guaranteed failures.
    """
    text = str(err).lower()
    return any(marker in text for marker in _PRUNED_HISTORY_MARKERS)


class TransferLogScanner:
    """Walk ``eth_getLogs`` over a block range in provider-safe chunks."""

    def __init__(
        self,
        polygon_client: PolygonClient,
        *,
        chunk_size_blocks: int = DEFAULT_CHUNK_SIZE_BLOCKS,
        max_lookback_blocks: int = DEFAULT_MAX_LOOKBACK_BLOCKS,
    ) -> None:
        self.polygon_client = polygon_client
        self._chunk_size_blocks = chunk_size_blocks
        self._max_lookback_blocks = max_lookback_blocks

    async def get_transfer_logs(
        self,
        to_address: str,
        token_address: str,
        limit: int = 10,
        from_block: int | str = 0,
        to_block: int | str = "latest",
    ) -> list[dict[str, Any]]:
        """Get ERC20 Transfer event logs.

        Public Polygon RPCs (publicnode, ankr, llamarpc) cap ``eth_getLogs`` at
        10_000 blocks per call. To work around this we resolve the requested
        range into a concrete block window (defaulting to the last
        ``max_lookback_blocks`` when caller passes ``from_block=0``) and walk
        the window in chunks of ``chunk_size_blocks``, oldest-first, stopping
        once ``limit`` matches are collected. Walking oldest-first preserves
        the "first transfer" semantics expected by the funding chain tracer.

        If a chunk comes back with a "history has been pruned" style error
        the rest of the walk is short-circuited — every subsequent chunk
        would hit the same archive cutoff and there's no point burning quota
        on guaranteed failures.

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
        # Pad address to 32 bytes for topic filter
        padded_to = "0x" + to_address.lower().replace("0x", "").zfill(64)
        topics = [
            TRANSFER_EVENT_TOPIC,  # Transfer event (must be 0x-prefixed for drpc)
            None,  # from (any)
            padded_to,  # to (target address)
        ]
        contract_address = AsyncWeb3.to_checksum_address(token_address)

        start_block, end_block = await self._resolve_block_range(from_block, to_block)
        if start_block > end_block:
            return []

        return await self._scan_log_chunks(
            contract_address, topics, start_block, end_block, to_address, limit
        )

    async def _fetch_chunk_with_handling(
        self,
        contract_address: str,
        topics: list[Any],
        chunk_start: int,
        chunk_end: int,
        to_address: str,
    ) -> tuple[list[Any] | None, bool]:
        """Fetch logs for a chunk, returning (logs, should_stop)."""
        try:
            logs = await self._fetch_logs_chunk(
                contract_address=contract_address,
                topics=topics,
                from_block=chunk_start,
                to_block=chunk_end,
            )
            return logs, False
        except Exception as e:
            if is_pruned_history_error(e):
                logger.info(
                    "eth_getLogs chunk %d-%d outside archive horizon for %s; stopping trace",
                    chunk_start,
                    chunk_end,
                    to_address,
                )
                return None, True
            logger.warning(
                "eth_getLogs chunk %d-%d failed for %s: %s",
                chunk_start,
                chunk_end,
                to_address,
                redact_exception_message(e),
            )
            return None, False

    def _append_chunk_logs(
        self, results: list[dict[str, Any]], chunk_logs: list[Any] | None, limit: int
    ) -> bool:
        if not chunk_logs:
            return False
        for log in chunk_logs:
            results.append(dict(log))
            if len(results) >= limit:
                return True
        return False

    async def _scan_log_chunks(
        self,
        contract_address: str,
        topics: list[Any],
        start_block: int,
        end_block: int,
        to_address: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        chunk_size = max(1, self._chunk_size_blocks)
        chunk_start = start_block

        while chunk_start <= end_block:
            chunk_end = min(chunk_start + chunk_size - 1, end_block)
            chunk_logs, should_stop = await self._fetch_chunk_with_handling(
                contract_address, topics, chunk_start, chunk_end, to_address
            )
            if should_stop or self._append_chunk_logs(results, chunk_logs, limit):
                break
            chunk_start = chunk_end + 1

        return results

    async def _resolve_block_range(
        self,
        from_block: int | str,
        to_block: int | str,
    ) -> tuple[int, int]:
        """Resolve symbolic block params to concrete numeric bounds.

        ``from_block=0`` (the historical default) is rewritten to
        ``latest - max_lookback_blocks`` so we don't try to scan all of Polygon.
        """
        w3 = self._select_w3()

        if isinstance(to_block, str):
            await self.polygon_client.acquire_rate_limit()
            head = int(await w3.eth.block_number)
            end = head
        else:
            end = int(to_block)

        if isinstance(from_block, str):
            # Treat any symbolic from-block (e.g. "earliest") as "go back
            # max_lookback_blocks from end"; that's what callers actually want.
            start = max(0, end - self._max_lookback_blocks)
        elif from_block == 0:
            start = max(0, end - self._max_lookback_blocks)
        else:
            start = int(from_block)

        return start, end

    async def _fetch_logs_chunk(
        self,
        contract_address: str,
        topics: list[Any],
        from_block: int,
        to_block: int,
    ) -> list[Any]:
        """Issue a single bounded ``eth_getLogs`` call."""
        await self.polygon_client.acquire_rate_limit()
        w3 = self._select_w3()
        # Note: web3 typing is overly restrictive for block params
        return await w3.eth.get_logs(
            {
                "address": AsyncWeb3.to_checksum_address(contract_address),
                "topics": topics,
                "fromBlock": from_block,
                "toBlock": to_block,
            }
        )

    def _select_w3(self) -> Web3Handle:
        """Pick primary or fallback web3 instance based on health."""
        return self.polygon_client.select_web3()
