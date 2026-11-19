"""Wallet profiler - Blockchain analysis for trader intelligence.

Includes on-chain Polygon wallet analysis and best-effort funding-chain enrichment
(FundingTracer), persisted for research context without affecting live risk scores,
signal counts, or alert wording.
"""

from polymarket_insider_tracker.profiler.analyzer import (
    WalletAnalyzer,
)
from polymarket_insider_tracker.profiler.chain import (
    PolygonClient,
    PolygonClientError,
    RateLimitError,
    RPCError,
)
from polymarket_insider_tracker.profiler.entities import (
    EntityRegistry,
)
from polymarket_insider_tracker.profiler.entity_data import (
    EntityType,
)
from polymarket_insider_tracker.profiler.funding import (
    FundingTracer,
)
from polymarket_insider_tracker.profiler.models import (
    FundingChain,
    FundingTransfer,
    Transaction,
    WalletInfo,
    WalletProfile,
)

__all__ = [
    # Analyzer
    "WalletAnalyzer",
    # Entity Registry
    "EntityRegistry",
    "EntityType",
    # Funding Tracer
    "FundingChain",
    "FundingTracer",
    "FundingTransfer",
    # Polygon Client
    "PolygonClient",
    "PolygonClientError",
    "RateLimitError",
    "RPCError",
    # Models
    "Transaction",
    "WalletInfo",
    "WalletProfile",
]
