"""Sniper cluster detection using DBSCAN clustering.

This module identifies wallets that exhibit coordinated "sniper" behavior -
consistently entering markets within minutes of their creation, suggesting
advance knowledge of market creation times.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import numpy as np
from sklearn.cluster import DBSCAN

from polymarket_insider_tracker.detector.models import SniperClusterSignal

if TYPE_CHECKING:
    from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)


@dataclass
class MarketEntry:
    """Record of a wallet's entry into a market.

    Attributes:
        wallet_address: The wallet that entered the market.
        market_id: The market condition ID.
        entry_delta_seconds: Time between market creation and wallet entry.
        position_size: Size of the initial position in USDC.
        timestamp: When the entry occurred.
    """

    wallet_address: str
    market_id: str
    entry_delta_seconds: float
    position_size: Decimal
    timestamp: datetime


@dataclass
class ClusterInfo:
    """Information about a detected sniper cluster.

    Attributes:
        cluster_id: Unique identifier for this cluster.
        wallet_addresses: Set of wallet addresses in the cluster.
        avg_entry_delta: Average entry delay in seconds across the cluster.
        markets_in_common: Number of markets where cluster members overlap.
        created_at: When this cluster was first detected.
    """

    cluster_id: str
    wallet_addresses: set[str]
    avg_entry_delta: float
    markets_in_common: int
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class SniperDetector:
    """Detects sniper clusters using DBSCAN clustering algorithm.

    The detector tracks wallet entries across markets and periodically
    runs DBSCAN clustering to identify groups of wallets with similar
    timing patterns (consistently entering markets early after creation).

    Attributes:
        entry_threshold_seconds: Maximum seconds after market creation to be
            considered a "sniper" entry (default 300 = 5 minutes).
        min_cluster_size: Minimum wallets to form a cluster (default 3).
        eps: DBSCAN epsilon parameter for neighborhood distance (default 0.5).
        min_samples: DBSCAN minimum samples for core point (default 2).
    """

    def __init__(
        self,
        *,
        entry_threshold_seconds: int = 300,
        min_cluster_size: int = 3,
        eps: float = 0.5,
        min_samples: int = 2,
        min_entries_per_wallet: int = 2,
    ) -> None:
        """Initialize the sniper detector.

        Args:
            entry_threshold_seconds: Max seconds for sniper entry (default 300).
            min_cluster_size: Minimum cluster size (default 3).
            eps: DBSCAN epsilon (default 0.5).
            min_samples: DBSCAN min samples (default 2).
            min_entries_per_wallet: Minimum entries to include wallet (default 2).
        """
        self.entry_threshold_seconds = entry_threshold_seconds
        self.min_cluster_size = min_cluster_size
        self.eps = eps
        self.min_samples = min_samples
        self.min_entries_per_wallet = min_entries_per_wallet

        # Entry tracking
        self._entries: list[MarketEntry] = []
        self._wallet_entries: dict[str, list[MarketEntry]] = defaultdict(list)
        self._market_wallets: dict[str, set[str]] = defaultdict(set)

        # Cluster tracking
        self._known_clusters: dict[str, ClusterInfo] = {}
        self._wallet_cluster_map: dict[str, str] = {}

        # Previously signaled wallets (to avoid duplicate signals)
        self._signaled_wallets: set[str] = set()

    def record_entry(
        self,
        trade: TradeEvent,
        market_created_at: datetime,
    ) -> None:
        """Record a market entry for clustering analysis.

        Only records entries that occur within the threshold time after
        market creation (potential sniper behavior).

        Args:
            trade: The trade event representing market entry.
            market_created_at: When the market was created.
        """
        # Calculate entry delta
        entry_time = trade.timestamp
        delta = (entry_time - market_created_at).total_seconds()

        # Only track entries within threshold (potential snipers)
        if delta < 0 or delta > self.entry_threshold_seconds:
            return

        entry = MarketEntry(
            wallet_address=trade.wallet_address.lower(),
            market_id=trade.market_id,
            entry_delta_seconds=delta,
            position_size=trade.notional_value,
            timestamp=entry_time,
        )

        self._entries.append(entry)
        self._wallet_entries[entry.wallet_address].append(entry)
        self._market_wallets[entry.market_id].add(entry.wallet_address)

        logger.debug(
            "Recorded sniper entry: wallet=%s market=%s delta=%.1fs",
            entry.wallet_address[:10],
            entry.market_id[:10],
            delta,
        )

    def run_clustering(self) -> list[SniperClusterSignal]:
        """Run DBSCAN clustering and return new sniper signals.

        Clusters wallets based on their entry timing patterns across markets.
        Returns signals only for newly identified cluster members.

        Returns:
            List of SniperClusterSignal for newly detected cluster members.
        """
        # Filter wallets with enough entries
        eligible_wallets = [
            wallet
            for wallet, entries in self._wallet_entries.items()
            if len(entries) >= self.min_entries_per_wallet
        ]

        if len(eligible_wallets) < self.min_cluster_size:
            logger.debug(
                "Not enough eligible wallets for clustering: %d < %d",
                len(eligible_wallets),
                self.min_cluster_size,
            )
            return []

        # Build feature matrix
        feature_vectors, wallet_index = self._build_feature_matrix(eligible_wallets)

        if len(feature_vectors) == 0:
            return []

        # Run DBSCAN
        clustering = DBSCAN(
            eps=self.eps,
            min_samples=self.min_samples,
            metric="euclidean",
        ).fit(feature_vectors)

        # Process clusters
        signals = self._process_clustering_results(
            clustering.labels_,
            wallet_index,
        )

        return signals

    def _build_feature_matrix(
        self,
        wallets: list[str],
    ) -> tuple[np.ndarray, dict[int, str]]:
        """Build feature matrix for DBSCAN clustering.

        Features per entry:
        - Normalized market hash (0-1 range)
        - Normalized entry delta (in hours, typically 0-0.083)
        - Log-normalized position size

        Args:
            wallets: List of wallet addresses to include.

        Returns:
            Tuple of (feature_matrix, wallet_index_map).
        """
        features = []
        wallet_index: dict[int, str] = {}
        row_idx = 0

        for wallet in wallets:
            entries = self._wallet_entries[wallet]
            for entry in entries:
                # Normalize market ID to 0-1 range
                market_hash = (
                    (
                        int(
                            hashlib.md5(  # noqa: S324
                                entry.market_id.encode()
                            ).hexdigest()[:8],
                            16,
                        )
                        % 1000
                    )
                    / 1000.0
                )

                # Normalize entry delta to hours (0-5 mins = 0-0.083 hours)
                delta_hours = entry.entry_delta_seconds / 3600.0

                # Log-normalize position size
                log_size = float(np.log10(max(float(entry.position_size), 1.0)))

                features.append([market_hash, delta_hours, log_size])
                wallet_index[row_idx] = wallet
                row_idx += 1

        return np.array(features), wallet_index

    def _process_clustering_results(
        self,
        labels: np.ndarray,
        wallet_index: dict[int, str],
    ) -> list[SniperClusterSignal]:
        """Process DBSCAN clustering results into signals.

        Args:
            labels: Cluster labels from DBSCAN (-1 = noise).
            wallet_index: Map from row index to wallet address.

        Returns:
            List of signals for newly detected cluster members.
        """
        # Group rows by cluster
        cluster_rows: dict[int, list[int]] = defaultdict(list)
        for row_idx, label in enumerate(labels):
            if label != -1:  # Skip noise
                cluster_rows[label].append(row_idx)

        signals: list[SniperClusterSignal] = []

        for _cluster_label, rows in cluster_rows.items():
            # Get unique wallets in this cluster
            cluster_wallets = {wallet_index[row] for row in rows}

            if len(cluster_wallets) < self.min_cluster_size:
                continue

            # Calculate cluster statistics
            cluster_stats = self._calculate_cluster_stats(cluster_wallets)

            # Generate or reuse cluster ID
            cluster_id = self._get_or_create_cluster_id(cluster_wallets)

            # Update cluster info
            self._known_clusters[cluster_id] = ClusterInfo(
                cluster_id=cluster_id,
                wallet_addresses=cluster_wallets,
                avg_entry_delta=cluster_stats["avg_delta"],
                markets_in_common=int(cluster_stats["markets_in_common"]),
            )

            # Update wallet-cluster mapping
            for wallet in cluster_wallets:
                self._wallet_cluster_map[wallet] = cluster_id

            # Generate signals for new cluster members
            for wallet in cluster_wallets:
                if wallet not in self._signaled_wallets:
                    confidence = self._calculate_confidence(
                        cluster_wallets,
                        cluster_stats,
                    )

                    signal = SniperClusterSignal(
                        wallet_address=wallet,
                        cluster_id=cluster_id,
                        cluster_size=len(cluster_wallets),
                        avg_entry_delta_seconds=cluster_stats["avg_delta"],
                        markets_in_common=int(cluster_stats["markets_in_common"]),
                        confidence=confidence,
                    )

                    signals.append(signal)
                    self._signaled_wallets.add(wallet)

                    logger.info(
                        "New sniper detected: wallet=%s cluster=%s confidence=%.2f",
                        wallet[:10],
                        cluster_id[:8],
                        confidence,
                    )

        return signals

    def _calculate_cluster_stats(
        self,
        cluster_wallets: set[str],
    ) -> dict[str, float | int]:
        """Calculate statistics for a cluster of wallets.

        Args:
            cluster_wallets: Set of wallet addresses in the cluster.

        Returns:
            Dict with avg_delta, markets_in_common statistics.
        """
        # Calculate average entry delta
        all_deltas: list[float] = []
        for wallet in cluster_wallets:
            for entry in self._wallet_entries[wallet]:
                all_deltas.append(entry.entry_delta_seconds)

        avg_delta = sum(all_deltas) / len(all_deltas) if all_deltas else 0.0

        # Calculate markets in common
        wallet_markets: list[set[str]] = []
        for wallet in cluster_wallets:
            markets = {e.market_id for e in self._wallet_entries[wallet]}
            wallet_markets.append(markets)

        if len(wallet_markets) >= 2:
            common_markets = set.intersection(*wallet_markets)
            markets_in_common = len(common_markets)
        else:
            markets_in_common = 0

        return {
            "avg_delta": avg_delta,
            "markets_in_common": markets_in_common,
        }

    def _get_or_create_cluster_id(self, wallets: set[str]) -> str:
        """Get existing cluster ID or create new one.

        Checks if majority of wallets belong to an existing cluster
        and returns that ID, otherwise creates a new ID.

        Args:
            wallets: Set of wallet addresses.

        Returns:
            Cluster ID string.
        """
        # Check if majority belongs to existing cluster
        existing_clusters: dict[str, int] = defaultdict(int)
        for wallet in wallets:
            if wallet in self._wallet_cluster_map:
                existing_clusters[self._wallet_cluster_map[wallet]] += 1

        if existing_clusters:
            best_cluster = max(existing_clusters, key=lambda k: existing_clusters[k])
            if existing_clusters[best_cluster] >= len(wallets) // 2:
                return best_cluster

        return str(uuid.uuid4())

    def _calculate_confidence(
        self,
        cluster_wallets: set[str],
        stats: dict[str, float | int],
    ) -> float:
        """Calculate confidence score for a cluster.

        Higher confidence when:
        - Larger cluster size
        - Lower average entry delta (faster entries)
        - More markets in common

        Args:
            cluster_wallets: Wallets in the cluster.
            stats: Cluster statistics dict.

        Returns:
            Confidence score from 0.0 to 1.0.
        """
        # Size factor: more wallets = higher confidence
        size_factor = min(1.0, len(cluster_wallets) / 10.0)

        # Speed factor: faster entries = higher confidence
        # 0 seconds = 1.0, 300 seconds = 0.0
        avg_delta = float(stats["avg_delta"])
        speed_factor = max(0.0, 1.0 - (avg_delta / self.entry_threshold_seconds))

        # Overlap factor: more markets in common = higher confidence
        markets_common = int(stats["markets_in_common"])
        overlap_factor = min(1.0, markets_common / 5.0)

        # Weighted combination
        confidence = 0.3 * size_factor + 0.4 * speed_factor + 0.3 * overlap_factor

        return round(min(1.0, confidence), 3)

    def is_sniper(self, wallet_address: str) -> bool:
        """Check if a wallet is in any known sniper cluster.

        Args:
            wallet_address: Wallet address to check.

        Returns:
            True if wallet is a known sniper.
        """
        return wallet_address.lower() in self._wallet_cluster_map

    def get_cluster_for_wallet(self, wallet_address: str) -> ClusterInfo | None:
        """Get cluster info for a wallet if it belongs to one.

        Args:
            wallet_address: Wallet address to look up.

        Returns:
            ClusterInfo if wallet is in a cluster, None otherwise.
        """
        cluster_id = self._wallet_cluster_map.get(wallet_address.lower())
        if cluster_id:
            return self._known_clusters.get(cluster_id)
        return None

    def get_entry_count(self) -> int:
        """Return the total number of tracked entries."""
        return len(self._entries)

    def get_wallet_count(self) -> int:
        """Return the number of unique wallets tracked."""
        return len(self._wallet_entries)

    def get_cluster_count(self) -> int:
        """Return the number of detected clusters."""
        return len(self._known_clusters)

    def clear_entries(self) -> None:
        """Clear all tracked entries (for periodic cleanup)."""
        self._entries.clear()
        self._wallet_entries.clear()
        self._market_wallets.clear()
        logger.info("Cleared all sniper detector entries")
