"""Known entity registry for blockchain address classification.

This module provides the EntityRegistry class for classifying blockchain
addresses as known entities (CEX hot wallets, bridges, DEX contracts, etc.)
to support funding chain analysis and suspiciousness scoring.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from polymarket_insider_tracker.profiler.entity_data import (
    EntityType,
    get_all_known_entities,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class EntityRegistry:
    """Registry of known blockchain entities for address classification.

    The registry contains mappings from blockchain addresses to known entity
    types (CEX, bridges, DEX, etc.). This is used to:
    - Terminate funding chain traces at known entities
    - Classify funding sources for suspiciousness scoring
    - Identify retail vs sophisticated wallet patterns

    Attributes:
        _entities: Internal mapping of address to entity type.
    """

    # Entity types that should terminate funding chain traces
    TERMINAL_ENTITY_TYPES = frozenset(
        [
            EntityType.CEX_BINANCE,
            EntityType.CEX_COINBASE,
            EntityType.CEX_KRAKEN,
            EntityType.CEX_OKX,
            EntityType.CEX_KUCOIN,
            EntityType.CEX_BYBIT,
            EntityType.CEX_CRYPTO_COM,
            EntityType.CEX_OTHER,
            EntityType.BRIDGE_POLYGON,
            EntityType.BRIDGE_MULTICHAIN,
            EntityType.BRIDGE_STARGATE,
            EntityType.BRIDGE_HOP,
            EntityType.BRIDGE_OTHER,
        ]
    )

    # Entity types that indicate CEX origin
    CEX_ENTITY_TYPES = frozenset(
        [
            EntityType.CEX_BINANCE,
            EntityType.CEX_COINBASE,
            EntityType.CEX_KRAKEN,
            EntityType.CEX_OKX,
            EntityType.CEX_KUCOIN,
            EntityType.CEX_BYBIT,
            EntityType.CEX_CRYPTO_COM,
            EntityType.CEX_OTHER,
        ]
    )

    # Entity types that indicate bridge origin
    BRIDGE_ENTITY_TYPES = frozenset(
        [
            EntityType.BRIDGE_POLYGON,
            EntityType.BRIDGE_MULTICHAIN,
            EntityType.BRIDGE_STARGATE,
            EntityType.BRIDGE_HOP,
            EntityType.BRIDGE_OTHER,
        ]
    )

    # Entity types for DEX contracts
    DEX_ENTITY_TYPES = frozenset(
        [
            EntityType.DEX_UNISWAP,
            EntityType.DEX_SUSHISWAP,
            EntityType.DEX_QUICKSWAP,
            EntityType.DEX_1INCH,
            EntityType.DEX_OTHER,
        ]
    )

    def __init__(
        self,
        custom_entities: dict[str, EntityType] | None = None,
        *,
        include_defaults: bool = True,
    ) -> None:
        """Initialize the entity registry.

        Args:
            custom_entities: Additional custom entity mappings to include.
            include_defaults: Whether to include default known entities.
        """
        self._entities: dict[str, EntityType] = {}

        if include_defaults:
            self._entities.update(get_all_known_entities())

        if custom_entities:
            # Add custom entities (normalized to lowercase)
            for address, entity_type in custom_entities.items():
                self._entities[address.lower()] = entity_type

        logger.info(f"EntityRegistry initialized with {len(self._entities)} known entities")

    def classify(self, address: str) -> EntityType:
        """Classify an address by its entity type.

        Args:
            address: The blockchain address to classify.

        Returns:
            The EntityType for the address, or UNKNOWN if not in registry.
        """
        return self._entities.get(address.lower(), EntityType.UNKNOWN)

    def is_known_entity(self, address: str) -> bool:
        """Check if an address is a known entity.

        Args:
            address: The blockchain address to check.

        Returns:
            True if the address is in the registry, False otherwise.
        """
        return address.lower() in self._entities

    def is_cex(self, address: str) -> bool:
        """Check if an address is a known CEX hot wallet.

        Args:
            address: The blockchain address to check.

        Returns:
            True if the address is a CEX hot wallet.
        """
        return self.classify(address) in self.CEX_ENTITY_TYPES

    def is_bridge(self, address: str) -> bool:
        """Check if an address is a known bridge contract.

        Args:
            address: The blockchain address to check.

        Returns:
            True if the address is a bridge contract.
        """
        return self.classify(address) in self.BRIDGE_ENTITY_TYPES

    def is_dex(self, address: str) -> bool:
        """Check if an address is a known DEX contract.

        Args:
            address: The blockchain address to check.

        Returns:
            True if the address is a DEX contract.
        """
        return self.classify(address) in self.DEX_ENTITY_TYPES

    def is_terminal(self, address: str) -> bool:
        """Check if an address should terminate a funding chain trace.

        Terminal entities are those where tracing further back provides
        diminishing returns (CEX, bridges). These indicate the practical
        origin of funds from the perspective of on-chain analysis.

        Args:
            address: The blockchain address to check.

        Returns:
            True if the address should terminate a funding trace.
        """
        return self.classify(address) in self.TERMINAL_ENTITY_TYPES

    def is_contract(self, address: str) -> bool:
        """Check if an address is a known smart contract.

        This includes DEX routers, token contracts, and DeFi protocols.

        Args:
            address: The blockchain address to check.

        Returns:
            True if the address is a known smart contract.
        """
        entity_type = self.classify(address)
        contract_types = self.DEX_ENTITY_TYPES | {
            EntityType.TOKEN_USDC,
            EntityType.TOKEN_USDT,
            EntityType.TOKEN_WETH,
            EntityType.TOKEN_WMATIC,
            EntityType.DEFI_AAVE,
            EntityType.DEFI_COMPOUND,
            EntityType.DEFI_OTHER,
            EntityType.CONTRACT,
        }
        return entity_type in contract_types

    def get_entity_category(self, address: str) -> str:
        """Get a human-readable category for an address.

        Args:
            address: The blockchain address to categorize.

        Returns:
            Category string: "cex", "bridge", "dex", "token", "defi", "contract", or "unknown".
        """
        entity_type = self.classify(address)

        if entity_type in self.CEX_ENTITY_TYPES:
            return "cex"
        if entity_type in self.BRIDGE_ENTITY_TYPES:
            return "bridge"
        if entity_type in self.DEX_ENTITY_TYPES:
            return "dex"
        if entity_type in {
            EntityType.TOKEN_USDC,
            EntityType.TOKEN_USDT,
            EntityType.TOKEN_WETH,
            EntityType.TOKEN_WMATIC,
        }:
            return "token"
        if entity_type in {
            EntityType.DEFI_AAVE,
            EntityType.DEFI_COMPOUND,
            EntityType.DEFI_OTHER,
        }:
            return "defi"
        if entity_type == EntityType.CONTRACT:
            return "contract"

        return "unknown"

    def add_entity(self, address: str, entity_type: EntityType) -> None:
        """Add or update an entity in the registry.

        Args:
            address: The blockchain address.
            entity_type: The entity type to assign.
        """
        self._entities[address.lower()] = entity_type
        logger.debug(f"Added entity: {address} -> {entity_type.value}")

    def remove_entity(self, address: str) -> bool:
        """Remove an entity from the registry.

        Args:
            address: The blockchain address to remove.

        Returns:
            True if the entity was removed, False if not found.
        """
        normalized = address.lower()
        if normalized in self._entities:
            del self._entities[normalized]
            return True
        return False

    def __len__(self) -> int:
        """Return the number of entities in the registry."""
        return len(self._entities)

    def __contains__(self, address: str) -> bool:
        """Check if an address is in the registry."""
        return self.is_known_entity(address)
