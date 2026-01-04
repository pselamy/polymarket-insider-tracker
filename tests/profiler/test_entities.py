"""Tests for known entity registry."""

import pytest

from polymarket_insider_tracker.profiler.entities import EntityRegistry
from polymarket_insider_tracker.profiler.entity_data import (
    BRIDGE_ADDRESSES,
    CEX_ADDRESSES,
    DEFI_ADDRESSES,
    DEX_ADDRESSES,
    TOKEN_ADDRESSES,
    EntityType,
    get_all_known_entities,
)

# ============================================================================
# EntityType Tests
# ============================================================================


class TestEntityType:
    """Tests for EntityType enum."""

    def test_cex_types_exist(self) -> None:
        """Test that CEX entity types are defined."""
        assert EntityType.CEX_BINANCE.value == "cex_binance"
        assert EntityType.CEX_COINBASE.value == "cex_coinbase"
        assert EntityType.CEX_OTHER.value == "cex_other"

    def test_bridge_types_exist(self) -> None:
        """Test that bridge entity types are defined."""
        assert EntityType.BRIDGE_POLYGON.value == "bridge_polygon"
        assert EntityType.BRIDGE_MULTICHAIN.value == "bridge_multichain"

    def test_dex_types_exist(self) -> None:
        """Test that DEX entity types are defined."""
        assert EntityType.DEX_UNISWAP.value == "dex_uniswap"
        assert EntityType.DEX_SUSHISWAP.value == "dex_sushiswap"

    def test_token_types_exist(self) -> None:
        """Test that token entity types are defined."""
        assert EntityType.TOKEN_USDC.value == "token_usdc"
        assert EntityType.TOKEN_WETH.value == "token_weth"

    def test_unknown_type(self) -> None:
        """Test unknown entity type."""
        assert EntityType.UNKNOWN.value == "unknown"


# ============================================================================
# Entity Data Tests
# ============================================================================


class TestEntityData:
    """Tests for entity data mappings."""

    def test_cex_addresses_populated(self) -> None:
        """Test that CEX addresses are populated."""
        assert len(CEX_ADDRESSES) > 0
        # Check Binance address is present
        binance_found = any(entity == EntityType.CEX_BINANCE for entity in CEX_ADDRESSES.values())
        assert binance_found

    def test_bridge_addresses_populated(self) -> None:
        """Test that bridge addresses are populated."""
        assert len(BRIDGE_ADDRESSES) > 0

    def test_dex_addresses_populated(self) -> None:
        """Test that DEX addresses are populated."""
        assert len(DEX_ADDRESSES) > 0
        # Check Uniswap is present
        uniswap_found = any(entity == EntityType.DEX_UNISWAP for entity in DEX_ADDRESSES.values())
        assert uniswap_found

    def test_token_addresses_include_usdc(self) -> None:
        """Test that USDC address is in token addresses."""
        usdc_address = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        assert usdc_address in TOKEN_ADDRESSES
        assert TOKEN_ADDRESSES[usdc_address] == EntityType.TOKEN_USDC

    def test_get_all_known_entities(self) -> None:
        """Test combining all entity mappings."""
        all_entities = get_all_known_entities()
        total_expected = (
            len(CEX_ADDRESSES)
            + len(BRIDGE_ADDRESSES)
            + len(DEX_ADDRESSES)
            + len(TOKEN_ADDRESSES)
            + len(DEFI_ADDRESSES)
        )
        assert len(all_entities) == total_expected

    def test_addresses_are_lowercase(self) -> None:
        """Test that all addresses in get_all_known_entities are lowercase."""
        all_entities = get_all_known_entities()
        for address in all_entities:
            assert address == address.lower()


# ============================================================================
# EntityRegistry Tests
# ============================================================================


class TestEntityRegistryInit:
    """Tests for EntityRegistry initialization."""

    def test_default_initialization(self) -> None:
        """Test registry initializes with default entities."""
        registry = EntityRegistry()
        assert len(registry) > 0

    def test_without_defaults(self) -> None:
        """Test registry without default entities."""
        registry = EntityRegistry(include_defaults=False)
        assert len(registry) == 0

    def test_with_custom_entities(self) -> None:
        """Test registry with custom entities."""
        custom = {"0x1234": EntityType.CEX_OTHER}
        registry = EntityRegistry(custom_entities=custom, include_defaults=False)
        assert len(registry) == 1
        assert registry.classify("0x1234") == EntityType.CEX_OTHER

    def test_custom_entities_override_defaults(self) -> None:
        """Test that custom entities can override defaults."""
        # USDC address is in defaults as TOKEN_USDC
        usdc_address = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        custom = {usdc_address: EntityType.CONTRACT}
        registry = EntityRegistry(custom_entities=custom)
        assert registry.classify(usdc_address) == EntityType.CONTRACT


class TestEntityRegistryClassify:
    """Tests for EntityRegistry.classify method."""

    @pytest.fixture
    def registry(self) -> EntityRegistry:
        """Create a registry for testing."""
        return EntityRegistry()

    def test_classify_known_cex(self, registry: EntityRegistry) -> None:
        """Test classifying a known CEX address."""
        # Binance address
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert registry.classify(binance) == EntityType.CEX_BINANCE

    def test_classify_case_insensitive(self, registry: EntityRegistry) -> None:
        """Test that classification is case-insensitive."""
        binance_lower = "0x28c6c06298d514db089934071355e5743bf21d60"
        binance_mixed = "0x28C6c06298D514db089934071355E5743bf21d60"
        assert registry.classify(binance_lower) == registry.classify(binance_mixed)

    def test_classify_unknown(self, registry: EntityRegistry) -> None:
        """Test classifying an unknown address."""
        unknown = "0x0000000000000000000000000000000000000000"
        assert registry.classify(unknown) == EntityType.UNKNOWN

    def test_classify_usdc(self, registry: EntityRegistry) -> None:
        """Test classifying USDC token contract."""
        usdc = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        assert registry.classify(usdc) == EntityType.TOKEN_USDC


class TestEntityRegistryChecks:
    """Tests for EntityRegistry type check methods."""

    @pytest.fixture
    def registry(self) -> EntityRegistry:
        """Create a registry for testing."""
        return EntityRegistry()

    def test_is_known_entity_true(self, registry: EntityRegistry) -> None:
        """Test is_known_entity returns True for known addresses."""
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert registry.is_known_entity(binance) is True

    def test_is_known_entity_false(self, registry: EntityRegistry) -> None:
        """Test is_known_entity returns False for unknown addresses."""
        unknown = "0x0000000000000000000000000000000000000000"
        assert registry.is_known_entity(unknown) is False

    def test_is_cex_true(self, registry: EntityRegistry) -> None:
        """Test is_cex returns True for CEX addresses."""
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert registry.is_cex(binance) is True

    def test_is_cex_false(self, registry: EntityRegistry) -> None:
        """Test is_cex returns False for non-CEX addresses."""
        usdc = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        assert registry.is_cex(usdc) is False

    def test_is_bridge_true(self, registry: EntityRegistry) -> None:
        """Test is_bridge returns True for bridge addresses."""
        polygon_bridge = "0xa0c68c638235ee32657e8f720a23cec1bfc77c77"
        assert registry.is_bridge(polygon_bridge) is True

    def test_is_bridge_false(self, registry: EntityRegistry) -> None:
        """Test is_bridge returns False for non-bridge addresses."""
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert registry.is_bridge(binance) is False

    def test_is_dex_true(self, registry: EntityRegistry) -> None:
        """Test is_dex returns True for DEX addresses."""
        uniswap = "0xe592427a0aece92de3edee1f18e0157c05861564"
        assert registry.is_dex(uniswap) is True

    def test_is_dex_false(self, registry: EntityRegistry) -> None:
        """Test is_dex returns False for non-DEX addresses."""
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert registry.is_dex(binance) is False


class TestEntityRegistryTerminal:
    """Tests for EntityRegistry.is_terminal method."""

    @pytest.fixture
    def registry(self) -> EntityRegistry:
        """Create a registry for testing."""
        return EntityRegistry()

    def test_cex_is_terminal(self, registry: EntityRegistry) -> None:
        """Test that CEX addresses are terminal."""
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert registry.is_terminal(binance) is True

    def test_bridge_is_terminal(self, registry: EntityRegistry) -> None:
        """Test that bridge addresses are terminal."""
        polygon_bridge = "0xa0c68c638235ee32657e8f720a23cec1bfc77c77"
        assert registry.is_terminal(polygon_bridge) is True

    def test_dex_is_not_terminal(self, registry: EntityRegistry) -> None:
        """Test that DEX addresses are not terminal."""
        uniswap = "0xe592427a0aece92de3edee1f18e0157c05861564"
        assert registry.is_terminal(uniswap) is False

    def test_token_is_not_terminal(self, registry: EntityRegistry) -> None:
        """Test that token addresses are not terminal."""
        usdc = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        assert registry.is_terminal(usdc) is False

    def test_unknown_is_not_terminal(self, registry: EntityRegistry) -> None:
        """Test that unknown addresses are not terminal."""
        unknown = "0x0000000000000000000000000000000000000000"
        assert registry.is_terminal(unknown) is False


class TestEntityRegistryCategory:
    """Tests for EntityRegistry.get_entity_category method."""

    @pytest.fixture
    def registry(self) -> EntityRegistry:
        """Create a registry for testing."""
        return EntityRegistry()

    def test_category_cex(self, registry: EntityRegistry) -> None:
        """Test CEX category."""
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert registry.get_entity_category(binance) == "cex"

    def test_category_bridge(self, registry: EntityRegistry) -> None:
        """Test bridge category."""
        polygon_bridge = "0xa0c68c638235ee32657e8f720a23cec1bfc77c77"
        assert registry.get_entity_category(polygon_bridge) == "bridge"

    def test_category_dex(self, registry: EntityRegistry) -> None:
        """Test DEX category."""
        uniswap = "0xe592427a0aece92de3edee1f18e0157c05861564"
        assert registry.get_entity_category(uniswap) == "dex"

    def test_category_token(self, registry: EntityRegistry) -> None:
        """Test token category."""
        usdc = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        assert registry.get_entity_category(usdc) == "token"

    def test_category_defi(self, registry: EntityRegistry) -> None:
        """Test DeFi category."""
        aave = "0x794a61358d6845594f94dc1db02a252b5b4814ad"
        assert registry.get_entity_category(aave) == "defi"

    def test_category_unknown(self, registry: EntityRegistry) -> None:
        """Test unknown category."""
        unknown = "0x0000000000000000000000000000000000000000"
        assert registry.get_entity_category(unknown) == "unknown"


class TestEntityRegistryMutations:
    """Tests for EntityRegistry mutation methods."""

    def test_add_entity(self) -> None:
        """Test adding an entity."""
        registry = EntityRegistry(include_defaults=False)
        registry.add_entity("0x1234", EntityType.CEX_OTHER)
        assert registry.classify("0x1234") == EntityType.CEX_OTHER

    def test_add_entity_normalizes_address(self) -> None:
        """Test that add_entity normalizes addresses to lowercase."""
        registry = EntityRegistry(include_defaults=False)
        registry.add_entity("0xABCD", EntityType.CEX_OTHER)
        assert registry.classify("0xabcd") == EntityType.CEX_OTHER

    def test_remove_entity(self) -> None:
        """Test removing an entity."""
        registry = EntityRegistry(include_defaults=False)
        registry.add_entity("0x1234", EntityType.CEX_OTHER)
        assert registry.remove_entity("0x1234") is True
        assert registry.classify("0x1234") == EntityType.UNKNOWN

    def test_remove_nonexistent(self) -> None:
        """Test removing a non-existent entity."""
        registry = EntityRegistry(include_defaults=False)
        assert registry.remove_entity("0x1234") is False


class TestEntityRegistryDunder:
    """Tests for EntityRegistry dunder methods."""

    def test_len(self) -> None:
        """Test __len__ returns count of entities."""
        registry = EntityRegistry(include_defaults=False)
        registry.add_entity("0x1234", EntityType.CEX_OTHER)
        registry.add_entity("0x5678", EntityType.DEX_OTHER)
        assert len(registry) == 2

    def test_contains(self) -> None:
        """Test __contains__ for membership testing."""
        registry = EntityRegistry()
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert binance in registry
        assert "0x0000000000000000000000000000000000000000" not in registry


class TestEntityRegistryContract:
    """Tests for EntityRegistry.is_contract method."""

    @pytest.fixture
    def registry(self) -> EntityRegistry:
        """Create a registry for testing."""
        return EntityRegistry()

    def test_dex_is_contract(self, registry: EntityRegistry) -> None:
        """Test that DEX addresses are contracts."""
        uniswap = "0xe592427a0aece92de3edee1f18e0157c05861564"
        assert registry.is_contract(uniswap) is True

    def test_token_is_contract(self, registry: EntityRegistry) -> None:
        """Test that token addresses are contracts."""
        usdc = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        assert registry.is_contract(usdc) is True

    def test_defi_is_contract(self, registry: EntityRegistry) -> None:
        """Test that DeFi protocol addresses are contracts."""
        aave = "0x794a61358d6845594f94dc1db02a252b5b4814ad"
        assert registry.is_contract(aave) is True

    def test_cex_is_not_contract(self, registry: EntityRegistry) -> None:
        """Test that CEX addresses are not contracts."""
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        assert registry.is_contract(binance) is False

    def test_unknown_is_not_contract(self, registry: EntityRegistry) -> None:
        """Test that unknown addresses are not contracts."""
        unknown = "0x0000000000000000000000000000000000000000"
        assert registry.is_contract(unknown) is False
