"""Known blockchain entity address mappings.

This module contains address-to-entity mappings for known blockchain
entities on Polygon including CEX hot wallets, bridges, and DEX contracts.

Sources:
- Etherscan labels
- Arkham Intelligence
- Official protocol documentation
"""

from __future__ import annotations

from enum import Enum


class EntityType(Enum):
    """Classification of known blockchain entities."""

    # Centralized Exchanges
    CEX_BINANCE = "cex_binance"
    CEX_COINBASE = "cex_coinbase"
    CEX_KRAKEN = "cex_kraken"
    CEX_OKX = "cex_okx"
    CEX_KUCOIN = "cex_kucoin"
    CEX_BYBIT = "cex_bybit"
    CEX_CRYPTO_COM = "cex_crypto_com"
    CEX_OTHER = "cex_other"

    # Bridges
    BRIDGE_POLYGON = "bridge_polygon"
    BRIDGE_MULTICHAIN = "bridge_multichain"
    BRIDGE_STARGATE = "bridge_stargate"
    BRIDGE_HOP = "bridge_hop"
    BRIDGE_OTHER = "bridge_other"

    # Decentralized Exchanges
    DEX_UNISWAP = "dex_uniswap"
    DEX_SUSHISWAP = "dex_sushiswap"
    DEX_QUICKSWAP = "dex_quickswap"
    DEX_1INCH = "dex_1inch"
    DEX_OTHER = "dex_other"

    # Token Contracts
    TOKEN_USDC = "token_usdc"
    TOKEN_USDT = "token_usdt"
    TOKEN_WETH = "token_weth"
    TOKEN_WMATIC = "token_wmatic"

    # Lending/DeFi
    DEFI_AAVE = "defi_aave"
    DEFI_COMPOUND = "defi_compound"
    DEFI_OTHER = "defi_other"

    # Other
    CONTRACT = "contract"
    UNKNOWN = "unknown"


# CEX hot wallet addresses on Polygon
# Sources: Etherscan labels, Arkham Intelligence, public disclosures
CEX_ADDRESSES: dict[str, EntityType] = {
    # Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": EntityType.CEX_BINANCE,
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": EntityType.CEX_BINANCE,
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": EntityType.CEX_BINANCE,
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": EntityType.CEX_BINANCE,
    # Coinbase
    "0x503828976d22510aad0339f595f37cc4e4645c80": EntityType.CEX_COINBASE,
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": EntityType.CEX_COINBASE,
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": EntityType.CEX_COINBASE,
    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": EntityType.CEX_KRAKEN,
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": EntityType.CEX_KRAKEN,
    # OKX
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": EntityType.CEX_OKX,
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": EntityType.CEX_OKX,
    # KuCoin
    "0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91": EntityType.CEX_KUCOIN,
    "0xd6216fc19db775df9774a6e33526131da7d19a2c": EntityType.CEX_KUCOIN,
    # Bybit
    "0xf89e6d82be28f5cc97a9e6a94a16a17e5be73e78": EntityType.CEX_BYBIT,
    # Crypto.com
    "0x6262998ced04146fa42253a5c0af90ca02dfd2a3": EntityType.CEX_CRYPTO_COM,
    "0x46340b20830761efd32832a74d7169b29feb9758": EntityType.CEX_CRYPTO_COM,
}

# Bridge contract addresses on Polygon
BRIDGE_ADDRESSES: dict[str, EntityType] = {
    # Polygon PoS Bridge (RootChain / Plasma Bridge related)
    "0xa0c68c638235ee32657e8f720a23cec1bfc77c77": EntityType.BRIDGE_POLYGON,
    "0x401f6c983ea34274ec46f84d70b31c151321188b": EntityType.BRIDGE_POLYGON,
    # Multichain (formerly AnySwap)
    "0x4f3aff3a747fcade12598081e80c6605a8be192f": EntityType.BRIDGE_MULTICHAIN,
    # Stargate
    "0x45a01e4e04f14f7a4a6880d0cbaf2c3c1acfbed4": EntityType.BRIDGE_STARGATE,
    # Hop Protocol
    "0x76b22b8c1079a44f1211b0e72c5d26c5e3b3c3c9": EntityType.BRIDGE_HOP,
}

# DEX router addresses on Polygon
DEX_ADDRESSES: dict[str, EntityType] = {
    # Uniswap V3
    "0xe592427a0aece92de3edee1f18e0157c05861564": EntityType.DEX_UNISWAP,
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": EntityType.DEX_UNISWAP,  # SwapRouter02
    # SushiSwap
    "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506": EntityType.DEX_SUSHISWAP,
    # QuickSwap
    "0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff": EntityType.DEX_QUICKSWAP,
    # 1inch
    "0x1111111254eeb25477b68fb85ed929f73a960582": EntityType.DEX_1INCH,
}

# Token contract addresses on Polygon
TOKEN_ADDRESSES: dict[str, EntityType] = {
    # USDC (Bridged)
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": EntityType.TOKEN_USDC,
    # USDC (Native)
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359": EntityType.TOKEN_USDC,
    # USDT
    "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": EntityType.TOKEN_USDT,
    # WETH
    "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619": EntityType.TOKEN_WETH,
    # WMATIC
    "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270": EntityType.TOKEN_WMATIC,
}

# DeFi protocol addresses on Polygon
DEFI_ADDRESSES: dict[str, EntityType] = {
    # Aave V3
    "0x794a61358d6845594f94dc1db02a252b5b4814ad": EntityType.DEFI_AAVE,  # Pool
    "0x8145edddf43f50276641b55bd3ad95944510021e": EntityType.DEFI_AAVE,  # PoolAddressesProvider
}


def get_all_known_entities() -> dict[str, EntityType]:
    """Get all known entity addresses combined.

    Returns:
        Dictionary mapping lowercase addresses to their entity types.
    """
    all_entities: dict[str, EntityType] = {}

    for entities in [
        CEX_ADDRESSES,
        BRIDGE_ADDRESSES,
        DEX_ADDRESSES,
        TOKEN_ADDRESSES,
        DEFI_ADDRESSES,
    ]:
        for address, entity_type in entities.items():
            all_entities[address.lower()] = entity_type

    return all_entities
