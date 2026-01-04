"""Data models for the profiler module."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal


@dataclass(frozen=True)
class Transaction:
    """Represents a blockchain transaction."""

    hash: str
    block_number: int
    timestamp: datetime
    from_address: str
    to_address: str | None
    value: Decimal  # In Wei
    gas_used: int
    gas_price: Decimal  # In Wei

    @property
    def value_matic(self) -> Decimal:
        """Return value in MATIC (10^18 Wei = 1 MATIC)."""
        return self.value / Decimal("1000000000000000000")

    @property
    def gas_cost_wei(self) -> Decimal:
        """Return total gas cost in Wei."""
        return Decimal(self.gas_used) * self.gas_price

    @property
    def gas_cost_matic(self) -> Decimal:
        """Return total gas cost in MATIC."""
        return self.gas_cost_wei / Decimal("1000000000000000000")


@dataclass(frozen=True)
class WalletInfo:
    """Aggregated wallet information from blockchain queries."""

    address: str
    transaction_count: int  # Nonce
    balance_wei: Decimal
    first_transaction: Transaction | None = None

    @property
    def balance_matic(self) -> Decimal:
        """Return balance in MATIC."""
        return self.balance_wei / Decimal("1000000000000000000")

    @property
    def is_fresh(self) -> bool:
        """Return True if wallet has very few transactions (potential fresh wallet)."""
        return self.transaction_count < 10

    @property
    def wallet_age_days(self) -> float | None:
        """Return wallet age in days based on first transaction."""
        if self.first_transaction is None:
            return None
        delta = (
            datetime.now(tz=self.first_transaction.timestamp.tzinfo)
            - self.first_transaction.timestamp
        )
        return delta.total_seconds() / 86400


@dataclass(frozen=True)
class WalletProfile:
    """Complete wallet analysis profile.

    This is the result of analyzing a wallet's on-chain activity to determine
    if it exhibits suspicious behavior patterns like fresh wallet trading.

    Attributes:
        address: The wallet address (lowercase).
        nonce: Transaction count (number of outgoing transactions).
        first_seen: Timestamp of first transaction, if available.
        age_hours: Wallet age in hours since first transaction.
        is_fresh: True if wallet meets fresh wallet criteria.
        total_tx_count: Total number of transactions (same as nonce for now).
        matic_balance: MATIC balance in Wei.
        usdc_balance: USDC balance in smallest unit (6 decimals).
        analyzed_at: Timestamp when this profile was created.
        fresh_threshold: The threshold used to determine freshness.
    """

    address: str
    nonce: int
    first_seen: datetime | None
    age_hours: float | None
    is_fresh: bool
    total_tx_count: int
    matic_balance: Decimal
    usdc_balance: Decimal
    analyzed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    fresh_threshold: int = 5

    @property
    def age_days(self) -> float | None:
        """Return wallet age in days."""
        if self.age_hours is None:
            return None
        return self.age_hours / 24.0

    @property
    def matic_balance_formatted(self) -> Decimal:
        """Return MATIC balance in human-readable format (18 decimals)."""
        return self.matic_balance / Decimal("1000000000000000000")

    @property
    def usdc_balance_formatted(self) -> Decimal:
        """Return USDC balance in human-readable format (6 decimals)."""
        return self.usdc_balance / Decimal("1000000")

    @property
    def is_brand_new(self) -> bool:
        """Return True if wallet has never transacted (nonce = 0)."""
        return self.nonce == 0

    @property
    def freshness_score(self) -> float:
        """Return a 0-1 score where 1 is maximally fresh.

        Score is based on:
        - Nonce (fewer = fresher)
        - Age (younger = fresher)
        """
        # Nonce component: 1.0 at 0, 0.0 at threshold or higher
        nonce_score = max(0.0, 1.0 - (self.nonce / self.fresh_threshold))

        # Age component: 1.0 at 0 hours, 0.0 at 48 hours or more
        age_score = 1.0 if self.age_hours is None else max(0.0, 1.0 - self.age_hours / 48.0)

        # Weighted average: nonce is slightly more important
        return 0.6 * nonce_score + 0.4 * age_score


@dataclass(frozen=True)
class FundingTransfer:
    """Represents an ERC20 token transfer for funding chain analysis.

    Attributes:
        from_address: Source wallet address.
        to_address: Destination wallet address.
        amount: Transfer amount in token decimals.
        token: Token symbol (e.g., "USDC", "MATIC").
        tx_hash: Transaction hash.
        block_number: Block number of the transaction.
        timestamp: Timestamp of the transaction.
    """

    from_address: str
    to_address: str
    amount: Decimal
    token: str
    tx_hash: str
    block_number: int
    timestamp: datetime

    @property
    def amount_formatted(self) -> Decimal:
        """Return amount in human-readable format.

        Assumes 6 decimals for USDC/USDT, 18 for others.
        """
        if self.token in ("USDC", "USDT"):
            return self.amount / Decimal("1000000")
        return self.amount / Decimal("1000000000000000000")


@dataclass
class FundingChain:
    """Result of funding chain analysis.

    Represents the path of funds from origin to target wallet,
    tracing back through intermediate wallets.

    Attributes:
        target_address: The wallet being analyzed.
        chain: Ordered list of transfers from target back to origin.
        origin_address: The ultimate source of funds.
        origin_type: Classification of the origin (cex, bridge, unknown, contract).
        hop_count: Number of hops from target to origin.
        traced_at: When the trace was performed.
    """

    target_address: str
    chain: list[FundingTransfer] = field(default_factory=list)
    origin_address: str = ""
    origin_type: str = "unknown"
    hop_count: int = 0
    traced_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_cex_origin(self) -> bool:
        """Return True if funds originated from a CEX."""
        return self.origin_type.startswith("cex")

    @property
    def is_bridge_origin(self) -> bool:
        """Return True if funds came through a bridge."""
        return self.origin_type.startswith("bridge")

    @property
    def is_unknown_origin(self) -> bool:
        """Return True if origin could not be determined."""
        return self.origin_type == "unknown"

    @property
    def total_amount(self) -> Decimal:
        """Return total amount transferred in the chain."""
        if not self.chain:
            return Decimal("0")
        return self.chain[0].amount

    @property
    def funding_depth(self) -> int:
        """Return the funding depth (hops from known entity)."""
        return self.hop_count
