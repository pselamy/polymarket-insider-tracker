"""Data models for the profiler module."""

from dataclasses import dataclass
from datetime import datetime
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
        delta = datetime.now(tz=self.first_transaction.timestamp.tzinfo) - self.first_transaction.timestamp
        return delta.total_seconds() / 86400
