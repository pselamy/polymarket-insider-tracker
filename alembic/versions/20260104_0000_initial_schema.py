"""Initial schema for wallet profiles and funding transfers.

Revision ID: 001_initial
Revises:
Create Date: 2026-01-04 00:00:00.000000+00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Wallet profiles table
    op.create_table(
        "wallet_profiles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("address", sa.String(42), nullable=False),
        sa.Column("nonce", sa.Integer(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_fresh", sa.Boolean(), nullable=False),
        sa.Column("matic_balance", sa.Numeric(30, 0), nullable=True),
        sa.Column("usdc_balance", sa.Numeric(20, 6), nullable=True),
        sa.Column("analyzed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("address"),
    )
    op.create_index("idx_wallet_profiles_address", "wallet_profiles", ["address"])

    # Funding transfers table
    op.create_table(
        "funding_transfers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("from_address", sa.String(42), nullable=False),
        sa.Column("to_address", sa.String(42), nullable=False),
        sa.Column("amount", sa.Numeric(30, 6), nullable=False),
        sa.Column("token", sa.String(10), nullable=False),
        sa.Column("tx_hash", sa.String(66), nullable=False),
        sa.Column("block_number", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tx_hash"),
    )
    op.create_index("idx_funding_transfers_to", "funding_transfers", ["to_address"])
    op.create_index("idx_funding_transfers_from", "funding_transfers", ["from_address"])
    op.create_index("idx_funding_transfers_block", "funding_transfers", ["block_number"])

    # Wallet relationships table
    op.create_table(
        "wallet_relationships",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("wallet_a", sa.String(42), nullable=False),
        sa.Column("wallet_b", sa.String(42), nullable=False),
        sa.Column("relationship_type", sa.String(20), nullable=False),
        sa.Column("confidence", sa.Numeric(3, 2), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "wallet_a", "wallet_b", "relationship_type", name="uq_wallet_relationship"
        ),
    )
    op.create_index("idx_wallet_relationships_a", "wallet_relationships", ["wallet_a"])
    op.create_index("idx_wallet_relationships_b", "wallet_relationships", ["wallet_b"])


def downgrade() -> None:
    op.drop_index("idx_wallet_relationships_b", table_name="wallet_relationships")
    op.drop_index("idx_wallet_relationships_a", table_name="wallet_relationships")
    op.drop_table("wallet_relationships")

    op.drop_index("idx_funding_transfers_block", table_name="funding_transfers")
    op.drop_index("idx_funding_transfers_from", table_name="funding_transfers")
    op.drop_index("idx_funding_transfers_to", table_name="funding_transfers")
    op.drop_table("funding_transfers")

    op.drop_index("idx_wallet_profiles_address", table_name="wallet_profiles")
    op.drop_table("wallet_profiles")
