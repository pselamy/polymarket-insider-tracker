"""Tail-bet detector columns on risk_assessments.

Adds four nullable columns so signal-bearing trades that triggered the
new TailBetDetector can be replayed in backtests without recomputing
the structural shape from raw trade rows.

Revision ID: 003_tail_bet_columns
Revises: 002_risk_assessments
Create Date: 2026-05-28 09:00:00.000000+00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "003_tail_bet_columns"
down_revision: str | None = "002_risk_assessments"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "risk_assessments",
        sa.Column("tail_bet_confidence", sa.Numeric(4, 3), nullable=True),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("potential_payout_usdc", sa.Numeric(20, 6), nullable=True),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("payout_to_volume_ratio", sa.Numeric(10, 6), nullable=True),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("payout_to_notional_ratio", sa.Numeric(12, 4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("risk_assessments", "payout_to_notional_ratio")
    op.drop_column("risk_assessments", "payout_to_volume_ratio")
    op.drop_column("risk_assessments", "potential_payout_usdc")
    op.drop_column("risk_assessments", "tail_bet_confidence")
