"""Add slice 003/004 observability and disposition columns to risk_assessments.

Revision ID: 003_safe_observable_operation
Revises: 002_risk_assessments
Create Date: 2026-09-10 00:00:00.000000+00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "003_safe_observable_operation"
down_revision: str | None = "002_risk_assessments"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = [
    "branch_labels",
    "depends_on",
    "down_revision",
    "downgrade",
    "revision",
    "upgrade",
]


def upgrade() -> None:
    op.add_column(
        "risk_assessments",
        sa.Column(
            "delivery_disposition",
            sa.String(32),
            server_default="dry_run",
            nullable=False,
        ),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("delivery_channels", sa.Text(), nullable=True),
    )
    op.add_column(
        "risk_assessments",
        sa.Column(
            "dry_run",
            sa.Boolean(),
            server_default="false",
            nullable=False,
        ),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("volume_available", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("market_daily_volume", sa.Numeric(20, 6), nullable=True),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("book_depth_available", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("wallet_tx_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "risk_assessments",
        sa.Column("wallet_age_known", sa.Boolean(), nullable=True),
    )
    op.create_index(
        "idx_risk_assessments_disposition",
        "risk_assessments",
        ["delivery_disposition"],
    )


def downgrade() -> None:
    op.drop_index("idx_risk_assessments_disposition", table_name="risk_assessments")
    op.drop_column("risk_assessments", "wallet_age_known")
    op.drop_column("risk_assessments", "wallet_tx_count")
    op.drop_column("risk_assessments", "book_depth_available")
    op.drop_column("risk_assessments", "market_daily_volume")
    op.drop_column("risk_assessments", "volume_available")
    op.drop_column("risk_assessments", "dry_run")
    op.drop_column("risk_assessments", "delivery_channels")
    op.drop_column("risk_assessments", "delivery_disposition")
