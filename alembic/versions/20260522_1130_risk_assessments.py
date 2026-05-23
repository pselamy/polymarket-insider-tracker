"""Risk assessment persistence layer.

Adds the `risk_assessments` table — one row per signal-bearing trade —
so future backtests can rebuild ground truth without grepping the
systemd log or hammering the public data-api.

Revision ID: 002_risk_assessments
Revises: 001_initial
Create Date: 2026-05-22 11:30:00.000000+00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "002_risk_assessments"
down_revision: str | None = "001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "risk_assessments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("assessment_id", sa.String(36), nullable=False),
        sa.Column("trade_id", sa.String(80), nullable=False),
        sa.Column("wallet_address", sa.String(42), nullable=False),
        sa.Column("market_id", sa.String(80), nullable=False),
        sa.Column("asset_id", sa.String(80), nullable=True),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("outcome", sa.String(120), nullable=True),
        sa.Column("outcome_index", sa.Integer(), nullable=True),
        sa.Column("price", sa.Numeric(10, 6), nullable=False),
        sa.Column("size", sa.Numeric(20, 6), nullable=False),
        sa.Column("notional_usdc", sa.Numeric(20, 6), nullable=False),
        sa.Column("trade_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("weighted_score", sa.Numeric(4, 3), nullable=False),
        sa.Column("signals_triggered", sa.Integer(), nullable=False),
        sa.Column("fresh_wallet_confidence", sa.Numeric(4, 3), nullable=True),
        sa.Column("size_anomaly_confidence", sa.Numeric(4, 3), nullable=True),
        sa.Column("is_niche_market", sa.Boolean(), nullable=True),
        sa.Column("volume_impact", sa.Numeric(8, 4), nullable=True),
        sa.Column("book_impact", sa.Numeric(8, 4), nullable=True),
        sa.Column("wallet_age_hours", sa.Numeric(10, 2), nullable=True),
        sa.Column("should_alert", sa.Boolean(), nullable=False),
        sa.Column("threshold_at_eval", sa.Numeric(4, 3), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("assessment_id"),
    )
    op.create_index("idx_risk_assessments_wallet", "risk_assessments", ["wallet_address"])
    op.create_index("idx_risk_assessments_market", "risk_assessments", ["market_id"])
    op.create_index("idx_risk_assessments_trade_ts", "risk_assessments", ["trade_timestamp"])
    op.create_index("idx_risk_assessments_score", "risk_assessments", ["weighted_score"])


def downgrade() -> None:
    op.drop_index("idx_risk_assessments_score", table_name="risk_assessments")
    op.drop_index("idx_risk_assessments_trade_ts", table_name="risk_assessments")
    op.drop_index("idx_risk_assessments_market", table_name="risk_assessments")
    op.drop_index("idx_risk_assessments_wallet", table_name="risk_assessments")
    op.drop_table("risk_assessments")
