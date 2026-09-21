"""Durable terminal worker-failure record (G2).

Adds the ``pipeline_terminal_errors`` table: one row per terminal
pipeline worker failure, so the terminal reason survives process exit
and ``/ready`` plus ``/health`` can read through to it.

Revision ID: 004_terminal_errors
Revises: 003_safe_observable_operation
Create Date: 2026-09-20 00:00:00.000000+00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "004_terminal_errors"
down_revision: str | None = "003_safe_observable_operation"
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
    op.create_table(
        "pipeline_terminal_errors",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("worker", sa.String(32), nullable=False, server_default="trade_poller"),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_pipeline_terminal_errors_recorded",
        "pipeline_terminal_errors",
        ["recorded_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_pipeline_terminal_errors_recorded", table_name="pipeline_terminal_errors")
    op.drop_table("pipeline_terminal_errors")
