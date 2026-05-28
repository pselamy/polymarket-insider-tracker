"""Alpha-pattern views over risk_assessments.

Adds two read-only views that turn the raw `risk_assessments` stream into
two repeat-pattern leaderboards:

- `alpha_wallet_repeat_patterns` — same wallet flagged across multiple
  markets with non-trivial notional. Useful for spotting professional
  punters or insider farms that re-use the same address.
- `alpha_market_coordination` — same market touched by multiple flagged
  wallets in close succession. Useful for spotting coordinated pushes
  or two-sided arbitrage.

Both views ignore dust (notional < $50/$100), settlement edges
(price <0.05 or >0.95), and require at least 2 distinct markets/wallets.

Revision ID: 004_alpha_views
Revises: 003_tail_bet_columns
Create Date: 2026-05-28 10:00:00.000000+00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "004_alpha_views"
down_revision: str | None = "003_tail_bet_columns"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_WALLET_REPEAT_VIEW = """
CREATE OR REPLACE VIEW alpha_wallet_repeat_patterns AS
WITH flagged AS (
    SELECT *
    FROM risk_assessments
    WHERE weighted_score >= 0.30
      AND price BETWEEN 0.05 AND 0.95
      AND notional_usdc >= 50
)
SELECT
    wallet_address,
    count(DISTINCT market_id)              AS distinct_markets,
    count(*)                               AS flagged_trades,
    sum(notional_usdc)                     AS total_notional_usdc,
    max(weighted_score)                    AS max_score,
    avg(weighted_score)                    AS avg_score,
    min(trade_timestamp)                   AS first_seen_at,
    max(trade_timestamp)                   AS last_seen_at,
    EXTRACT(EPOCH FROM (max(trade_timestamp) - min(trade_timestamp)))::bigint
                                           AS span_seconds
FROM flagged
GROUP BY wallet_address
HAVING count(DISTINCT market_id) >= 2
"""

_MARKET_COORD_VIEW = """
CREATE OR REPLACE VIEW alpha_market_coordination AS
WITH flagged AS (
    SELECT *
    FROM risk_assessments
    WHERE weighted_score >= 0.30
      AND price BETWEEN 0.05 AND 0.95
      AND notional_usdc >= 100
)
SELECT
    market_id,
    count(DISTINCT wallet_address)         AS distinct_wallets,
    count(*)                               AS flagged_trades,
    sum(notional_usdc)                     AS total_notional_usdc,
    max(weighted_score)                    AS max_score,
    avg(weighted_score)                    AS avg_score,
    min(trade_timestamp)                   AS first_seen_at,
    max(trade_timestamp)                   AS last_seen_at,
    EXTRACT(EPOCH FROM (max(trade_timestamp) - min(trade_timestamp)))::bigint
                                           AS span_seconds
FROM flagged
GROUP BY market_id
HAVING count(DISTINCT wallet_address) >= 2
"""


def upgrade() -> None:
    op.execute(_WALLET_REPEAT_VIEW)
    op.execute(_MARKET_COORD_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS alpha_market_coordination")
    op.execute("DROP VIEW IF EXISTS alpha_wallet_repeat_patterns")
