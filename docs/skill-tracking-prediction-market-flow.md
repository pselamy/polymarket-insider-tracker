# Skill: tracking-prediction-market-flow

Use when analyzing prediction market activity for informed-flow signals, insider
trading patterns, or suspicious wallet behavior on Polymarket.

## What This Tool Does

polymarket-insider-tracker streams real-time trades from Polymarket's WebSocket
feed, profiles trader wallets on the Polygon blockchain, and scores each trade
for informed-flow risk using multiple detection signals:

- **Fresh wallet detection**: New wallets (age < 48h, nonce <= 5) making large
  trades (> $1k). Insiders create disposable wallets per trade.
- **Size anomaly detection**: Trades consuming > 2% of 24h volume or > 5% of
  visible order book depth. Informed traders bet bigger when they have edge.
- **Niche market scoring**: Low-volume markets (< $50k daily) get a 1.5x risk
  multiplier. Easier to have inside information on obscure events.
- **Funding chain analysis**: Traces wallet funding sources on-chain to link
  seemingly separate wallets to the same entity or exchange.
- **Sniper cluster detection**: DBSCAN clustering identifies wallets that
  consistently enter markets within minutes of creation.

Composite risk scoring combines signals with configurable weights (default
alert threshold: 0.6). Multi-signal bonuses: 2 signals +20%, 3+ signals +30%.

## Installation

```bash
git clone https://github.com/pselamy/polymarket-insider-tracker.git
cd polymarket-insider-tracker
uv sync --all-extras
docker compose up -d   # PostgreSQL + Redis
cp .env.example .env   # defaults work for local dev
uv run alembic upgrade head
```

No API keys required for basic operation (Polymarket APIs are public).

## Usage

```bash
# Start the tracker (streams trades, profiles wallets, scores risk, alerts)
uv run python -m polymarket_insider_tracker

# Dry run (no alerts sent)
uv run python -m polymarket_insider_tracker --dry-run

# Debug mode (see every trade)
uv run python -m polymarket_insider_tracker --log-level DEBUG

# Validate config without starting
uv run python -m polymarket_insider_tracker --config-check
```

## Interpreting Signals

### Risk Assessment Output

Each flagged trade produces a risk assessment with:

- **Confidence score** (0.0-1.0): Composite of weighted signals
- **Signal breakdown**: Which detectors fired and their individual confidence
- **Wallet profile**: Age, nonce, transaction count, funding source
- **Market context**: Volume, category, order book depth

### Signal Interpretation Guide

| Score Range | Interpretation | Action |
|-------------|---------------|--------|
| 0.6-0.7 | Moderate: single strong signal or two weak ones | Monitor, note the market |
| 0.7-0.85 | High: multiple signals converging | Investigate the market and wallet |
| 0.85-1.0 | Critical: fresh wallet + large size + niche market | High-confidence informed flow |

### What This Is NOT

- Not a trading signal generator. Informed flow != actionable alpha without
  further analysis (hypothesis -> leakage-aware backtest -> capital).
- Not real-time enough for front-running. The tool detects patterns for
  research and monitoring, not millisecond-level execution.
- Detection of informed flow does not prove insider trading. Many legitimate
  reasons exist for the patterns this tool flags.

## Rate Limits and Etiquette

- **Polymarket WebSocket**: No explicit rate limit; one persistent connection.
  Do not open multiple connections unnecessarily.
- **Polymarket CLOB REST**: Built-in rate limiter at 10 req/s with retry
  backoff on 429/5xx. Respect this for metadata/orderbook queries.
- **Polygon RPC**: Public endpoints (polygon-rpc.com) have low limits. For
  sustained use, configure a dedicated RPC provider via `POLYGON_RPC_URL`.
  Built-in token-bucket rate limiter at 25 req/s with Redis caching (5min TTL).

## Known Pitfalls

1. **WebSocket subscription format**: Must include `action: "subscribe"` in the
   envelope. Without it, the server accepts the connection but delivers zero
   trade events (silent failure). Fixed in the current version.

2. **Message routing**: Live-data WebSocket pushes `{connection_id, payload:
   {...trade fields}}`, not `{topic, type, payload}`. Route by checking for
   `transactionHash` + `proxyWallet` keys in `payload`.

3. **Public RPC rate limits**: Default Polygon RPC will throttle under load.
   Use a dedicated provider for production.

4. **Database required**: PostgreSQL + Redis must be running. Use
   `docker compose up -d` for local dev.

## Cross-References

- **Repository**: https://github.com/pselamy/polymarket-insider-tracker
- **Issues**: https://github.com/pselamy/polymarket-insider-tracker/issues
- **Agent skill landing** (follow-on): selamy-labs/agent-skills
