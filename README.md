# Polymarket Insider Tracker

**Detect informed money before the market moves.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

---

## The Opportunity

On January 3, 2026, a trader spotted a significant political event on Polymarket **before it happened**. How? Not by predicting the future, but by tracking suspicious trading behavior.

> "You don't need to predict the future, you need to track suspicious behavior."
> — [@DidiTrading](https://x.com/DidiTrading)

An insider wallet turned **$35,000 into $442,000** (12.6x return) by entering a position hours before a major market move. The tool that detected this activity flagged five separate alerts before the event occurred.

**This repository builds that tool.**

---

## What This Does

The Polymarket Insider Tracker monitors prediction market trading activity in real-time and identifies patterns that suggest informed trading:

| Signal | What It Detects | Why It Matters |
|--------|-----------------|----------------|
| **Fresh Wallets** | Brand new wallets making large trades | Insiders create new wallets to hide their identity |
| **Unusual Sizing** | Trades that are disproportionately large for the market | Informed traders bet bigger when they have edge |
| **Niche Markets** | Activity in low-volume, specific-outcome markets | Easier to have inside information on obscure events |
| **Funding Chains** | Where wallet funds originated from | Links seemingly separate wallets to the same entity |

When suspicious activity is detected, you receive an instant alert with actionable intelligence.

---

## How It Works

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│  Polymarket API │────>│  Wallet Profiler │────>│  Anomaly Detector  │
│  (Real-time)    │     │  (Blockchain)    │     │  (ML + Heuristics) │
└─────────────────┘     └──────────────────┘     └────────────────────┘
                                                          │
                              ┌────────────────────────────┘
                              v
                   ┌─────────────────────┐
                   │   Alert Dispatcher  │───> Discord / Telegram / Email
                   │   "Fresh wallet     │
                   │    buying YES @7.5¢ │
                   │    on niche market" │
                   └─────────────────────┘
```

### Detection Algorithms

1. **Fresh Wallet Detection**
   - Checks wallet transaction history on Polygon
   - Flags wallets with fewer than 5 lifetime transactions making trades over $1,000
   - Traces funding source to identify if connected to known entities

2. **Liquidity Impact Analysis**
   - Calculates trade size relative to market depth
   - Flags trades consuming more than 2% of visible order book
   - Weights by market category (niche markets score higher)

3. **Sniper Cluster Detection**
   - Uses DBSCAN clustering to find wallets that consistently enter markets within minutes of creation
   - Identifies coordinated behavior patterns

4. **Event Correlation**
   - Cross-references trading activity with news feeds
   - Detects positions opened 1-4 hours before related news breaks

---

## Sample Alert

```
SUSPICIOUS ACTIVITY DETECTED

Wallet: 0x7a3...f91 (Age: 2 hours, 3 transactions)
Market: "Will X announce Y by March 2026?"
Action: BUY YES @ $0.075
Size: $15,000 USDC (8.2% of daily volume)

Risk Signals:
  [x] Fresh Wallet (fewer than 5 transactions lifetime)
  [x] Niche Market (less than $50k daily volume)
  [x] Large Position (more than 2% order book impact)

Funding Trail:
  --> 0xdef...789 (2-year-old wallet, 500+ txns)
      --> Binance Hot Wallet

Confidence: HIGH (3/4 signals triggered)
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Polygon RPC endpoint (Alchemy, QuickNode, or self-hosted)
- Polymarket API key (free at [docs.polymarket.com](https://docs.polymarket.com))

### Installation

```bash
# Clone the repository
git clone https://github.com/pselamy/polymarket-insider-tracker.git
cd polymarket-insider-tracker

# Copy environment template
cp .env.example .env
# Edit .env with your API keys

# Start infrastructure (PostgreSQL, Redis)
docker-compose up -d

# Install Python dependencies
pip install -e .

# Run the tracker
python -m src.main
```

### Configuration

```bash
# .env file
POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
POLYMARKET_API_KEY=your_polymarket_api_key

# Alert destinations (optional)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Detection thresholds
MIN_TRADE_SIZE_USDC=1000
FRESH_WALLET_MAX_NONCE=5
LIQUIDITY_IMPACT_THRESHOLD=0.02
```

---

## Project Structure

```
polymarket-insider-tracker/
├── src/
│   ├── ingestor/           # Real-time market data ingestion
│   │   ├── clob_client.py  # Polymarket CLOB API wrapper
│   │   └── websocket.py    # WebSocket event handler
│   ├── profiler/           # Wallet analysis
│   │   ├── analyzer.py     # Core wallet profiling logic
│   │   ├── chain.py        # Polygon blockchain client
│   │   └── funding.py      # Funding chain tracer
│   ├── detector/           # Anomaly detection engines
│   │   ├── fresh_wallet.py
│   │   ├── size_anomaly.py
│   │   ├── sniper.py       # DBSCAN clustering
│   │   └── scorer.py       # Composite risk scoring
│   ├── alerter/            # Notification dispatch
│   │   ├── formatter.py    # Alert message formatting
│   │   └── dispatcher.py   # Multi-channel delivery
│   └── storage/            # Persistence layer
│       ├── models.py       # SQLAlchemy models
│       └── repos.py        # Repository pattern
├── tests/                  # Test suite
├── scripts/
│   └── backtest.py         # Historical analysis
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## Roadmap

### Phase 1: Core Detection (Current)
- [x] Project structure and documentation
- [ ] Polymarket CLOB API integration
- [ ] Fresh wallet detection
- [ ] Size anomaly detection
- [ ] Basic alerting (Discord/Telegram)

### Phase 2: Advanced Intelligence
- [ ] Funding chain analysis
- [ ] Sniper cluster detection (DBSCAN)
- [ ] Market categorization (niche vs mainstream)
- [ ] Historical backtesting framework

### Phase 3: Production Hardening
- [ ] High-availability deployment
- [ ] Rate limit management
- [ ] False positive feedback loop
- [ ] Web dashboard

---

## Why This Matters

Prediction markets are becoming a critical source of real-time probability estimates for world events. As they grow, so does the incentive for informed actors to exploit information asymmetry.

This tool democratizes access to the same detection capabilities that sophisticated traders use. Whether you are:

- **A trader** looking for alpha signals
- **A researcher** studying market microstructure
- **A platform operator** monitoring for manipulation

...this tracker provides visibility into the hidden flows that move markets.

---

## Technical Background

### Polymarket Architecture

Polymarket is a prediction market platform built on Polygon (Ethereum L2). Key characteristics:

- **CLOB (Central Limit Order Book)**: Centralized matching engine for speed
- **On-chain Settlement**: Final trades settle on Polygon blockchain
- **USDC Collateral**: All positions denominated in USDC stablecoin
- **Binary Outcomes**: Shares priced between $0.00 and $1.00

### Data Sources

| Source | Purpose | Latency |
|--------|---------|---------|
| Polymarket CLOB API | Real-time trades, orderbook | Milliseconds |
| Polygon RPC | Wallet history, nonce, funding | 1-2 seconds |
| Market Metadata API | Market categorization | On-demand |

### Detection Challenges

1. **Sybil Resistance**: Insiders use fresh wallets per trade
2. **Rate Limits**: Polygon RPC calls require caching strategy
3. **Market Classification**: NLP needed to categorize market niches
4. **Timing**: CLOB data leads on-chain by seconds

---

## Contributing

Contributions are welcome! Please read our Contributing Guide before submitting PRs.

### Development Setup

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check src/

# Run type checking
mypy src/
```

---

## Disclaimer

This software is provided for **educational and research purposes only**.

- Trading prediction markets involves significant financial risk
- This tool does not constitute financial advice
- Insider trading is illegal in regulated markets; this tool is for transparency and research
- Users are responsible for compliance with applicable laws

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

## Acknowledgments

- Inspired by [@DidiTrading](https://x.com/DidiTrading) and [@spacexbt](https://x.com/spacexbt)
- Built on the open Polymarket API ecosystem
- Community contributions welcome

---

**Questions?** Open an issue or start a discussion.
