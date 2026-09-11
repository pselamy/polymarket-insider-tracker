# Data Model: Safe Observable Operation

**Feature**: `specs/003-safe-observable-operation`
**Date**: 2026-09-10

This document defines the data models, database schema updates, in-memory representations, and Redis key structures for slice 003.

---

## 1. Persisted Risk Assessment Model (`risk_assessments`)

This slice modifies the existing `risk_assessments` table via Alembic revision `003_safe_observable_operation`.

### Table Schema (`risk_assessments`)

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `id` | INTEGER | No | autoincrement | Primary key |
| `assessment_id` | VARCHAR(36) | No | - | Unique UUID string |
| `trade_id` | VARCHAR(80) | No | - | Ingested trade identifier |
| `wallet_address` | VARCHAR(42) | No | - | Normalized lowercase 0x wallet address |
| `market_id` | VARCHAR(80) | No | - | Polymarket condition or market ID |
| `asset_id` | VARCHAR(80) | Yes | NULL | Token asset ID |
| `side` | VARCHAR(8) | No | - | BUY or SELL |
| `outcome` | VARCHAR(120) | Yes | NULL | Outcome description |
| `outcome_index` | INTEGER | Yes | NULL | Outcome index |
| `price` | NUMERIC(10, 6) | No | - | Execution price |
| `size` | NUMERIC(20, 6) | No | - | Share count |
| `notional_usdc` | NUMERIC(20, 6) | No | - | Total trade value in USDC |
| `trade_timestamp` | TIMESTAMP WITH TIME ZONE | No | - | Execution time |
| `weighted_score` | NUMERIC(4, 3) | No | - | Calculated composite risk score (0.000 - 1.000). Confidences, the score, and the threshold are quantized to this 3-decimal precision *before* the alert decision, so the stored values exactly explain and replay `should_alert`; signal weights are the code-pinned scorer defaults (non-default weights are logged) |
| `signals_triggered` | INTEGER | No | - | Number of operational signal groups triggered |
| `fresh_wallet_confidence` | NUMERIC(4, 3) | Yes | NULL | Confidence of fresh wallet signal |
| `size_anomaly_confidence` | NUMERIC(4, 3) | Yes | NULL | Confidence of size anomaly signal |
| `is_niche_market` | BOOLEAN | Yes | NULL | Flag indicating niche market context |
| `volume_impact` | NUMERIC(8, 4) | Yes | NULL | Calculated volume impact ratio |
| `book_impact` | NUMERIC(8, 4) | Yes | NULL | Order book impact ratio |
| `wallet_age_hours` | NUMERIC(10, 2) | Yes | NULL | Observed wallet age in hours |
| `should_alert` | BOOLEAN | No | - | True if weighted_score >= threshold_at_eval |
| `threshold_at_eval` | NUMERIC(4, 3) | No | - | Effective threshold used for evaluation |
| `delivery_disposition` | VARCHAR(32) | No | 'unrecorded' | Outcome: `dry_run`, `below_threshold`, `delivered`, `partial_failure`, `failed`, `duplicate`, `ambiguous`, `no_channels`, `unrecorded` (delivery state not recorded: rows predating migration 003 or inserts outside the dispatch path) |
| `delivery_channels` | TEXT | Yes | NULL | JSON map of channel name to delivery status |
| `dry_run` | BOOLEAN | No | FALSE | True if evaluated under dry-run mode |
| `volume_available` | BOOLEAN | Yes | NULL | True if 24h market volume was available |
| `market_daily_volume` | NUMERIC(20, 6) | Yes | NULL | 24h market volume used during evaluation |
| `book_depth_available` | BOOLEAN | Yes | NULL | True if order book depth was available |
| `wallet_tx_count` | INTEGER | Yes | NULL | Nonce or transaction count observed for wallet |
| `wallet_age_known` | BOOLEAN | Yes | NULL | True if wallet age could be proven <= 48h |
| `created_at` | TIMESTAMP WITH TIME ZONE | No | now() | Record creation time |

### Indexes
- `idx_risk_assessments_wallet` on (`wallet_address`)
- `idx_risk_assessments_market` on (`market_id`)
- `idx_risk_assessments_trade_ts` on (`trade_timestamp`)
- `idx_risk_assessments_score` on (`weighted_score`)
- `idx_risk_assessments_disposition` on (`delivery_disposition`)

---

## 2. Redis Deduplication & Ambiguity Models

### 2.1 Confirmed Delivery Deduplication Key
- **Key Pattern**: `alert:dedup:{channel}:{wallet}:{market}`
  - Example: `alert:dedup:discord:0x1234567890abcdef1234567890abcdef12345678:0xabcdef...`
- **Value**: ISO-8601 UTC timestamp of delivery (string).
- **TTL**: `dedup_window_seconds` (default 3600 seconds = 1 hour).
- **Written By**: `AlertHistory.record_channel_delivery()` upon confirmed HTTP 2xx from webhook.
- **Never Written By**:
  - `RiskScorer.assess()`
  - Dry-run dispatch
  - Failed channel dispatch

### 2.2 Ambiguity / In-Flight Claim Key
- **Key Pattern**: `alert:ambiguous:{channel}:{wallet}:{market}`
- **Value**: ISO-8601 UTC timestamp of the send attempt.
- **TTL**: 60 seconds (refreshed to the full window on an ambiguous outcome).
- **Written By**: `AlertDispatcher`, atomically (`SET NX EX`) before each send attempt as
  the per-identity in-flight claim; released (`DEL`) on confirmed success or confirmed
  failure, retained on network timeout or indeterminate send outcome.
- **Semantics**:
  - While active (<60s): Suppresses automatic retry — and any concurrent dispatch of the
    same identity — to prevent double delivery.
  - After expiry (>=60s): Channel becomes re-eligible. The ambiguous attempt is durably
    recorded (assessment disposition `ambiguous`) and logged with an explicit
    possible-duplicate warning at ambiguity time, because the expired key leaves no
    marker; a later successful retry may therefore duplicate a delivery.

---

## 3. In-Memory Health & Observability Models

### 3.1 `HealthStatus` & `StreamStatus`
```python
class HealthStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

class ComponentStatus(StrEnum):
    UP = "up"
    DOWN = "down"
    DEGRADED = "degraded"
```

### 3.2 `DetailedHealthReport`
```python
@dataclass
class ComponentHealth:
    name: str
    status: ComponentStatus
    latency_ms: float | None = None
    last_check_time: float | None = None
    error: str | None = None

@dataclass
class DetailedHealthReport:
    status: HealthStatus
    uptime_seconds: float
    last_acquisition_time: float | None
    last_trade_time: float | None
    acquisition_freshness_seconds: float | None
    trade_freshness_seconds: float | None
    components: dict[str, ComponentHealth]
    total_events_received: int
    events_per_second: float
    last_error: str | None
    timestamp: float
```

---

## 4. Pipeline State Transitions

```text
       ┌─────────────┐
       │   STOPPED   │◄───────────────────────┐
       └──────┬──────┘                        │
              │ start()                       │
              ▼                               │
       ┌─────────────┐                        │
       │  STARTING   │                        │
       └──────┬──────┘                        │
              │ components initialized        │
              ▼                               │
       ┌─────────────┐                        │
       │   RUNNING   │                        │
       └──────┬──────┘                        │
              │                               │
     ┌────────┴────────┐                      │
     │                 │                      │
     │ stop()          │ worker crashed       │
     ▼                 ▼                      │
┌──────────┐     ┌───────────┐                │
│ STOPPING │     │   ERROR   │                │
└────┬─────┘     └─────┬─────┘                │
     │                 │                      │
     │ cleanup()       │ cleanup()            │
     └─────────────────┴──────────────────────┘
```

When in `ERROR` state:
- `/ready` returns HTTP 503.
- Supervisor triggers graceful stop of remaining tasks.
- CLI process terminates with `EXIT_ERROR` (1).
