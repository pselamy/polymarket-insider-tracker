# Data Model: Supported Trade Ingestion

This slice adds no PostgreSQL schema or migration. Its entities are the normalized observation handed
to existing consumers, the durable Redis-backed boundary, the ingestion status exposed to operators
and later health wiring, and the source configuration. Field names below are the implementation
contract; the provider field names come from the official trades query documentation.

## Trade Observation

One valid provider row, normalized for the existing pipeline. The existing `TradeEvent` value remains
the downstream contract; the observation wraps it with its identity and provenance.

| Field | Type | Source field | Rule |
|---|---|---|---|
| `event` | `TradeEvent` | row | Built by a strict parser; never by the lenient WebSocket parser defaults |
| `event.trade_id` | string | `transactionHash` | Required; shared by every observation in a transaction |
| `event.wallet_address` | string | `proxyWallet` | Required; stored as provided, lowercased only inside the identity |
| `event.market_id` | string | `conditionId` | Required |
| `event.asset_id` | string | `asset` | Required |
| `event.side` | `BUY` or `SELL` | `side` | Required; any other value is invalid |
| `event.price` | decimal | `price` | Required; must parse as a finite decimal |
| `event.size` | decimal | `size` | Required; must parse as a finite decimal |
| `event.timestamp` | aware datetime | `timestamp` | Required integer epoch seconds; at most 60 s ahead of the local clock |
| `event.outcome` | string | `outcome` or metadata repair | Optional; empty string when unknown |
| `event.outcome_index` | integer | `outcomeIndex` or metadata repair | Optional; defaults to 0 only when repair also fails and is then reported as unknown by `outcome_known=False` |
| `event.market_slug`, `event.event_slug`, `event.event_title`, `event.trader_name`, `event.trader_pseudonym` | string | `slug`, `eventSlug`, `title`, `name`, `pseudonym` | Optional; empty when absent |
| `identity` | 64-hex string | derived | SHA-256 of the canonical identity tuple |
| `provider_timestamp` | integer | `timestamp` | Epoch seconds as returned |
| `outcome_known` | boolean | derived | False when neither the row nor metadata supplied the outcome |
| `repaired` | boolean | derived | True when metadata supplied the outcome |

### Identity tuple

```text
transactionHash | lowercase(proxyWallet) | conditionId | asset | side | outcomeIndex-or-empty |
canonical(price) | canonical(size) | timestamp
```

`canonical()` renders a decimal without exponent or trailing zeros so `0.50` and `0.5` are the same
observation. Two rows with the same tuple are one observation; any difference is a distinct observation.

## Row Disposition

Every parsed row receives exactly one disposition. Counts are exposed in status and metrics.

| Disposition | Meaning | Boundary effect |
|---|---|---|
| `emitted` | New identity inside the horizon; delivered downstream | Identity retained |
| `duplicate` | Identity already retained | None |
| `padding` | Older than the horizon behind the newest cycle-eligible timestamp | None |
| `anchor-history` | Valid row used only to establish a first-start boundary | Identity may be retained; never emitted |
| `deferred:future-cycle` | Valid row newer than this cycle's requested `end` | Reacquired later; no identity or boundary effect |
| `invalid:<field>` | Missing or malformed identity-bearing field | Never advances the boundary |
| `invalid:future-timestamp` | More than 60 s ahead of the local clock | Never advances the boundary |
| `invalid:schema` | Row is not an object | Never advances the boundary |

An invalid row is described by a bounded, wallet-free diagnostic: the SHA-256 of the raw row JSON and
the failing field name.

Outcome resolution (`provided`, `repaired`, or `unknown`) is an independent attribute and counter, not
a second row disposition. This keeps every row in exactly one disposition bucket.

## Observation Boundary

Durable state under the Redis prefix `polymarket:ingest:<source-id>:`, where `source-id` is the first 12
hex characters of the SHA-256 of `<trades-url>|<coverage>`. All three keys are written in one
transactional pipeline.

### Checkpoint (`checkpoint` hash)

| Field | Type | Rule |
|---|---|---|
| `schema_version` | integer | `1`; an unknown version is a terminal configuration error |
| `boundary_time` | integer epoch seconds | Complete-through timestamp of the last proven page; not merely the newest observed row |
| `boundary_origin` | enum | `first-start`, `proven`, `re-anchored` |
| `written_at` | integer epoch seconds | Local clock at write time |
| `coverage` | enum | `all` or `taker-only`; must match configuration or the boundary is treated as absent |
| `last_request_end` | integer epoch seconds | Highest `end` sent; the next request is strictly greater |

### Identity window (`identities` sorted set)

Members are identities; scores are provider timestamps. Retention floor is
`newest_accepted_timestamp - recovery_horizon_seconds`; members below the floor are trimmed after
each proven cycle. The in-memory mirror is loaded from this set at start and must equal it after every
cycle in tests.

### Loss events (`loss-events` list)

Newest first, trimmed to 50 entries. Each entry is JSON:

| Field | Type | Rule |
|---|---|---|
| `from_time` | integer | Frozen boundary time when the gap opened |
| `to_time` | integer | Oldest timestamp of the first unproven page, or the re-anchor time |
| `reason` | enum | `horizon-expired`, `restart-beyond-horizon`, `continuity-mismatch` |
| `detected_at` | integer | Local clock when the gap was detected |
| `recorded_at` | integer | Local clock when the boundary re-anchored |
| `pages_examined` | integer | Requests spent trying to prove the interval |

### Boundary proof

```text
page       := valid rows at or before the cycle cutoff, sorted ascending by (timestamp, identity)
oldest     := min timestamp over every parsed valid row, padding included
reach      := oldest < boundary_time
expected   := { identity in window : score(identity) > oldest }
continuity := expected ⊆ identities(page)
proven     := reach and continuity
```

An empty page is neither proven nor a loss: reachability is updated, nothing is emitted, and the
boundary does not move.

### State transitions

```text
absent ──first page──▶ first-start (boundary = newest, no emission)
proven page ──▶ proven (boundary = newest cycle-eligible, identities trimmed)
unproven page ──recovery page proven──▶ proven
unproven page ──recovery page unproven──▶ frozen (possible-data-loss; provisional boundary in memory)
frozen ──boundary age > horizon──▶ re-anchored (loss event written, boundary = provisional newest)
absent-or-stale checkpoint older than horizon at start ──▶ re-anchored (restart-beyond-horizon)
```

The durable boundary never moves while a gap is unresolved. The only ways out of `frozen` are a later
proven page against the frozen boundary or aging into a recorded loss event.

## Ingestion Status

Non-persistent, read from the poller by the pipeline, logs, tests, and later health wiring.

| Field | Type | Rule |
|---|---|---|
| `state` | enum | `stopped`, `starting`, `running`, `degraded`, `possible-data-loss`, `failed` |
| `coverage` | enum | Configured coverage mode |
| `boundary_time` | integer or null | Durable boundary |
| `boundary_origin` | enum or null | As in the checkpoint |
| `last_acquisition_at` | aware datetime or null | Last request start |
| `last_success_at` | aware datetime or null | Last HTTP 200 with a parseable body, including an empty list |
| `last_trade_at` | aware datetime or null | Newest emitted provider timestamp |
| `latest_seen_at` | aware datetime or null | Newest valid provider timestamp seen, including deferred rows |
| `provider_lag_seconds` | float or null | Local clock at response minus newest cycle-eligible timestamp; timestamp freshness only, not first-publication latency |
| `processing_lag_seconds` | float | Time the last cycle spent inside downstream callbacks |
| `consecutive_failures` | integer | Reset on success |
| `last_error` | string or null | Redacted, wallet-free |
| `counts` | mapping | One integer per Row Disposition plus `polls`, `recovery_pages`, `empty_responses`, `retries` |
| `outcome_counts` | mapping | `provided`, `repaired`, and `unknown` counts, independent of row disposition |
| `page_rows` | integer | Rows in the last response |
| `page_span_seconds` | integer | Newest minus oldest timestamp in the last page |
| `loss_events` | tuple | Up to five most recent loss events |
| `requests_last_10s` | integer | Sliding count for the budget metric |

### Lifecycle

```text
stopped → starting → running ⇄ degraded
running → possible-data-loss → running (loss event recorded)
any → failed (terminal) ; any → stopped (clean cancellation)
```

`degraded` means the last cycle exhausted transient retries; `possible-data-loss` means the boundary is
frozen over an unresolved gap; `failed` means a non-retryable error stopped acquisition. Metrics mirror
the state with a gauge and the counts with counters.

## Source Configuration

Part of `PolymarketSettings`; secrets are excluded from every rendering.

| Field | Environment variable | Type | Default | Rule |
|---|---|---|---|---|
| `trades_url` | `POLYMARKET_TRADES_URL` | HTTPS/HTTP URL | `https://data-api.polymarket.com/trades` | Must be an `http(s)://` endpoint |
| `trades_coverage` | `POLYMARKET_TRADES_COVERAGE` | enum | `all` | `all` or `taker-only` |
| `trades_poll_interval_seconds` | `POLYMARKET_TRADES_POLL_INTERVAL_SECONDS` | integer | `5` | 1–60 inclusive |
| `trades_recovery_horizon_seconds` | `POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS` | integer | `600` | 60–3600 inclusive |
| `ws_url` | `POLYMARKET_WS_URL` | WebSocket URL or null | `None` | Accepted only with `ws://`/`wss://`; deprecated warning when set; never used |
| `api_key` | `POLYMARKET_API_KEY` | secret or null | `None` | Unchanged; consumed by the CLOB client only |

Fixed acquisition constants (not configuration): page limit 10,000; one recovery page at offset 10,000;
request timeout 15 s; four retries with base 1 s, cap 30 s, full jitter; `Retry-After` honoured up to
60 s; clock-skew allowance 60 s; loss-event retention 50.

## Smoke Evidence Record

Output of the live-safe smoke check and the shape of every feasibility probe. It never contains a
wallet, name, pseudonym, or raw row.

| Field | Type | Rule |
|---|---|---|
| `endpoint` | string | Request URL without query string |
| `captured_at` | ISO-8601 UTC | Local clock |
| `case` | enum | `valid-wallet-bearing`, `valid-empty`, `throttled`, `timeout`, `malformed-row`, `incompatible-schema`, `possible-page-saturation` |
| `http_status` | integer or null | Null on transport failure |
| `row_count`, `valid_rows`, `invalid_rows` | integers | Aggregates |
| `missing_required_fields` | mapping | Field name to count |
| `newest_timestamp`, `oldest_timestamp` | integers or null | Provider timestamps |
| `provider_lag_seconds` | float or null | Local clock minus newest timestamp; never labeled first-publication latency |
| `page_saturated` | boolean | Row count equals the page limit |
| `cache_control`, `cf_cache_status`, `age` | strings or null | Response headers as received |
| `response_sha256` | string | Digest of the raw body; the body itself is discarded |
| `retained_wallet_identifiers` | boolean | Always `false`; asserted by tests |
| `passed` | boolean | Case-specific verdict |
