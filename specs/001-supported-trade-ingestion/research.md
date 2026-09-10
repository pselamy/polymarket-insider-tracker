# Research: Supported Trade Ingestion

**Date**: 2026-09-09

**Scope**: Resolve every implementation choice needed to replace the obsolete WebSocket trade path with
near-real-time acquisition from the officially documented public trades query, without changing
detection, scoring, persistence, or delivery behavior. Every decision below is grounded in current
official documentation or in the aggregate-only live probes recorded in
[evidence/feasibility.md](evidence/feasibility.md).

## Evidence at the Planning Anchor

- `ingestor/websocket.py` subscribes to an `activity`/`trades` topic on
  `wss://ws-live-data.polymarket.com`; current documentation limits the real-time data service to
  comments and crypto prices, and the documented Market WebSocket omits the trader wallet (G-001–G-003).
- `pipeline.py` awaits `MarketMetadataSync.start()`, which performs a complete metadata crawl, before the
  trade stream task is created (G-004; issue #93 reports 204 seconds).
- `config.py` defaults `POLYMARKET_WS_URL` to the CLOB Market channel, README documents a different host,
  and `.env.example` documents a third value (G-002, G-006).
- No replay, duplicate, or saturation contract exists (G-005, G-031, G-032).
- The public trades query returns `proxyWallet`, `side`, `asset`, `conditionId`, `size`, `price`,
  `timestamp`, `title`, `slug`, `icon`, `eventSlug`, `outcome`, `outcomeIndex`, `name`, `pseudonym`,
  `bio`, `profileImage`, `profileImageOptimized`, and `transactionHash`; these are the same field names
  that `TradeEvent.from_websocket_message` already consumes.

## Decision 1: Source is the documented anonymous public trades query

**Decision**: Acquire trades with anonymous `GET https://data-api.polymarket.com/trades` using
`limit=10000`, `offset=0`, `takerOnly=false` by default, and documented `start`/`end` parameters, over
the existing `httpx` dependency. No new dependency is added.

**Rationale**: It is the only currently documented public interface that supplies the trader wallet for
every market. Decision A1 conditionally approved this source; the 2026-09-09 probes satisfied the
conditions (cadence, ordering, lag, cache, and saturation detection) for planning.

**Alternatives considered**:

- Market WebSocket `last_trade_price` events: rejected because the documented message omits the wallet
  and requires per-asset subscriptions, so it cannot drive an all-market wallet monitor.
- The legacy real-time data service `activity`/`trades` topic: rejected because current documentation no
  longer lists trade activity; relying on it would violate the truthful-contract principle.
- Any authenticated user-trade feed: rejected because the product must stay public-data and read-only.
- A new HTTP client library: rejected because `httpx` is already a locked dependency with an async client,
  transport injection for fakes, and timeout support.

**Sources**: [Get trades for a user or markets](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets),
[Market WebSocket](https://docs.polymarket.com/api-reference/wss/market)

## Decision 2: Coverage defaults to all participants, taker-only is explicit

**Decision**: Request `takerOnly=false` by default (`POLYMARKET_TRADES_COVERAGE=all`); a visible
`taker-only` mode sends `takerOnly=true`. The two modes use separate boundary namespaces so switching
modes starts a fresh, explicit boundary rather than mixing identity spaces.

**Rationale**: FR-015 and G-032. The ten-second probe showed every transaction in all-participant mode
carried multiple wallet observations (763 observations across 290 transactions, up to 18 per
transaction), so the provider default would silently drop most wallet evidence.

**Alternatives considered**:

- Inherit the provider default: rejected; it hides more than half of the wallet-bearing observations.
- Fetch both modes: rejected; taker rows are a subset of all-participant rows, so it doubles requests for
  no additional coverage.

## Decision 3: Every request carries a strictly increasing upper bound as its cache key

**Decision**: Each request sets `end` to `max(previous_end + 1, floor(now))` and `start` to the durable
boundary time minus the recovery horizon. `end` is both a distinct cache key and the client-enforced
cycle cutoff: newer rows are deferred until a later cycle. The client never trusts either bound as a
provider-side filter.

**Rationale**: Identical URLs were served from a shared cache (`cache-control: public, max-age=300`,
`cf-cache-status: HIT`, `age` 2 and 4, identical response hashes), while every request with a moving
`end` was a `MISS`. Rows newer than the requested `end` appeared in every sample, and moving `end`
backwards as a cursor returned 9,957 rows newer than the requested value. A separate deep-window
`offset=10000` probe returned 10,000 of 10,000 rows newer than its `end`, confirming that time bounds
cannot page deeper. Using the documented parameters keeps the request within the published contract
while producing a distinct URL per acquisition cycle; client deferral prevents those extra rows from
moving the complete-through boundary prematurely.

**Alternatives considered**:

- `Cache-Control: no-cache` request headers: rejected because the edge cache honoured neither freshness
  hints nor `age` in the probe and the behavior is undocumented.
- An undocumented cache-busting query parameter: rejected because the provider may reject unknown
  parameters and the documented ones already achieve distinct URLs.
- Trusting `start`/`end` server-side filtering: rejected by direct evidence.

## Decision 4: Boundary proof uses reach plus retained identities, never page length

**Decision**: A cycle proves continuity only when, after client-side ascending sort, the page's oldest
timestamp is strictly older than the durable boundary time and every retained identity with a timestamp
newer than that oldest row reappears in the page. A page that satisfies neither triggers exactly one
recovery request with `offset=10000`; if proof still fails the tracker enters `possible-data-loss` and
freezes the durable boundary.

**Rationale**: FR-017. Every probe returned exactly 10,000 rows regardless of window, so page length is
meaningless as a saturation signal. Reach guarantees that every row timestamped at or after the boundary
was inside the reachable page if the provider is contiguous; the identity check detects a non-contiguous
or rewritten page. The offset probe showed the second page overlapping the first by 56 identities rather
than leaving a gap, because new rows shift older rows to higher offsets.

**Alternatives considered**:

- Timestamp reach alone: rejected because equal-second rows and provider rewrites would be invisible.
- Cursor pagination by moving `end` to the previous oldest timestamp: rejected by evidence; the second
  request stalled with zero new identities.
- Deeper offset pagination: rejected because the documentation rejects offsets past 10,000 with HTTP 400.
- Treating a short page as proof: rejected; the provider never returned a short page in any probe.

## Decision 5: Composite observation identity

**Decision**: The observation identity is a SHA-256 digest of the canonical tuple
(`transactionHash`, lowercase `proxyWallet`, `conditionId`, `asset`, `side`, `outcomeIndex`, canonical
decimal `price`, canonical decimal `size`, integer `timestamp`). Exact repeats of that tuple are
duplicates; any difference is a distinct observation.

**Rationale**: FR-005. All-participant transactions contain several legitimate wallet rows; two exact
repeated composite rows appeared in 763 in-window observations and 69–72 per 10,000-row page, so
transaction hash alone is not identity and exact repeats do occur.

**Alternatives considered**:

- `transactionHash` alone: rejected; it collapses maker and taker observations.
- `transactionHash` plus wallet: rejected; one wallet can appear twice in a transaction with different
  assets or sizes.
- Provider-issued row identifier: rejected; none is documented.

## Decision 6: Durable boundary and identity window live in Redis

**Decision**: Store the checkpoint hash, the identity sorted set scored by provider timestamp, and a
bounded loss-event list under a `polymarket:ingest:<source-id>:` prefix, written together in one
transactional pipeline after each proven cycle. The identity window is mirrored in memory so duplicate
checks do not round-trip; on start the window is reloaded from Redis. Identities older than the
recovery horizon behind the newest accepted timestamp are trimmed.

**Rationale**: FR-006. Redis is already a required local dependency, `fakeredis` is the approved
double, and the shared contract suite already proves sorted-set ranges and transactional pipelines
against a real loopback Redis; the new hash, `ZREMRANGEBYSCORE`, and list scenarios are added to that
suite. At observed rates the window holds roughly 45,000–65,000 members (about 4 MB).

**Alternatives considered**:

- PostgreSQL checkpoint table: rejected; it needs a migration and a downgrade path for what is
  operational cache state, and it couples acquisition cadence to database latency.
- In-memory only: rejected; a restart could not distinguish missed from processed observations.
- Redis Streams via the existing `EventPublisher`: rejected for this slice; it changes the consumer
  contract, and the pipeline delivers trades through a callback today.

## Decision 7: Emission order and crash semantics

**Decision**: New observations are delivered oldest-first through the existing `on_trade` callback.
Each observation's identity is recorded immediately after its callback returns, and the checkpoint
advances only after the whole proven page is delivered. A crash between a callback and its identity
write can therefore re-deliver at most one observation; it can never lose one.

**Rationale**: FR-004 and SC-001 favour no silent loss; downstream deduplication (scorer window) is a
second guard against a single re-delivery. Recording identities before delivery would risk silently
losing in-flight observations on crash.

**Alternatives considered**:

- Batch identity writes before delivery: rejected because a crash could lose a whole page silently.
- Exactly-once delivery: not achievable across a process crash without downstream idempotence.

## Decision 8: First start, restart, horizon, and loss events

**Decision**:

- First start (no checkpoint): fetch one page, anchor the boundary at its newest accepted timestamp,
  retain identities inside the horizon, emit nothing, record origin `first-start`.
- Restart with a checkpoint inside the recovery horizon: acquire with proof; when reachable, emit only
  identities not retained; on failed proof after the recovery page, enter `possible-data-loss` with the
  gap recorded. Being inside the horizon does not itself imply reachability.
- Restart beyond the horizon, or a frozen boundary that ages beyond the horizon: write a loss event with
  the interval and reason (`restart-beyond-horizon` or `horizon-expired`), re-anchor at the newest proven
  page without emitting pre-anchor rows, and return to `running`. Loss events are visible in status and
  logs and are kept in a bounded list of the most recent 50.
- While in `possible-data-loss`, a provisional in-memory boundary keeps proving continuity between
  consecutive pages so that each gap is one bounded interval, and new observations keep flowing with
  identity de-duplication; the durable boundary does not move.

**Rationale**: FR-006, FR-007, FR-017, the 2026-09-09 clarifications. Because rows since the boundary
only accumulate, a gap that failed proof with two pages cannot be proven later; aging into a recorded
loss event is the only honest resolution, and it is never silent. The default horizon stays at the
specification's 10 minutes; the reachable depth is 20,000 rows, which covered 320–440 seconds at
observed rates, and the evidence file states that limitation. The horizon is therefore a retention and
loss-detection bound, not a provider-backed completeness guarantee.

**Alternatives considered**:

- Stalling until an operator restarts: rejected because monitoring would stop for every burst, while the
  recorded-loss path keeps monitoring and keeps the gap visible. Patrick may still choose this stricter
  behavior before implementation; the tasks call it out as a confirmation item.
- Emitting page rows after a re-anchor: rejected because it replays history as new activity.

## Decision 9: Retry, backoff, throttling, and terminal failures

**Decision**: Transient failures (connection errors, timeouts, HTTP 408/425/429/5xx, invalid JSON) are
retried up to four times per cycle with exponential backoff (base 1 s, cap 30 s, full jitter). A
`Retry-After` header is honoured up to 60 s. HTTP 400/401/403/404/410 and an incompatible top-level
schema are terminal: the poller reports `failed`, stops, and surfaces the error. Exhausted transient
retries mark the poller `degraded` and the next cycle retries. Request timeout is 15 s.

**Rationale**: FR-008. The documentation states that exceeding the `/trades` limit throttles requests
rather than rejecting them, so slow responses must be bounded by a timeout, while HTTP 429 handling
remains defensive. Terminal codes indicate configuration or contract drift that retries cannot fix.

**Alternatives considered**:

- Unbounded retries: rejected; they hide terminal drift and can exceed the published limit.
- Treating every failure as terminal: rejected; transient network faults would stop monitoring.

## Decision 10: Request budget

**Decision**: Default cadence is one acquisition every 5 seconds, measured from request start, with at
most one recovery request per cycle. Normal operation therefore uses 2 requests per 10 seconds (1% of
the published 200 per 10 seconds); a recovery page raises it to 2%, and the worst retry burst stays
below 10 requests per 10 seconds (5%). The poll interval is configurable with a minimum of 1 second so
the 5% ceiling cannot be exceeded by configuration.

**Rationale**: FR-018 and the published limit. At observed rates one page covered roughly 160–220
seconds of all-participant history, so a five-second cycle consumes about 3% of the reachable page.

**Source**: [Rate limits](https://docs.polymarket.com/api-reference/rate-limits)

## Decision 11: Row validation and outcome repair

**Decision**: A row is structurally valid when every identity-bearing field parses (`transactionHash`, `proxyWallet`,
`conditionId`, `asset`, `side` in `{BUY, SELL}`, decimal `price`, decimal `size`, integer `timestamp`).
Missing `outcome`/`outcomeIndex` is repaired from the cached market metadata token whose id equals
`asset`; when metadata is absent the observation is emitted with an empty outcome and increments the
`unknown` outcome-resolution count. Invalid rows are counted per missing or malformed field; a bounded number of
wallet-free diagnostics (row hash, field name) is logged per cycle. Rows more than 60 seconds ahead of
the local clock are invalid `future-timestamp` rows. A structurally valid row newer than the cycle cutoff
is `deferred:future-cycle` and cannot emit or advance the durable boundary until reacquired.

**Rationale**: FR-004, US2 acceptance scenario 4, and the probe finding that 3–6 of every 290–763
in-window rows lacked `outcome`. Metadata is enrichment and must not block ingestion (FR-003).

**Alternatives considered**:

- Dropping rows without `outcome`: rejected; the wallet observation is still valid research evidence.
- Guessing the outcome from `outcomeIndex` parity: rejected; that invents data.

## Decision 12: Startup independent of the metadata crawl

**Decision**: The pipeline starts the trade poller task first and runs `MarketMetadataSync.start()` as a
separately tracked background task instead of awaiting the initial crawl. Stop cancels both in order.
`MarketMetadataSync`'s public API is unchanged.

**Rationale**: FR-003, G-004, and SC-002. The size-anomaly detector already tolerates absent metadata
through its niche fallback.

**Alternatives considered**:

- Making the metadata sync lazy inside `MarketMetadataSync`: rejected because it changes a public
  component's semantics that slice 003 health reporting will observe.

## Decision 13: Configuration compatibility

**Decision**: Add `POLYMARKET_TRADES_URL` (default `https://data-api.polymarket.com/trades`),
`POLYMARKET_TRADES_COVERAGE` (`all` | `taker-only`, default `all`),
`POLYMARKET_TRADES_POLL_INTERVAL_SECONDS` (default 5, minimum 1, maximum 60), and
`POLYMARKET_TRADES_RECOVERY_HORIZON_SECONDS` (default 600, range 60–3600). Make `POLYMARKET_WS_URL`
optional with no default, keep its WebSocket-scheme validation, emit a `WebSocketSettingDeprecationWarning`
(a `UserWarning` subclass following the existing database URL pattern) when it is set, and render it as
`(deprecated, set)` or `(not set)` in the redacted summary and configuration check. `POLYMARKET_API_KEY`
is unchanged because the CLOB client still consumes it.

**Rationale**: FR-002, FR-011, US3. A value with a non-WebSocket scheme is still rejected so the setting
is never silently reinterpreted as an HTTP source.

**Alternatives considered**:

- Rejecting `POLYMARKET_WS_URL` immediately: rejected; the constitution requires a migration path.
- Reusing `POLYMARKET_WS_URL` for the HTTP source: rejected; that is silent reinterpretation.

## Decision 14: Public Python surface

**Decision**: Keep `TradeStreamHandler`, `ConnectionState`, `WebSocketStreamStats`, and
`TradeStreamError` exported; `TradeStreamHandler.__init__` emits a `DeprecationWarning`. The pipeline
no longer constructs it. Removal is deferred to a later approved change.

**Rationale**: Constitution principle V; the specification authorizes a configuration deprecation path
but not deleting an exported class. Existing tests keep the module exercised, so Vulture stays clean.

## Decision 15: Live-safe smoke check

**Decision**: Add `scripts/trades_smoke.py`. Its deterministic core takes an injected transport and
classifies seven named cases; the `--live` flag performs one bounded anonymous request (no alerts, no
Redis writes, no wallet identifiers in output) and prints the aggregate JSON shape used by the
feasibility probes. It is never part of `verify.py` profiles or CI.

**Rationale**: FR-013, SC-005, constitution principle III. Ordinary tests and CI make no external calls.

## Decision 16: Health and metrics boundary with slice 003

**Decision**: This slice exposes `IngestionStatus`, an `on_state_change` callback, and Prometheus
metrics from the poller module; it records terminal failures on pipeline stats. Wiring readiness routes,
pipeline state propagation, and CLI exit on terminal ingestion failure remain owned by slice 003
(G-015, G-016).

**Rationale**: Keeps slice ownership from the gap register intact while making the evidence available.

## Resolved Unknowns

All Technical Context items are resolved; no `NEEDS CLARIFICATION` remains. The reachable-depth
limitation and the stall-versus-record choice for aged gaps are recorded for Patrick's confirmation in
[plan.md](plan.md) and [tasks.md](tasks.md).
