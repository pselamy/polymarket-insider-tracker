# Contract: Source Acquisition

## Purpose

Defines how the tracker acquires public wallet-bearing trades from the officially documented trades
query, what it sends, what it accepts, how it retries, and what it never does. Consumers are
`ingestor/trade_poller.py`, the live-safe smoke check, and the tests that fake the provider.

## Endpoint and Request

```text
GET {POLYMARKET_TRADES_URL}?limit=10000&offset={0|10000}&takerOnly={false|true}&start={S}&end={E}
```

| Parameter | Value | Rule |
|---|---|---|
| `limit` | `10000` | Documented maximum; values above are clamped by the provider |
| `offset` | `0`, or `10000` for the single recovery request | Documented maximum; larger offsets are rejected with HTTP 400 and are never sent |
| `takerOnly` | `false` for coverage `all`, `true` for `taker-only` | Always sent explicitly; the provider default is never inherited |
| `start` | `boundary_time - recovery_horizon_seconds`, or `0` on first start | Documented intent only; never trusted as a filter |
| `end` | `max(previous_end + 1, floor(local_now))` | Strictly increasing cache key and cycle cutoff; the cycle waits if needed so it is not ahead of the local clock |

Headers: `Accept: application/json` and `User-Agent: polymarket-insider-tracker/<version>`. No
authentication header is ever sent to this endpoint. Request timeout is 15 seconds.

## Response Acceptance

| Condition | Classification | Behavior |
|---|---|---|
| HTTP 200 with a JSON list | success | Rows are parsed by the row contract; an empty list is a successful empty acquisition |
| HTTP 200 with a non-list JSON body or unparseable body after retries | terminal `incompatible-schema` | Poller enters `failed` |
| HTTP 408, 425, 429, or any 5xx | transient | Retry with backoff; `Retry-After` honoured up to 60 s |
| Transport error or timeout | transient | Retry with backoff |
| HTTP 400, 401, 403, 404, 410 | terminal | Poller enters `failed` with the status and a redacted URL |
| Any other status | terminal | As above |

Transient retries: at most four per request (five attempts), delay `min(30, 1 * 2**attempt)` scaled by a
uniform random factor in `[0, 1]` (full jitter), plus any `Retry-After` value up to 60 s. After
exhaustion the cycle fails, the poller enters `degraded`, and the next cycle starts after the poll
interval.

The documentation states that exceeding the `/trades` limit throttles requests instead of rejecting
them; the timeout therefore bounds throttling, and the 429 path is defensive.

## Request Budget

| Situation | Requests per 10 s | Share of published 200 |
|---|---|---|
| Healthy 5 s cadence | 2 | 1% |
| Healthy cadence with one recovery page per cycle | 4 | 2% |
| Worst retry burst (five attempts within 10 s plus the next cycle) | at most 8 | 4% |
| Configured minimum interval of 1 s without retries | 10 | 5% |

A cycle never starts earlier than one poll interval after the previous cycle started. The status
field `requests_last_10s` and the metric `polymarket_ingest_requests_total` make the budget observable.

## Ordering and Time Bounds

- The provider returns newest-first; the client always sorts ascending by `(timestamp, identity)` and
  never relies on provider order.
- Rows newer than the requested `end` are `deferred:future-cycle`: they are counted but do not emit,
  enter identity state, or advance the complete-through boundary. They are reacquired after a later
  cycle cutoff reaches their timestamp. Rows older than the requested `start` are expected and are
  classified by the boundary contract rather than trusting the provider-side filter.
- A row timestamped more than 60 seconds after the local clock is invalid (`future-timestamp`).

## Row Contract

Required fields and parsing rules are defined in [data-model.md](../data-model.md#trade-observation).
`outcome` and `outcomeIndex` are optional and repairable from cached market metadata. The parser never
substitutes defaults for missing identity-bearing fields.

## What Acquisition Never Does

- Never sends a credential, cookie, or API key to the trades endpoint.
- Never requests `offset` above 10,000 or `limit` above 10,000.
- Never falls back to a WebSocket, an undocumented topic, or a different host when the documented
  endpoint fails; failures are surfaced as `degraded` or `failed`.
- Never logs a wallet address, name, pseudonym, or raw row; diagnostics use row hashes and field names.
- Never runs during ordinary tests or CI; tests use `tests/fakes/trades.py` behind `httpx.MockTransport`.

## Fake Provider Contract (tests)

`FakeTradesServer` must model the observed provider faithfully so tests are evidence:

- serves pages newest-first from a synthetic ledger keyed by request parameters, always filling to
  `limit` when enough rows exist, ignoring `start`/`end` as filters, and honouring `offset`;
- returns the identical body for an identical URL until the ledger changes, to model the shared cache;
- can inject HTTP 429 with `Retry-After`, 5xx, timeouts, malformed rows, rows without `outcome`, exact
  duplicate rows, rows newer than `end`, and a non-list body;
- records every request (URL, parameters, time) so budget and cache-busting assertions are exact;
- uses only synthetic wallet addresses.
