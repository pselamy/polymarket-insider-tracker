# Feasibility Record: Supported Trade Ingestion

**Recorded**: 2026-09-09 (probes captured 2026-09-09T19:45:14Z through 2026-09-09T19:59:55Z)

**Requirement**: FR-016 and SC-007 of [spec.md](../spec.md); condition of Decision A1 in
[review-gate.md](../../audit/review-gate.md)

**Method status**: aggregate-only, read-only, anonymous; no wallet identifiers retained; no alert sent;
no trade placed; no credential used

## Official Sources (verified 2026-09-09)

- [Get trades for a user or markets](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets):
  anonymous `GET https://data-api.polymarket.com/trades`; `limit` default 100, maximum 10,000 (values
  above are clamped); `offset` default 0, maximum 10,000 (requests past the cap are rejected with 400);
  `takerOnly` default `true`; `start`/`end` epoch-second bounds; response rows include `proxyWallet`,
  `side`, `asset`, `conditionId`, `size`, `price`, `timestamp`, `title`, `slug`, `icon`, `eventSlug`,
  `outcome`, `outcomeIndex`, `name`, `pseudonym`, `bio`, `profileImage`, `profileImageOptimized`, and
  `transactionHash`.
- [Rate limits](https://docs.polymarket.com/api-reference/rate-limits): Data API `/trades` 200 requests
  per 10 seconds; exceeding a limit throttles (delays or queues) requests rather than rejecting them.
- [Market WebSocket](https://docs.polymarket.com/api-reference/wss/market): public last-trade messages
  carry no trader wallet, so the channel cannot replace the research source.

## Probe Method

Each probe issued anonymous `GET` requests to the documented endpoint with `limit=10000`, an explicit
`takerOnly` value, and `start`/`end` computed from the local clock. For every response the probe
recorded the HTTP status, elapsed time, `cache-control`, `cf-cache-status`, and `age` headers, the
SHA-256 of the raw body, the row count, the oldest and newest `timestamp`, the number of rows inside
and outside the requested window, rows newer than the requested `end`, adjacent-pair ordering
violations in both directions, rows missing a required field (by field name), exact repeated composite
rows, and, for the transaction analysis, the number of unique `transactionHash` values, the maximum
observations per transaction, and the number of transactions with more than one wallet. Composite
identities were hashed in memory for overlap counting. Wallet, name, pseudonym, and profile fields were
discarded before any aggregate was written; the raw bodies were not retained. The five aggregate
records are committed under [probes/](probes/) and each declares `retained_wallet_identifiers: false`;
a repository grep for 40-hex-character addresses over those files returns zero matches.

| Record | SHA-256 |
|---|---|
| `probes/slice001-live-feasibility-20260909.json` | `72556dcf585c713f1d3813ae9f7d9ad52b5a49170c3c9b9e303d4b6738f4eff0` |
| `probes/slice001-cache-boundary-20260909.json` | `c61080b81fb2c9617ab72e4e09ef54e9e3cda96853b2cc8e58b53b78798b92a7` |
| `probes/slice001-window-semantics-20260909.json` | `ba205cbe3503b79f507b70983bc3a94ab902f675abad02ccb2cad7da2828ef28` |
| `probes/slice001-offset-boundary-20260909.json` | `42af60bc21a8363c6500f8347e5b26f9bda353c7a7941f4cc3df094d0e9f3719` |
| `probes/slice001-ten-minute-recovery-20260909.json` | `9a4bf84028e6afc3b93116db1c4c992b59aac4b452bb7f567f5e1e2eee79fa3d` |

Sixteen requests were made in total over about fifteen minutes, never more than two within any ten
seconds (at most 1% of the published limit). The probe script was a throwaway; the implementation's
`scripts/trades_smoke.py` ([contracts/live-smoke.md](../contracts/live-smoke.md)) formalizes the same
record shape.

## Sample 1: Six Five-Second Windows

All six responses returned HTTP 200, `cache-control: public, max-age=300`, `cf-cache-status: MISS`,
and exactly 10,000 rows. Descending-order violations were 0 in every sample (newest-first).

| # | takerOnly | Window (epoch s) | Rows | In window | Out of window | Oldest | Newest | Page span (s) | In-window rows/s | Newest lag (s) | Missing `outcome` | Exact repeats | Elapsed (ms) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | true | 1788983108–1788983113 | 10,000 | 175 | 9,825 | 1788982587 | 1788983113 | 526 | 35.0 | 0 | 111 | 0 | 965 |
| 1 | false | 1788983108–1788983113 | 10,000 | 547 | 9,453 | 1788982950 | 1788983113 | 163 | 109.4 | 0 | 88 | 71 | 891 |
| 2 | true | 1788983113–1788983118 | 10,000 | 173 | 9,827 | 1788982599 | 1788983118 | 519 | 34.6 | 0 | 111 | 0 | 1,352 |
| 2 | false | 1788983113–1788983118 | 10,000 | 437 | 9,563 | 1788982957 | 1788983119 | 162 | 87.4 | -1 | 80 | 72 | 983 |
| 3 | true | 1788983118–1788983123 | 10,000 | 199 | 9,801 | 1788982609 | 1788983124 | 515 | 39.8 | -1 | 115 | 0 | 1,032 |
| 3 | false | 1788983118–1788983123 | 10,000 | 478 | 9,522 | 1788982965 | 1788983124 | 159 | 95.6 | -1 | 86 | 69 | 1,073 |

"Newest lag" is `end - newest_timestamp`; a negative value means a row newer than the requested `end`
was returned. "Missing `outcome`" counts rows across the whole page that lacked the field; no other
required field was missing.

## Sample 2: Cache Behavior

| Label | Window | HTTP | `cf-cache-status` | `age` | Elapsed (ms) | Rows | In window | Rows newer than `end` | Missing `outcome` | Body SHA-256 (prefix) |
|---|---|---|---|---|---|---|---|---|---|---|
| fixed-repeat-1 | 1788983179–1788983184 | 200 | MISS | – | 908 | 10,000 | 305 | 0 | 80 | `df694a29` |
| fixed-repeat-2 | same URL | 200 | HIT | 2 | 402 | 10,000 | 305 | 0 | 80 | `df694a29` |
| fixed-repeat-3 | same URL | 200 | HIT | 4 | 388 | 10,000 | 305 | 0 | 80 | `df694a29` |
| one-second | 1788983183–1788983184 | 200 | MISS | – | 1,243 | 10,000 | 61 | 214 | 94 | `78eb9c80` |

Identical URLs were served from the shared edge cache with identical bodies; a different URL was a
miss. The one-second request, issued about nine seconds after its `end`, returned 214 rows newer than
that `end`.

## Sample 3: Ten-Second Window Transaction Analysis

Window 1788983459–1788983469, captured about two seconds after `end`.

| takerOnly | Rows returned | In window | Rows/s | Unique transactions | Max observations per transaction | Transactions with several wallets | Exact repeats in window | Missing `outcome` in window | Oldest returned before window | Elapsed (ms) |
|---|---|---|---|---|---|---|---|---|---|---|
| true | 10,000 | 290 | 29.0 | 290 | 1 | 0 | 0 | 3 | yes | 1,024 |
| false | 10,000 | 763 | 76.3 | 290 | 18 | 290 | 2 | 6 | yes | 956 |

The same 290 transactions produced 763 all-participant observations; every transaction carried more
than one wallet row.

## Sample 4: Offset Pagination (Documented Recovery Page)

Window 1788983393–1788983993 (600 s), `takerOnly=false`, captured 2026-09-09T19:59:55Z.

| Offset | HTTP | Rows | Oldest | Newest | Span (s) | Reach behind capture (s) | New identities | Overlap with previous page | Outside bounds | Elapsed (ms) |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 200 | 10,000 | 1788983791 | 1788983992 | 201 | 202 | 10,000 | – | 0 | 966 |
| 10,000 | 200 | 10,000 | 1788983554 | 1788983793 | 239 | 439 | 9,944 | 56 | 0 | 1,048 |

Unique composite identities across both pages: 19,801 (143 exact repeats). The second page overlapped
the first rather than leaving a gap: new rows published between the two requests shifted older rows to
higher offsets. Two pages reached 439 seconds behind the capture time.

## Sample 5: Moving `end` Backwards as a Cursor (Rejected)

Target window 1788983336–1788983936 (600 s), captured 2026-09-09T19:58:58Z.

| Page | Requested `end` | Rows | Eligible rows at or before `end` | Rows newer than `end` | Oldest eligible | Newest eligible | Span (s) | New identities | Result |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1788983936 | 10,000 | 9,940 | 60 | 1788983716 | 1788983935 | 219 | 9,940 | target not reached |
| 2 | 1788983716 | 10,000 | 43 | 9,957 | 1788983716 | 1788983716 | 0 | 0 | stalled |

Setting `end` to the previous oldest timestamp returned the same newest page with 9,957 rows newer
than the requested bound. The upper bound is not applied to the newest-first page; cursor pagination
by time is not available, and `offset` is the only documented way to reach older rows.

## Interpretation Against the Approval Conditions

| Condition (Decision A1) | Finding | Status |
|---|---|---|
| Sustainable cadence within the published limit | A 5-second cadence uses 2 requests per 10 s (1%); with the recovery page 2%; the configured minimum of 1 s stays at 5% | Met |
| Ordering and replay handling | Every page was newest-first but the order is undocumented; the client sorts, de-duplicates by composite identity, and proves reach plus identity continuity | Met by design; validated by probes |
| Publication lag | Newest rows were timestamped within one second of the request clock (lag 0 or -1 s); latency 388–1,352 ms | Met; late-arrival lag beyond the newest row not measurable from these probes |
| Cache behavior | Fixed URLs cached up to 300 s; a strictly increasing `end` produced a distinct URL and a miss every time | Met |
| Visible page-saturation and loss detection | Every response was full, so length is never evidence; reach plus retained identities, one `offset=10000` recovery page, then a visible possible-data-loss state | Met by design; reachable depth is 20,000 rows |
| No wallet identity retained | All records aggregate-only with `retained_wallet_identifiers: false` | Met |

Derived capacity at observed all-participant rates (45–110 rows/s): one page covers roughly 160–220 s;
a 5-second cycle consumes 3–5% of a page; proof would first fail only if more than about 10,000 rows
were published inside one cycle (about 1,500 rows/s sustained, 14–33 times the observed rates), and
the recovery page doubles that.

## Privacy Guarantees

- No wallet address, name, pseudonym, biography, or profile image value was written to any file.
- Response bodies were hashed and discarded; only counts, timestamps, headers, and digests remain.
- The committed probe records were grep-checked for 40-hex-character addresses (zero matches) before
  commit, and the implementation's smoke tests assert the same invariant on every record.

## Conclusion

The feasibility gate for planning passes. The documented public trades query supplies wallet-bearing
rows for all markets anonymously; a five-second all-participant cadence is sustainable at about 1% of
the published limit with an order of magnitude of page headroom; shared caching is defeated by a
moving documented parameter; ordering, duplicate, and outcome-gap behaviors are characterized; and the
boundary proof plus the single documented recovery page give a testable, visible loss-detection
contract. Decision A1's conditions are satisfied for planning, not for convergence: the pre-convergence
live-safe smoke run must repeat the reachability, schema, lag, saturation, and cache checks.

## Limitations

- All samples come from one fifteen-minute period on one day; peak-event throughput was not observed.
- Reachable depth is 20,000 rows, so the 10-minute recovery horizon is time-bounded but not
  rate-guaranteed: at observed rates two pages covered 320–440 s. Longer outages produce recorded loss
  events by design.
- Provider order, the `start`/`end` semantics observed here, and cache policy are behaviors, not
  documented guarantees; the implementation relies only on the documented parameters and treats the rest
  defensively.
- Late-arriving rows (published well after their `timestamp`) could not be measured with these probes;
  the identity window inside the horizon makes them visible as `late` emissions rather than losses.
- The throwaway probe script is not in the repository; the smoke script defined by the live-smoke
  contract replaces it and is tested deterministically.

## Product Stop Gate

Implementation MUST stop for Patrick's product decision, and MUST NOT substitute any undocumented or
withdrawn interface, if any of the following is observed during implementation or the pre-convergence
live-safe smoke run:

1. Any anonymous request to the documented trades endpoint returns 401/403 or the response omits
   `proxyWallet`, `transactionHash`, `conditionId`, `asset`, `side`, `price`, `size`, or `timestamp`.
2. `takerOnly=false`, `limit=10000`, or `offset=10000` is rejected or ignored (fewer than the
   documented rows are reachable), or an identical body is served for a request with a strictly
   increasing `end` (`cf-cache-status: HIT`, equal hash).
3. In a bounded live-safe run of at least twelve consecutive five-second cycles, any cycle fails the
   boundary proof even after the recovery page, or any single page spans fewer than 30 seconds of
   history (sustained rate above about 330 rows/s).
4. Measured provider lag (local clock minus newest accepted timestamp) exceeds 60 seconds in two
   consecutive cycles, which would make the freshness claim in SC-002 untrue.
5. The invalid-row share of a page exceeds 5%, or a field other than `outcome`/`outcomeIndex` is
   missing in more than 0.1% of rows, indicating schema drift beyond the repair contract.
6. Any observation would require a credential, a non-public endpoint, a WebSocket topic, or retaining
   wallet identifiers in evidence to satisfy coverage.
