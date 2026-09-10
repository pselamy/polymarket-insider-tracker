# Live-Safe Smoke Evidence: Supported Trade Ingestion

**Recorded**: 2026-09-10 (runs between 2026-09-10T06:01:30Z and 2026-09-10T06:10:50Z)

**Requirement**: FR-013, FR-016, FR-019, SC-005, SC-007; task T024

**Operator**: Claude Fable 5.1 on `dev@selamy-core`, acting for `pselamy` in the prepared worktree

**Method status**: anonymous, read-only, aggregate-only; no wallet identifier, name, pseudonym, or
raw row retained in any record below; no alert sent; no trade placed; no credential used; every
Redis key written by the cycle driver lived in a disposable loopback Redis built from source
(`redis://127.0.0.1:6390`, database 3) and was deleted at the end of the run. Nothing durable was
left behind. Total live requests across every run in this record: 66 over about nine minutes, never
more than four within any ten seconds (at most 2% of the published 200 per 10 s).

## Verdict

**Stopped at the product stop gate (condition 3).** Twelve consecutive five-second poller cycles
against the real endpoint reproducibly entered `possible-data-loss` after the first proven cycle:
the reach condition held on every cycle, but the continuity condition failed even after the
documented recovery page. Three bounded aggregate probes isolated the cause: the provider serves
rows at the newest one or two seconds with a placeholder `outcomeIndex` of `999` and replaces it with
the real index (`0` or `1`) within seconds, while `outcome`, `price`, `size`, `timestamp`, and every
identity-bearing field stay unchanged. Because the approved identity tuple includes
`outcomeIndex-or-empty` (data model, "Identity tuple"; research Decision 5), each such row changes
identity: the retained identity vanishes (continuity mismatch) and the corrected row is emitted a
second time as a new observation. Per FR-019 and the engineering contract, the identity tuple was
not changed unilaterally; see "Decision requested" below.

All other stop-gate conditions held. Reachability, schema compatibility, timestamp freshness,
cache-key distinctness, coverage, and the request budget behaved as the feasibility record
predicted. A second, implementation-level defect against FR-007 (pre-anchor history revealed by a
recovery page was emitted as new activity in the first run) was found by this run and fixed with a
regression test before the final run recorded below.

## Command 1: explicit bounded live smoke

```bash
uv run python scripts/trades_smoke.py --live --coverage both --window-seconds 5 --json
```

`POLYMARKET_TRADES_URL` unset (documented default). Exit status `0`. Two anonymous requests.
Records (verbatim, final run at 06:09:41Z):

```json
{"age": null, "cache_control": "public, max-age=300", "captured_at": "2026-09-10T06:09:42.244851+00:00", "case": "valid-wallet-bearing", "cf_cache_status": "MISS", "coverage": "all", "endpoint": "https://data-api.polymarket.com/trades", "error": null, "http_status": 200, "in_window_rows": 153, "invalid_rows": 0, "missing_required_fields": {}, "newest_timestamp": 1789020582, "oldest_timestamp": 1789020306, "page_saturated": true, "passed": true, "provider_lag_seconds": 0.903545618057251, "reachable": true, "response_sha256": "4466f2c4cfe6d82314d62c15712aa0803f329a18e2ed47338e5b61dc111a7221", "retained_wallet_identifiers": false, "retry_after_honoured": true, "row_count": 10000, "timeout": false, "transient": false, "valid_rows": 10000, "warning": null, "window_seconds": 5}
{"age": null, "cache_control": "public, max-age=300", "captured_at": "2026-09-10T06:09:43.010996+00:00", "case": "valid-wallet-bearing", "cf_cache_status": "MISS", "coverage": "taker-only", "endpoint": "https://data-api.polymarket.com/trades", "error": null, "http_status": 200, "in_window_rows": 43, "invalid_rows": 0, "missing_required_fields": {}, "newest_timestamp": 1789020583, "oldest_timestamp": 1789019887, "page_saturated": true, "passed": true, "provider_lag_seconds": 1.024064540863037, "reachable": true, "response_sha256": "fdba95e100c759abd5810234d8b3e7d822e545237586b77f2d925011e5e5dd69", "retained_wallet_identifiers": false, "retry_after_honoured": true, "row_count": 10000, "timeout": false, "transient": false, "valid_rows": 10000, "warning": null, "window_seconds": 5}
```

Both coverage modes: HTTP 200, 10,000 rows, 0 invalid rows, `cf-cache-status: MISS`, newest-row
timestamp lag about one second, `retained_wallet_identifiers: false`. The first run at 06:01:30Z
produced the same classification (`valid-wallet-bearing`, passed, lag 1.87 s and 2.05 s).

## Command 2: twelve consecutive five-second poller cycles

The driver below (kept outside the repository under the job's temporary directory) constructs the
real `TradePoller` with `PolymarketSettings` defaults, a disposable loopback Redis, and a trade
callback that only counts, then runs one anchor cycle plus twelve proof cycles five seconds apart
and prints one aggregate JSON object per cycle. It retains no row and deletes its Redis keys.

```python
"""T024 driver: twelve consecutive five-second poller cycles from a disposable Redis (aggregate only)."""
import asyncio, json, time
from redis.asyncio import Redis
from polymarket_insider_tracker.config import PolymarketSettings
from polymarket_insider_tracker.ingestor.trade_poller import TradePoller

async def main() -> None:
    redis = Redis.from_url("redis://127.0.0.1:6390/3")
    settings = PolymarketSettings(_env_file=None)
    delivered = 0
    async def on_trade(_event):  # counts only; the event is discarded
        nonlocal delivered
        delivered += 1
    poller = TradePoller(on_trade, redis=redis, settings=settings)
    previous = None
    for cycle in range(13):
        started = time.time()
        await poller.run_cycle()
        s = poller.status
        delta = {k: s.counts[k] - (previous.counts[k] if previous else 0) for k in ("emitted","duplicate","padding","anchor-history","deferred:future-cycle","invalid:future-timestamp","recovery_pages","retries")}
        invalid_total = sum(v for k, v in s.counts.items() if k.startswith("invalid:")) - ((sum(v for k, v in previous.counts.items() if k.startswith("invalid:"))) if previous else 0)
        print(json.dumps({
            "cycle": cycle, "kind": "anchor" if cycle == 0 else "proof",
            "state": s.state.value, "boundary_time": s.boundary_time, "boundary_origin": s.boundary_origin.value if s.boundary_origin else None,
            "page_rows": s.page_rows, "page_span_seconds": s.page_span_seconds,
            "provider_lag_seconds": None if s.provider_lag_seconds is None else round(s.provider_lag_seconds, 3),
            "processing_lag_seconds": round(s.processing_lag_seconds, 4),
            "cycle_delta": delta, "invalid_rows_this_cycle": invalid_total,
            "outcome_counts_total": s.outcome_counts, "requests_last_10s": s.requests_last_10s,
            "consecutive_failures": s.consecutive_failures, "last_error": s.last_error,
            "loss_events": [e.__dict__ for e in s.loss_events], "delivered_total": delivered,
            "cycle_seconds": round(time.time() - started, 3),
        }, default=str))
        previous = s
        await asyncio.sleep(max(0.0, 5.0 - (time.time() - started)))
    await poller.stop()
    keys = [k async for k in redis.scan_iter(match="polymarket:ingest:*")]
    for key in keys:
        await redis.delete(key)
    print(json.dumps({"disposable_redis_keys_deleted": len(keys)}))
    await redis.aclose()

asyncio.run(main())
```

```bash
uv run python "$CLAUDE_JOB_DIR/tmp/live_cycles.py"
```

### Final run (06:09:44Z to 06:10:50Z, after the FR-007 fix)

| Cycle | Kind | State | Boundary | Rows | Span (s) | Lag (s) | Emitted | Duplicate | Padding | Deferred | Recovery | Invalid | Req/10s | Loss events |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | anchor | `running` | 1789020583 | 10000 | 276 | 3.012 | 0 | 51 | 0 | 4 | 0 | 0 | 1 | 0 |
| 1 | proof | `running` | 1789020589 | 10000 | 280 | 1.593 | 89 | 9911 | 0 | 0 | 0 | 0 | 2 | 0 |
| 2 | proof | `possible-data-loss` | 1789020589 | 20000 | 588 | 1.893 | 146 | 10137 | 9717 | 0 | 1 | 0 | 3 | 0 |
| 3 | proof | `possible-data-loss` | 1789020589 | 20000 | 589 | 2.77 | 206 | 10237 | 9542 | 15 | 1 | 0 | 4 | 0 |
| 4 | proof | `possible-data-loss` | 1789020589 | 20000 | 594 | 1.823 | 165 | 10475 | 9360 | 0 | 1 | 0 | 4 | 0 |
| 5 | proof | `possible-data-loss` | 1789020589 | 20000 | 591 | 1.84 | 398 | 10573 | 9029 | 0 | 1 | 0 | 4 | 0 |
| 6 | proof | `possible-data-loss` | 1789020589 | 20000 | 593 | 0.962 | 247 | 10929 | 8824 | 0 | 1 | 0 | 4 | 0 |
| 7 | proof | `possible-data-loss` | 1789020589 | 20000 | 591 | 1.717 | 244 | 11113 | 8643 | 0 | 1 | 0 | 4 | 0 |
| 8 | proof | `possible-data-loss` | 1789020589 | 20000 | 586 | 0.95 | 377 | 11435 | 8188 | 0 | 1 | 0 | 4 | 0 |
| 9 | proof | `possible-data-loss` | 1789020589 | 20000 | 580 | 2.701 | 490 | 11650 | 7860 | 0 | 1 | 0 | 4 | 0 |
| 10 | proof | `possible-data-loss` | 1789020589 | 20000 | 564 | 1.735 | 755 | 12071 | 7174 | 0 | 1 | 0 | 4 | 0 |
| 11 | proof | `possible-data-loss` | 1789020589 | 20000 | 557 | 1.823 | 293 | 12774 | 6933 | 0 | 1 | 0 | 4 | 0 |
| 12 | proof | `possible-data-loss` | 1789020589 | 20000 | 546 | 0.797 | 350 | 13012 | 6638 | 0 | 1 | 0 | 4 | 0 |


Delivered downstream by the counting callback: 3,760 observations over the twelve proof cycles;
outcome resolution `provided` for every one (no row lacked `outcome`); `consecutive_failures` 0 and
`last_error` null on every cycle; no loss event written (the frozen boundary was younger than the
600 s horizon when the run ended). Padding on cycles 2–12 is pre-anchor history revealed by the
recovery page and correctly withheld (FR-007).

### First run (06:01:33Z to 06:02:39Z, before the FR-007 fix)

Same shape: cycle 0 anchored at 1789020093, cycles 1–2 proven (70 and 194 emitted), cycle 3 and
every later cycle `possible-data-loss` with one recovery page each. Cycle 3 emitted 9,731 rows,
almost all of them pre-anchor history inside the 600 s horizon but deeper than the anchor page.
That replay violated FR-007 and was fixed (candidate floor bounded by the oldest retained identity;
`tests/ingestor/test_trade_poller.py::TestPreStartHistory`). The final run shows those rows as
`padding`.

## Diagnostics (aggregate-only, bounded)

1. **Poller first failed proof** (06:05Z, 2 requests): at cycle 1 the durable proof reached
   (page oldest 1789020007 < boundary 1789020313) but 38 of 9,927 retained identities were missing,
   all with score exactly equal to the anchor boundary second and all first seen in the anchor page.
2. **Newest-second persistence** (06:06Z, 6 requests, three page pairs 5 s apart): 53, 70, and 45
   rows at the newest eligible second of the first page were missing from the second page by full
   identity; 100% of them reappeared when the identity ignored `outcomeIndex`; the only differing
   field was `outcomeIndex`; timestamp shift 0.
3. **Transition shape** (06:08Z, 2 requests): of 9,802 rows present in both pages, 9,766 kept their
   `outcomeIndex`; 36 changed, all `999 -> 1` (20) or `999 -> 0` (16), all aged two seconds at the
   first request, `outcome` string unchanged in every case, and no row lacked `outcomeIndex`.
4. **Ten-second pair** (06:04Z, 2 requests): 0 of 9,541 expected identities missing, showing the
   effect is confined to the newest one or two seconds of a page.

## Product stop gate evaluation

| # | Condition | Observation | Result |
|---|---|---|---|
| 1 | 401/403 or a missing identity-bearing field | HTTP 200 on every request; `missing_required_fields` empty; 0 invalid rows in 26 pages | Held |
| 2 | `takerOnly=false`, `limit=10000`, or `offset=10000` rejected or ignored; identical body for a strictly increasing `end` | 10,000 rows on every primary and recovery page; `cf-cache-status: MISS` on every smoke request; distinct `end` per cycle by construction | Held |
| 3 | Any cycle fails the boundary proof after the recovery page, or a page spans < 30 s | Cycles 2–12 (final run) and 3–12 (first run) unproven after the recovery page; page spans 276–594 s | **Tripped** (root cause: provider `outcomeIndex` placeholder, above) |
| 4 | Newest-row timestamp lag > 60 s in two consecutive cycles | Lag 0.8–3.0 s on every cycle | Held |
| 5 | Invalid-row share > 5%, or a non-outcome field missing in > 0.1% of rows | 0 invalid rows in every page | Held |
| 6 | Coverage would require a credential, non-public endpoint, WebSocket topic, or retained wallet identifiers | Anonymous `GET` only; records aggregate-only | Held |

## Decision requested from Patrick

The provider back-fills `outcomeIndex` after first publication. Options, in recommended order:

1. **Remove `outcomeIndex` from the identity tuple** (asset already identifies the outcome token
   within a market, so the index carries no identity information) and treat an `outcomeIndex` that
   is not a valid token index (such as `999`) as unknown, repairable from cached metadata like a
   missing one, so the placeholder never reaches persisted assessments. This changes research
   Decision 5 and the data model's identity tuple and therefore needs approval before implementation.
2. Keep the tuple and accept a permanent `possible-data-loss` state with a loss event every horizon
   and duplicate downstream delivery of every trade. Not recommended; it defeats FR-005 and SC-001.

Until a decision is recorded, T024 is incomplete and the slice cannot converge; every deterministic
gate is green and the live evidence above is complete.

## Privacy check

Every record above was grep-checked for 40-hex-character addresses before commit (zero matches
outside the synthetic test fixtures); response bodies were hashed and discarded.
