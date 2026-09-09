# Contract: Durable Observation Boundary

## Purpose

Defines the durable state that makes overlapping acquisition safe, the proof required before the
boundary advances, the recovery path, the possible-data-loss state, and the crash semantics. It is
implemented by `ingestor/observation_boundary.py` and driven by `ingestor/trade_poller.py`.

## Keys

All keys share the prefix `polymarket:ingest:<source-id>:` where `source-id` is the first 12 hex
characters of `sha256("<trades_url>|<coverage>")`. Changing the URL or the coverage mode therefore
starts a fresh boundary with first-start semantics rather than mixing identity spaces.

| Key | Type | Content |
|---|---|---|
| `checkpoint` | hash | `schema_version`, `boundary_time`, `boundary_origin`, `written_at`, `coverage`, `last_request_end` |
| `identities` | sorted set | member = observation identity, score = provider timestamp |
| `loss-events` | list | newest-first JSON loss events, trimmed to 50 |

Every boundary advance writes `checkpoint`, the new `identities` members, the `identities` trim, and any
`loss-events` push in one `MULTI`/`EXEC` pipeline. A failed write leaves the previous checkpoint intact
and the cycle is reported as a failure.

## Proof Before Advance

Given the durable boundary time `T` and the retained identity window `W`, and a cycle's parsed page
`P` (valid rows at or before the cycle cutoff only, padding included, sorted ascending by
`(timestamp, identity)`):

```text
oldest     = min(timestamp(row) for row in P)
reach      = oldest < T
expected   = { identity ∈ W : score(identity) > oldest }
continuity = expected ⊆ identities(P)
proven     = reach and continuity
```

- `P` empty: not proven, not a loss; `last_success_at` updates, nothing is emitted, `T` unchanged.
- `proven`: emit new observations, retain their identities, set `T' = max(timestamp(row) for eligible
  rows)`, trim identities with score `< T' - horizon`, write `boundary_origin = proven`.
- not `proven`: perform exactly one recovery request (`offset=10000`), merge by identity, and evaluate
  again. If still not proven, open a gap `[T, oldest)` and enter `possible-data-loss`.

Page length is never an input to the proof.

Rows newer than the cycle cutoff are counted as `deferred:future-cycle` and excluded from proof,
emission, identity retention, and `T'`. They are eligible for reacquisition in a later cycle. Thus the
durable `T` is a complete-through claim, not a newest-seen watermark.

## Candidate Selection and Emission

- Candidate rows are valid rows with `timestamp >= newest_accepted - horizon`; older rows are
  `padding`.
- A candidate whose identity is in `W` is a `duplicate`; otherwise it is emitted oldest-first through
  the trade callback, then its identity is added to `W` and to the pending pipeline write.
- Multiple rows sharing a transaction hash are distinct observations unless their full identity tuple is
  equal; exact repeats within one page are suppressed once and counted as duplicates.

## Possible-Data-Loss State

- The durable `T` is frozen; the gap interval and `pages_examined` are held in memory.
- A provisional boundary `T_p` (newest accepted timestamp of the last page) is used to prove
  continuity between consecutive pages with the same rule, so every unproven interval becomes its own
  gap record.
- New observations keep flowing with identity de-duplication; `W` keeps growing behind `T_p` and is
  trimmed relative to `T_p`.
- Exit paths:
  - a later page proves against the frozen `T`: the gap closes without a loss event and normal advance
    resumes;
  - `T` becomes older than the horizon: every open gap is written as a loss event with reason
    `horizon-expired`, `T' = T_p`, `boundary_origin = re-anchored`, state returns to `running`.

## First Start and Restart

| Situation | Behavior |
|---|---|
| No checkpoint, or a checkpoint whose `coverage` differs from the configuration | First start: fetch one page, `T = newest accepted`, retain identities within the horizon, emit nothing, `boundary_origin = first-start` |
| Checkpoint with an unknown `schema_version` | Terminal configuration error; the poller enters `failed` and touches nothing |
| Checkpoint age `now - T <= horizon` | Reload `W` from Redis, then run the normal proof; if reachable, emitted rows are exactly the candidates not in `W`; otherwise enter `possible-data-loss` |
| Checkpoint age `now - T > horizon` | Write a loss event `[T, newest accepted]` with reason `restart-beyond-horizon`, re-anchor without emission |

The reachable depth is 20,000 rows, so the 10-minute horizon does not guarantee 10-minute recovery. A
restart inside the horizon can still fail proof after roughly five to seven minutes at the observed
rates; that produces the possible-data-loss path above, not a silent skip.

## Crash Semantics

- The identity of an observation is written after its callback returns, in the order of emission.
- The checkpoint advances after the whole proven page is delivered.
- A crash between a callback and its identity write re-delivers at most that one observation on
  restart; no observation is lost silently. The scorer's existing deduplication window is the second
  guard for that single case.
- A crash between identity writes and the checkpoint write leaves `T` at its previous value; the next
  proof still holds because the identities were retained, and the candidates are suppressed as
  duplicates.

## Loss Event

```json
{"from_time": 1788983716, "to_time": 1788983935, "reason": "horizon-expired",
 "detected_at": 1788983940, "recorded_at": 1788984320, "pages_examined": 2}
```

Reasons: `horizon-expired`, `restart-beyond-horizon`, `continuity-mismatch`. Events are logged at
`ERROR` when written, exposed in status (`loss_events`, newest five), and counted by the metric
`polymarket_ingest_loss_events_total{reason}`.

## Real and Fake Parity

The operations used by this contract (`HSET`/`HGETALL`, `ZADD`, `ZRANGEBYSCORE`, `ZREMRANGEBYSCORE`,
`ZCARD`, `LPUSH`/`LTRIM`/`LRANGE`, and a transactional pipeline that contains all of them) are added as
scenarios to `tests/integration/test_redis_contract.py`, which runs against `fakeredis` in every unit run
and against the real loopback Redis in the `services` profile. The in-memory mirror of `W` must equal
the Redis sorted set after every cycle in tests. `FLUSHDB` is never used; every scenario writes inside a
unique namespace it deletes.
