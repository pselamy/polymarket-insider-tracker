# Slice 004 Human Decision Packet: proposed implementation contract

**Date**: 2026-10-17. **Scope**: approval is for the reviewed implementation contract only —
not a routine merge, production deployment, real notification, trading, or calibration proof.
Tests and implementation remain future work. Data API V2 ingestion is now the live contract on main (see spec Dependencies); this
packet is rewritten in full by the 2026-10-19 revision.

## DECISIONS FOR PATRICK (open; none is settled by this revision)

### D1 — Wallet-age source (DECISION FOR PATRICK)

Current main cannot produce a known wallet age (`profiler/chain.py:485-532` returns no first
transaction for any wallet), and the profiled address is the V2 `proxy_wallet`, whose nonce meaning is
**unverified**. Full analysis: [research.md](research.md) R09.

| Option | Live behaviour | Main trade-off |
|---|---|---|
| **O1** age branch non-operational; score only on low-activity evidence | every wallet candidate is "low-activity wallet, age unknown", confidence exactly 0.50 | Truthful, no new dependency; maximum score is 0.78 (see D2) |
| **O2** Polymarket-activity age from the earliest public trade for the proxy address | "first Polymarket trade seen < 48 h ago" when full history is reachable, else unknown | Measures newness to Polymarket without the nonce question; needs a qualified per-user query, offset-capped so often only a lower bound |
| **O3** external indexer for chain first-transaction age | "first on-chain transaction < 48 h ago" | Vendor, credential/quota and cost; still subject to the proxy-address question |

**Recommended default: O1 for slice 004**, O2 as the first follow-up after the T027 qualification
receipt. If you pick O2 or O3 the spec already has the wording rules and the unknown-age fallback; the
implementation then gains a source task. Until you choose, the contract is written for O1 and says so.

### D2 — What the 0.80 threshold means when age is unknown (DECISION FOR PATRICK)

Recomputed by `evidence/reachability.py` ([research.md](research.md) R10): with age unknown the
highest attainable score is **0.780 for every trade size**; markets with ≥ $50k volume top out at
0.45 (unknown age) or 0.642 (best-case known age); main today (003.1 rules, volume never passed) tops
out at 0.528. The checked-in 114-case replay's "0 → 15 qualifications" is entirely known-age cases.
Unknown-age live-realistic replay: 0 of 40 qualify at 0.80, 7 at 0.78.

| Option | Consequence | Main trade-off |
|---|---|---|
| **R0** keep 0.80 and exact-0.50; publish the ceilings | with D1 = O1 the monitor records assessments but nothing qualifies by default; `DETECTOR_ALERT_THRESHOLD` remains the explicit, per-assessment-recorded override | no unsupported calibration claim; no deliverable alerts out of the box |
| **R1** lower the default to ≤ 0.78 | saturated niche unknown-age cases alert | changes what an alert means with no outcome data; the scorer comment's "cost-adjusted backtest" justification has no reproducible evidence in the repo (G-028) |
| **R2** unknown-age base ≥ 0.541 | keeps 0.80 reachable | unknown age would outscore a known 24 h wallet; breaks FR-008 |
| **R3** two tiers (alert 0.80 + lower "research watch") | surfaces the 0.78 class without calling it an alert | new product surface; needs its own spec |
| **R4** re-weight / remove niche double counting | changes every ceiling | model change needing calibration data |

**Recommended default: R0.** It is the only option that adds no unvalidated claim, but you should
decide knowingly that with D1 = O1 the default configuration produces no deliverable alert.

## Final behavior (if authorized)

Two-group research core (fresh/low-activity wallet + size anomaly; niche as size modifier),
threshold 0.80 inclusive, effective exclusive boundary `B` (default `< 5`; boundary 3
excludes nonce 3, boundary 10 includes nonce 9; nonce exactly `B` never qualifies), strict
volume-ratio `> 0.02`, unknown-age confidence exactly 0.50 with no age bonus, seven
distinct volume-availability states (never measured zero), per-page acquisition stamps with
a 0–600s local recency rule, immutable evaluation evidence in a versioned `004.1` envelope
amending shared 003 storage (no new migration), additive evaluation API with a deprecated
inclusive-alias path.

## Evidence and limitations

- 114-case / 228-score / 63-delta behavioral replay (hashes in
  `evidence/implementation-validation.md`); default-runtime 99 cases go 0 → 15 qualification;
  9 override cases separated. Comparison only, not calibration.
- R01 monetary-volume interpretation grounded in reviewed first-party integration
  (`outputs/polymarket-gamma-unit-primary-review.md`); archived 2025-12-22 example, not a
  current warranty; no USDC-settlement/freshness/aggregation claims; no total-volume fallback;
  bounded current schema check still required at implementation entry.
- Scoring runtime verified unchanged, so replay conclusions are reusable in scope.

## Persistence/API compatibility

Additive envelope inside existing TEXT column; old `003.1`/legacy rows untouched;
`multi_signal_bonus_3` retained neutral; exclusive-boundary keyword with deprecated alias
(conflicts rejected); nullable impacts never render as zero. Named conflict: direct
`scoring_config` JSON consumers must tolerate additive keys.

## Operational assumptions

Slice 001 owns near-real-time anonymous participant trade polling (5s cadence); Gamma
market metadata syncs every 300s with a 600s cache TTL
(`ingestor/metadata_sync.py:41-42`). Local 600s acquisition-age rule (matches cache TTL,
not a provider SLA); conservative omission on fetch failure; dry-run safety per G-018.

## Exact scope to authorize

Approve or reject the proposed contract described here and in plan/research/data-model/
contracts/tasks. Do not decide whether a factual provider claim is true — evidence category
and limits are stated above. No routine technical choice is reserved as a new blocker.
