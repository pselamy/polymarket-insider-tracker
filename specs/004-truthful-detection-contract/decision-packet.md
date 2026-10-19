# Slice 004 Human Decision Packet: proposed implementation contract

**Date**: 2026-10-17. **Scope**: approval is for the reviewed implementation contract only —
not a routine merge, production deployment, real notification, trading, or calibration proof.
Tests and implementation remain future work. Data API V2 ingestion is now the live contract on main (see spec Dependencies); this
packet is rewritten in full by the 2026-10-19 revision.

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
