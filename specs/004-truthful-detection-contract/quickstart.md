# Quickstart: Truthful Detection Contract (validation guide)

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17 | **Status**: proposed.
No implementation exists; this guide defines the validation journey the future implementation
must pass. Reuses 003's assessment schema and e2e harness; no competing harness.

## Journey scenarios (each: evaluate → persist → render/dry-run)

1. **Positive monetary volume**: usable positive current volume → `volume_ratio = notional / volume`
   (±0.0001), recorded inputs, persisted row replays decision.
2. **Stale volume**: `evaluated_at - acquired_at > 600s` → unavailable, no ratio, no zero.
3. **Invalid/missing/nonpositive volume**: each distinct state → unavailable, labeled, no ratio.
4. **Source failure**: failed Gamma fetch leaves volume unavailable; CLOB metadata still usable; no carry-forward refresh.
5. **Threshold boundaries**: score exactly 0.80 qualifies (inclusive); nonce exactly 5 excluded (`< 5`);
   age exactly 48h allowed; volume ratio strictly `> 0.02`.
6. **Unknown age**: low-activity candidate, confidence exactly 0.50, labeled age-unknown, no age bonus.
7. **Overrides**: valid threshold/boundary overrides recorded per assessment and used.
8. **Restart**: persisted rows retain evidence envelope; readers keep version-specific semantics.
9. **Schema compatibility**: old `003.1`/legacy rows read unchanged; new envelope additive only.
10. **Dry-run**: full formatted output with same score/threshold/evidence; no delivery, no dedup poisoning.

## Prerequisites

Bounded current Gamma field/schema check (read-only, no wallets/secrets/trade retention);
real PostgreSQL/Redis parity per 002/003 contracts; managed quality gates green.
