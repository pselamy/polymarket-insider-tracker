# Quickstart: Truthful Detection Contract (validation guide)

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17, repaired 2026-10-17 | **Status**: proposed.
No 004 implementation exists; this guide defines the validation journey the future
implementation must pass. Reuses 003's assessment schema and e2e harness; no competing harness.

## Runnable today (planning packet + existing suite)

These commands execute against the current tree with no 004 code; they verify the base the
proposal builds on:

```bash
uv sync --locked --all-extras --python 3.13
uv run pytest tests/detector/ tests/ingestor/ -q
SPECIFY_FEATURE_DIRECTORY=specs/004-truthful-detection-contract \
  .specify/scripts/bash/check-prerequisites.sh --json --require-spec --require-tasks --include-tasks
```

Expected today: existing detector/ingestor suites green; prerequisite helper exits 0 with
all five 004 artifacts listed (document check only — no runtime test credit inferred).

## Future journey scenarios (each: evaluate → persist → render/dry-run)

Each scenario names its implementation task(s), test obligation, and evidence receipt:

1. **Positive monetary volume** (T011/T012 → `tests/integration/test_end_to_end.py` →
   persisted row): usable positive current volume → `volume_ratio = notional / volume`
   (±0.0001), recorded inputs, persisted row replays decision.
2. **Stale volume** (T012 → ingestor table test): `evaluated_at - acquired_at > 600s` →
   unavailable, no ratio, no zero.
3. **Invalid/missing/nonpositive volume** (T012/T009d): each distinct state → unavailable,
   labeled, no ratio; raw value retained verbatim.
4. **Source failure** (T012/T019): failed Gamma fetch leaves volume unavailable; CLOB
   metadata still usable; no carry-forward refresh.
5. **Threshold boundaries** (T009d/T015): score exactly 0.80 qualifies (inclusive); nonce
   exactly `B` excluded (`< B`); boundary-3/nonce-3 and boundary-10/nonce-9 overrides
   decided by captured `B`; age exactly 48h allowed; volume ratio strictly `> 0.02`.
6. **Unknown age** (T009c joint regression): low-activity candidate, confidence exactly
   0.50, labeled age-unknown, no age bonus; JSON flag, persisted `is_fresh`, and rendered
   output agree under the canonical rule (data-model §2a), including the boundary-10
   override mismatch.
7. **Overrides** (T015/T016): valid threshold/boundary overrides recorded per assessment
   and used; conflicting `max_nonce`/`fresh_transaction_boundary` rejected.
8. **Restart** (T017/T019): persisted rows retain evidence envelope; readers keep
   version-specific semantics for `004.1`, `003.1`, and legacy-unversioned rows.
9. **Schema compatibility** (T017): old `003.1`/legacy rows read unchanged; new envelope
   additive only; direct `scoring_config` consumers tolerate additive keys.
10. **Dry-run** (T010/T019): full formatted output with same score/threshold/evidence; no
    delivery, no dedup poisoning (G-018: `AlertDispatcher.dispatch` returns before any
    delivery-state write).
11. **Shared transaction** (T010; FR-021): two per-participant observations sharing one
    transaction ID evaluated as separate wallet-bearing trades; pipeline assertion fails if
    participants collapse before profiling.
12. **Old-row compatibility** (T017/T019): pre-004 rows keep legacy freshness/impact
    semantics; never reinterpreted as current measured classifications.

## Future run commands (after implementation, under reserved human validation)

```bash
uv run pytest tests/detector/test_fresh_wallet.py tests/detector/test_size_anomaly.py tests/detector/test_scorer.py -q
uv run pytest tests/integration/test_end_to_end.py -q
uv run python scripts/verify.py --profile static
```

## Prerequisites

Bounded current Gamma field/schema check (read-only, no wallets/secrets/trade retention);
real PostgreSQL/Redis parity per 002/003 contracts; managed quality gates green.
