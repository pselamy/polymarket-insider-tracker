# Contract: Scoring Configuration Amendment (004.1)

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17 | **Status**: proposed.
Amends the shared [003 assessment-storage contract](../../003-safe-observable-operation/contracts/assessment-storage.md)
without a competing migration. Implementation is not authorized.

## Proposed vs existing (read carefully)

Everything below is a **proposed** amendment for the future implementation to author under
reserved human validation. Existing behavior on main `1bdccbc` (refreshed 2026-10-19; originally `46e4192`): `scoring_config` is the
canonical pure-configuration JSON recorded on every new assessment
(`detector/scorer.py:217-220`); assessment rows carry scalar columns plus nullable
`scoring_config` TEXT (`storage/models.py:168`, `storage/repos.py:574,618`). No existing
strict reader accepts the additive keys — direct library consumers reading
`scoring_config` JSON MUST tolerate them after implementation (pure-config readers keep
working; envelope readers opt into `evaluation_evidence`). There is exactly one versioned
`scoring_config`/`evaluation_evidence` amendment in this proposal; no second migration.

## Amendment

1. Preserve every 003 root key, type, column, index, downgrade path, backfill semantics,
   weight-compatibility window, and quantization rule.
2. Add to the per-assessment serialized envelope (inside existing `scoring_config` TEXT,
   no new DDL column in this proposal):
   - `detector_config`: `{alert_threshold, fresh_transaction_boundary, size_volume_threshold}` (static effective values).
   - `evaluation_evidence`: versioned object with exact source values, `daily_volume_acquired_at`,
     evaluation time, availability reasons (`volume_state`, wallet `classification`), factors,
     per-group contributions, effective threshold.
   - `evidence_schema`: `"004.1"`.
3. `RiskScorer.scoring_config` remains pure effective configuration; the extended envelope is
   composed per assessment at persist time from the captured assessment (threshold captured at
   assessment creation, never reread from mutable settings).
4. `multi_signal_bonus_3` root key retained as documented-neutral compatibility; operational
   3+ branch removed. Weights 0.40/0.35/0.25, two-group bonus 1.2, quantum 0.001 retained.
5. Readers: `003.1` and `legacy-unversioned` rows keep version-specific semantics, untouched.

## Compatibility risks named

- Direct library consumers reading `scoring_config` JSON must tolerate the additive keys
  (pure-config readers keep working; envelope readers opt into `evaluation_evidence`).
- The deprecated custom-weight API/window is preserved per 003.
