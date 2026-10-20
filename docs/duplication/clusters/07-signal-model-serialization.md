---
cluster: 7
status: incidental
files: src/polymarket_insider_tracker/detector/models.py, src/polymarket_insider_tracker/storage/repos.py
---

# Cluster 7

- **Files:** detector/models.py, storage/repos.py
- **Knowledge:** **Resolved**: confidence thresholds/classification, trade-identity property accessors and the trade-head serialization now have one home (`serialized_trade_head`, `is_high_confidence_score`, shared property objects, `UNRECORDED_DELIVERY_DISPOSITION`). Two residual groups remain visible on purpose: the `FreshWalletSignal`/`SizeAnomalySignal` declaration block (96 tokens) and the `RiskAssessment`/`RiskAssessmentDTO` field-default mirror (52 tokens, intentional persistence/domain layer boundary)
- **Verdict:** resolved; residual incidental (see the round-3 review correction below)
- **Counts when first grouped (historical):** 2 clones, 24 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: cluster 7 (signal model property and to_dict serialization)

Three clone groups carried the same knowledge across
`detector/models.py` and `storage/repos.py`: identical confidence-threshold
classification (`is_high_confidence` >= 0.7, `is_very_high_confidence` >= 0.85
on all three signal classes), identical trade-anchored identity accessors
(`wallet_address`, `market_id`, `trade_size_usdc` reading `trade_event`), the
identical six-entry trade-head `to_dict` serialization, and the identical
"unrecorded" delivery default on `RiskAssessment` and `RiskAssessmentDTO`.
Behaviour was characterized first (`tests/detector/test_models.py`,
`tests/storage/test_risk_assessments_schema.py`) and no behavioural difference
existed between the copies; the >= boundary is the only threshold semantics
and the serialization output is byte-identical before and after (field names,
types and ordering unchanged).

One home per knowledge piece: thresholds live in named constants
(`HIGH_CONFIDENCE_THRESHOLD`, `VERY_HIGH_CONFIDENCE_THRESHOLD`) consumed by
`is_high_confidence_score` / `is_very_high_confidence_score`, whose shared
getter functions back each signal class's property objects; trade-head entries
are built once by `serialized_trade_head` and spread into all three `to_dict`s;
`UNRECORDED_DELIVERY_DISPOSITION` lives in the dependency-free
`polymarket_insider_tracker/constants.py`, which both `detector/models.py` and
`storage/repos.py` import (the domain model no longer imports the persistence
layer, and the warning-strict storage-only entrypoints stay free of detector's
dependency tree). No base classes, mixins,
registries or `__module__` rewriting; public import paths unchanged and every
existing test passes unchanged.

Re-measured on the round-1 rebase onto `origin/main` at `6be456a` (this PR is
drafted against that tree), with the identical pinned method (jscpd 5.3.3,
`SCAN_POLICY` at the 50-token tier, empty config, no ignore lists or
exclusions), enumerating the authored scope as
`quality.source_scope.discover` + `validate_path` and staging it manually:

- Before (rebased card chain with the US-001 characterization tests, pre-refactor,
  283 authored Python files): **87 clone groups, 1095 duplicated lines, 1.81%**.
- After (the cluster-7 refactor applied, same 283 files): **84 clone groups,
  1035 duplicated lines, 1.71%**.

Set-difference of the raw clone lists: exactly the three cluster-7 clones
(`src/.../detector/models.py:38-75 <-> src/.../detector/models.py:109-146`,
197 tokens; `src/.../detector/models.py:57-70 <->
src/.../detector/models.py:182-195`, 67 tokens;
`src/.../detector/models.py:244-251 <-> src/.../storage/repos.py:574-581`,
52 tokens) disappeared and **no new clone group appeared anywhere**. Against
`origin/main` directly, the branch additionally contributes only the two
intra-file clones of the characterization test module required by US-001.

Round-3 rebase re-measurement (onto `origin/main` at `382a0f6`, same pinned
method and enumeration): `origin/main` **88 groups, 1116 lines, 1.85%** over 283
files; this branch **87 groups, 1071 lines, 1.76%** over 285 files (the two
added files are `constants.py` and the characterization module `tests/detector/test_models.py`).
Set-difference: the three cluster-7 clones above disappeared; the only additions
are the two intra-file characterization-test clones (51 and 68 tokens, now at
`tests/detector/test_models.py:313-317 <-> 326-330` and `321-330 <-> 334-343`).

### Round-3 review correction: two clones were hidden, not removed

The round-1 and round-3 rebase figures above claimed the 52-token
`RiskAssessment`/`RiskAssessmentDTO` clone was gone and that the 197-token
signal clone left nothing behind. Both claims came from token-shuffling edits,
not from removing knowledge: `RiskAssessmentDTO` restated its plain defaults as
`field(default=...)`, and `SizeAnomalySignal` bound the shared property objects
in a different order from `FreshWalletSignal`. Both edits are reverted.
`RiskAssessmentDTO` uses plain defaults again, with only the disposition
reading `UNRECORDED_DELIVERY_DISPOSITION`, and both signal classes bind the
accessors in the same order. The characterization module's three copy-pasted
threshold tests were also folded into one table-driven test over the three
builders, with the same twelve cases.

Re-measured with the identical pinned method and enumeration: `origin/main` at
`382a0f6` **88 groups, 1116 lines, 1.85%** over 283 files; this branch
**87 groups, 1080 lines, 1.78%** over 285 files. Raw clone-list difference:

- removed: `src/.../detector/models.py:38-75 <-> 109-146` (197 tokens) and
  `src/.../detector/models.py:57-70 <-> 182-195` (67 tokens);
- added: `src/.../detector/models.py:106-121 <-> 155-170` (96 tokens);
- unchanged on both sides: `src/.../detector/models.py:265-272 <->
  src/.../storage/repos.py:569-576` (52 tokens; main `244-251 <-> 574-581`).

Both remaining groups are left visible and are verdicted **incidental**:

- **96-token signal declaration block.** It spans the `confidence`, `factors`
  and `timestamp` field declarations, the five `property(...)` bindings and the
  `to_dict` opening that spreads `serialized_trade_head`. Every behaviour it
  names (thresholds, accessors, trade-head entries) now has one home in the
  module helpers. What remains is each frozen dataclass declaring its own
  public surface. Collapsing it further would need a shared base class or
  mixin, which this card forbids ("no base classes/mixins ... just to share
  lines").
- **52-token DTO/domain field-default mirror.** `RiskAssessmentDTO` is the
  persistence row shape (it mirrors `RiskAssessmentModel` columns), and
  `RiskAssessment` is the domain record. Their Slice 003 optional fields share
  names and `None`/`False` defaults because they describe the same persisted
  facts at two layers. The only shared *value* that could drift, the
  `"unrecorded"` disposition, has one home in the dependency-free
  `constants.py`. Sharing the field list would need dataclass inheritance or a
  generated schema across the persistence/domain boundary. Either way storage
  would depend on the domain shape or the reverse, and slice 004 will change
  the domain side independently. This is an intentional layer boundary and is
  recorded rather than disguised.

### Round-3 docstring revert and rebase onto `69c4807`

The `storage/repos.py` module docstring and the `RiskAssessmentDTO` docstring
had been shortened by this card for no cluster-7 reason; both are restored
byte-for-byte to `origin/main`, so the card's `repos.py` diff is only the
`UNRECORDED_DELIVERY_DISPOSITION` import and the disposition default (656 lines,
main 655). The branch was then rebased onto `origin/main` at `69c4807` (after
#160, cluster 28). Re-measured with the identical pinned method and
enumeration: `origin/main` **87 groups, 1107 lines, 1.83%** over 285 files; this
branch **86 groups, 1071 lines, 1.76%** over 287 files (the two additions are
`constants.py` and `tests/detector/test_models.py`). Raw clone-list difference:

- removed: `src/.../detector/models.py:38-75 <-> 109-146` (197 tokens) and
  `src/.../detector/models.py:57-70 <-> 182-195` (67 tokens);
- added: `src/.../detector/models.py:106-121 <-> 155-170` (96 tokens, the
  incidental signal declaration block verdicted above);
- unchanged on both sides: the 52-token DTO/domain mirror, now
  `src/.../detector/models.py:265-272 <-> src/.../storage/repos.py:575-582`
  (main `244-251 <-> 574-581`; the one-line shift is the constant import).

No other clone group appeared or disappeared. Resolved by draft PR
[#159](https://github.com/pselamy/polymarket-insider-tracker/pull/159) from branch
`ralph/dedup-models`.
