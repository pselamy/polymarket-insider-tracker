# Domain Requirements Checklist: Truthful Detection Contract

**Purpose**: Reviewer-owned requirements-quality review of the 004 plan/design artifacts.
**Created**: 2026-10-17 | **Feature**: [spec.md](../spec.md), [plan.md](../plan.md),
[research.md](../research.md), [data-model.md](../data-model.md), [tasks.md](../tasks.md).
**Ownership**: reviewer-owned. Agents must leave items unchecked; only Patrick or his
designated human reviewer may mark criteria satisfied. `[ ]` = pending reviewer judgment,
not implementation status.

## Requirement Completeness

- [ ] CHK001 - Are Gamma market `volume24hr` monetary-interpretation requirements specified with their archived-example limits and no-fallback rule? [Completeness, Spec §FR-004/005]
- [ ] CHK002 - Are per-page acquisition-stamp and 0–600s recency requirements specified for all volume observations? [Completeness, Gap]
- [ ] CHK003 - Are all seven availability states (available, missing, nonpositive, invalid, stale, invalid-time, unqualified) defined without overlap? [Completeness, Spec §FR-005]
- [ ] CHK004 - Are exclusive-boundary (`< 5`) and inclusive-threshold (`>= 0.80`) requirements specified for every boundary fixture? [Completeness, Spec §FR-002/009]
- [ ] CHK005 - Are unknown-age (cap 0.50, no age bonus) requirements distinguished from known-nonqualifying and missing evidence? [Completeness, Spec §FR-007/008/010]
- [ ] CHK006 - Are two-group-only scoring requirements (3+ bonus removed, niche as modifier) specified consistently? [Completeness, Spec §FR-001/011]

## Requirement Clarity

- [ ] CHK007 - Is "provider USD-denominated monetary market volume" quantified without implying USDC settlement, conversion, or aggregation guarantees? [Clarity]
- [ ] CHK008 - Is the 600s local-age rule specified as distinct from provider measurement freshness? [Clarity]
- [ ] CHK009 - Are "nullable authoritative impact" requirements explicit that null means non-measurement, not zero? [Clarity, Spec §FR-005]
- [ ] CHK010 - Is the deprecated inclusive `max_nonce` alias behavior specified for conflict and migration cases? [Clarity, Gap]

## Requirement Consistency

- [ ] CHK011 - Are threshold/boundary/bonus requirements consistent across spec, plan, data-model, contracts, and replay summary? [Consistency]
- [ ] CHK012 - Do volume-unit claims stay within the reviewed evidence category across all artifacts (no normative warranty drift)? [Consistency]
- [ ] CHK013 - Are 003-storage-reuse requirements consistent (amendment only, no competing migration/harness)? [Consistency, Spec §FR-020]

## Acceptance Criteria Quality

- [ ] CHK014 - Can SC-002 (ratio ±0.0001) and SC-005 (reconstruction ±0.01, 100% threshold display) be objectively verified from persisted rows? [Measurability, Spec §SC-002/005]
- [ ] CHK015 - Are replay-acceptance requirements (114 cases, 63 attributed deltas, no unanticipated change) measurable? [Acceptance Criteria, Spec §SC-008]
- [ ] CHK016 - Is every FR/SC/acceptance scenario mapped to named task/test/evidence obligations in tasks.md? [Traceability]

## Scenario & Edge Coverage

- [ ] CHK017 - Are stale/invalid/missing/nonpositive volume, source failure, restart, and schema-compatibility scenarios all addressed in requirements? [Coverage, Edge Case]
- [ ] CHK018 - Are threshold-boundary scenarios (0.80 equality, nonce 5, age 48h, ratio 0.02) all addressed? [Coverage]
- [ ] CHK019 - Are override scenarios (threshold, boundary, volume-ratio) specified with per-assessment recording? [Coverage, Spec §US3]
- [ ] CHK020 - Are dry-run safety requirements (no delivery, no dedup poisoning) specified for the new evidence path? [Coverage, Gap]

## Dependencies & Assumptions

- [ ] CHK021 - Are 001/003 contract dependencies (source acquisition, storage schema, e2e harness) documented and validated? [Dependency]
- [ ] CHK022 - Is the assumption of bounded current field/schema qualification at implementation entry documented with its limits? [Assumption]
- [ ] CHK023 - Are non-goals (trading, backtesting/calibration, book-depth scoring, v2 migration) explicitly excluded? [Gap]

## Notes

- Generated 2026-10-17 by the planning agent as a reviewer-owned artifact; all items unchecked.
- The built-in author-owned `checklists/requirements.md` (16/16) records spec-writing quality, not implementation acceptance.
