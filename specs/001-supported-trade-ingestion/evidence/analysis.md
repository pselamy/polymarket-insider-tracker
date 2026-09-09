# Spec Kit Analysis Report: Supported Trade Ingestion

**Recorded**: 2026-09-09
**Command context**: `SPECIFY_FEATURE_DIRECTORY=specs/001-supported-trade-ingestion`
`.specify/scripts/bash/check-prerequisites.sh --json --require-spec --require-tasks --include-tasks`
returned `research.md`, `data-model.md`, `contracts/`, `quickstart.md`, and `tasks.md`.
**Nature**: read-only cross-artifact analysis of `spec.md`, `plan.md`, and `tasks.md` against the
constitution, performed after task generation. The analysis itself modified nothing; the corrections
listed under "Resolution" were applied afterwards as separate planning edits in the same commit and
are recorded here so the reviewer can audit them. This file is planning evidence, not an authority.

## Specification Analysis Report

| ID | Category | Severity | Location(s) | Summary | Recommendation / Resolution |
|----|----------|----------|-------------|---------|-----------------------------|
| C1 | Coverage | HIGH | tasks.md T006, T007, T019, T020, T021 | FR-015 (all-participant default, explicit taker-only mode) had zero tagged tasks although T007 sends `takerOnly` explicitly and T020 adds the coverage setting | Resolved: FR-015 tagged on T006, T007, T019, T020, T021 |
| C2 | Coverage | MEDIUM | tasks.md T002 | FR-012 (deterministic coverage of every failure class) was traced only to the fake-server task, not to the test tasks that satisfy it | Resolved: FR-012 tagged on T006, T010, T014, T015, T019 |
| I1 | Inconsistency | MEDIUM | spec.md US2 AC2, FR-005, Key Entities | Terminology drift: "recovery window", "active replay horizon", and "recovery horizon" named one concept | Resolved: normalized to "recovery horizon"; Source Configuration entity now lists coverage mode |
| I2 | Inconsistency | MEDIUM | contracts/observation-boundary.md §First Start and Restart | One table row described a coverage mismatch as first-start and an unknown schema version as terminal in the same cell | Resolved: split into two rows with distinct outcomes |
| I3 | Inconsistency | LOW | plan.md §Technical Context (Scale/Scope) | Module and test-module counts disagreed with the project structure tree | Resolved: counts corrected to four new plus four modified source modules and five new plus five extended test modules |
| U1 | Underspecification | LOW | plan.md, contracts/*.md | Class names `ObservationBoundary` and `TradesSourceClient` appear only in tasks.md | Accepted: contracts name modules and behavior; class names are implementation detail owned by tasks |
| A1 | Ambiguity | LOW | spec.md US1 AC2 versus plan.md Performance Goals | "Within 10 seconds of that source response" versus "one poll interval plus request latency" | No change: 5 s cadence plus observed 0.4–1.4 s latency satisfies the 10 s bound; both statements retained |
| D1 | Duplication | LOW | spec.md §Assumptions | The 2026-09-06 and 2026-09-09 probe summaries overlap | Retained intentionally as dated history; the feasibility record is the authority |

No CRITICAL finding. No constitution conflict was detected.

## Coverage Summary Table

| Requirement Key | Has Task? | Task IDs | Notes |
|-----------------|-----------|----------|-------|
| FR-001 | Yes | T006, T007 | Documented source, wallet-bearing fields |
| FR-002 | Yes | T006, T007, T019, T020 | No credential; read-only |
| FR-003 | Yes | T011, T012, T013 | Startup independent of metadata crawl |
| FR-004 | Yes | T004, T005, T010, T012 | Dispositions, repair, no silent loss |
| FR-005 | Yes | T004, T005, T008, T009, T010, T012 | Composite identity, exact repeats |
| FR-006 | Yes | T003, T008, T009, T015, T017 | Durable boundary |
| FR-007 | Yes | T008, T009, T015, T017 | First start without emission |
| FR-008 | Yes | T006, T007, T014, T016 | Retry, backoff, terminal errors |
| FR-009 | Yes | T010, T012, T013, T016 | Status fields and states |
| FR-010 | Yes | T014, T016 | Empty success |
| FR-011 | Yes | T019, T020 | WebSocket deprecation window |
| FR-012 | Yes | T002, T006, T010, T014, T015, T019 | Deterministic coverage classes |
| FR-013 | Yes | T022, T023, T024 | Live-safe smoke |
| FR-014 | Yes | T021 | Documentation surfaces |
| FR-015 | Yes | T006, T007, T019, T020, T021 | All-participant default (after C1) |
| FR-016 | Yes | T001, T024 | Feasibility record; re-run before convergence |
| FR-017 | Yes | T008, T009, T015, T017 | Reach proof, recovery, possible-data-loss |
| FR-018 | Yes | T006, T007 | Request budget |
| FR-019 | Yes | T001, T024 | Product stop gate |
| SC-001 | Yes | T010, T012 | Exactly-once delivery |
| SC-002 | Yes | T011, T013 | Startup and freshness bounds |
| SC-003 | Yes | T014, T016 | Recovery and terminal reporting |
| SC-004 | Yes | T015, T017, T018 | Restart inside horizon |
| SC-005 | Yes | T022, T023, T024 | Seven smoke cases |
| SC-006 | Yes | T002, T021 | Tested behaviors documented without a bespoke checker |
| SC-007 | Yes | T024 | Feasibility record (already satisfied for planning by evidence/feasibility.md) |

## Constitution Alignment Issues

None. Research-only scope, truthful source grounding, end-to-end evidence through the real pipeline,
safe effects (no alert, transactional Redis writes), compatibility via an explicit deprecation window,
explicit slice activation, reviewer-owned checklist left unchecked, and the human validation gate
(T001) are all present.

## Unmapped Tasks

None. T001 (human gate), T025 (required gates), T026 (gap register), T027 (review sequence), and T028
(pull-request preparation) map to constitution and AGENTS.md obligations rather than to a single FR.

## Metrics

- Total requirements: 26 (19 functional, 7 success criteria)
- Total tasks: 28
- Coverage: 100% after C1 (96% before)
- Ambiguity count: 1 (LOW, no change needed)
- Duplication count: 1 (LOW, intentional)
- Critical issues: 0

## Author-Owned Spec Checklist Re-validation

`checklists/requirements.md`: 16/16 → 16/16 items passing after the 2026-09-09 clarifications; no
marker changed. The reviewer-owned `checklists/ingestion.md` has 36 items, all unchecked.

## Decisions Deferred to Patrick (not analysis findings)

1. Aged-gap behavior: record a loss event and keep monitoring (planned) versus stall until restart.
2. Acceptance of the 10-minute recovery horizon given the 20,000-row reachable depth.
3. The deprecation window for `POLYMARKET_WS_URL` and the retained deprecated WebSocket export.

## Next Actions

- No CRITICAL or HIGH finding remains; the package may proceed to human validation.
- After Patrick's decisions, if any answer changes the planned behavior, re-run `$speckit-clarify` to
  encode it in `spec.md` before `$speckit-implement`.
