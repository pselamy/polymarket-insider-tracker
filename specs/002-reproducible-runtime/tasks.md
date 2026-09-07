# Tasks: Reproducible Supported Runtime

**Input**: Design documents from `specs/002-reproducible-runtime/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`,
`contracts/runtime-verification.md`, and Patrick's 2026-09-07 approval to proceed while the reviewer-owned
`checklists/runtime.md` markers remain unchanged

**Tests**: Required by the feature specification and constitution. Regression/contract tests MUST be
written and observed failing before the corresponding implementation task.

**Organization**: Tasks are grouped by user story so each story has an independently runnable proof.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel because it touches different files and has no dependency on incomplete work
- **[Story]**: Maps implementation work to `US1`, `US2`, or `US3` from `spec.md`
- Every task names its exact file target or evidence artifact

## Phase 1: Setup (Shared Runtime Contract)

**Purpose**: Make the approved interpreter and dependency boundary resolvable before product-path work.

- [X] T001 Set `requires-python` to `>=3.11,<3.14`, require uv `>=0.11,<0.12`, require `sqlalchemy[asyncio]` and `psycopg[binary]`, configure typed tooling paths, regenerate the universal lock, and prove locked sync on 3.11/3.12/3.13 in `pyproject.toml` and `uv.lock`

**Checkpoint**: Every supported interpreter resolves the declared dependency graph and includes `greenlet`
and Psycopg 3; unsupported minors are outside project metadata.

---

## Phase 2: Foundational (Blocking Contracts)

**Purpose**: Establish cross-file truth checking and safe database-URL normalization used by all stories.

**⚠️ CRITICAL**: No user-story behavior starts until these contracts have failing tests and implementations.

- [X] T002 [P] Add fixture-based failing tests for exact Python/platform/uv sets, locked dependencies, full-SHA action pins, minimal workflow permissions, immutable matching service images, CI gate semantics, documentation/database consistency, and complete contradiction reporting in `tests/tooling/test_support_contract.py`
- [X] T003 Implement the read-only, deterministic, all-findings support-contract checker and redacted diagnostics in `scripts/check_support_contract.py`
- [X] T004 [P] Add failing table-driven tests for canonical, bare, asyncpg, malformed, portable-query, incompatible-driver-query, and credential-bearing database URLs in `tests/storage/test_database_url.py`
- [X] T005 Implement parsed driver-only normalization, portable-query preservation, incompatible-driver-option rejection, legacy warnings, validation, and safe rendering in `src/polymarket_insider_tracker/storage/database_url.py`

**Checkpoint**: Fixture tests prove cross-file contradictions fail deterministically and URL normalization
preserves all non-driver fields without leaking credentials.

---

## Phase 3: User Story 1 - Reach a Ready Local Installation (Priority: P1) 🎯 MVP

**Goal**: A clean supported checkout installs, reaches PostgreSQL/Redis, and proves upgrade/downgrade/
re-upgrade plus async access without an undeclared package or destructive operation on the normal database.

**Independent Test**: From a clean checkout, run `uv sync --locked --all-extras`, start the documented
local services, and run `uv run python scripts/runtime_services.py --phase all`; it must probe both
services, complete the disposable migration cycle, clean up, and exit zero with no credential-bearing output.

### Tests for User Story 1

- [X] T006 [US1] Add failing compatibility, deprecation-warning, normalization, and secret-redaction cases for `DatabaseSettings` in `tests/test_config.py`
- [X] T007 [P] [US1] Add failing sync/async Psycopg engine-selection and disposal tests in `tests/storage/test_database.py`
- [X] T008 [P] [US1] Add failing probe/migrations/all phase, real PostgreSQL/Redis, non-loopback refusal, migration-state, primary-failure cleanup, and cleanup-failure integration cases in `tests/integration/test_runtime_services.py`

### Implementation for User Story 1

- [X] T009 [US1] Apply canonical URL normalization at configuration and engine boundaries in `src/polymarket_insider_tracker/config.py` and `src/polymarket_insider_tracker/storage/database.py`
- [X] T010 [US1] Require the canonical `DATABASE_URL`, remove the implicit/stale fallback contract, align local defaults, and pin PostgreSQL 15/Redis 7 by reviewed multi-architecture digest in `alembic/env.py`, `alembic.ini`, `.env.example`, and `docker-compose.yml`
- [X] T011 [US1] Implement distinct probe/migrations/all phases, loopback validation, PostgreSQL/Redis probes, disposable database creation, migration-state checks, async query, and guaranteed cleanup in `scripts/runtime_services.py`
- [X] T012 [US1] Replace the foundation quick start and database troubleshooting instructions with locked, canonical, non-destructive commands in `README.md`
- [X] T013 [US1] Run the independent service command on Apple Silicon and record architecture, revisions, cleanup, exit status, and redacted output in `specs/002-reproducible-runtime/evidence/verification.md`

**Checkpoint**: User Story 1 is usable independently; the contributor's configured application database
has not been downgraded or dropped.

---

## Phase 4: User Story 2 - Trust Required Checks (Priority: P2)

**Goal**: One aggregate entry point and CI topology propagate every required quality, compatibility, and
service failure instead of presenting a false green result.

**Independent Test**: Run `uv run python scripts/verify.py --profile static` and
`--profile compatibility`, then run the table-driven fake-runner suite that forces every required gate to
fail and asserts a nonzero aggregate result.

### Tests for User Story 2

- [X] T014 [US2] Add failing tests for profile membership, ordering, de-duplication, human/JSON results, first-failure propagation, not-run gates, invalid invocation, and secret redaction in `tests/tooling/test_verify.py`

### Implementation for User Story 2

- [X] T015 [US2] Implement the typed `static`, `compatibility`, `services`, and `all` gate orchestrator with stable exit semantics in `scripts/verify.py`
- [X] T016 [P] [US2] Apply the existing Ruff formatter without behavioral edits to `tests/detector/test_size_anomaly.py`
- [X] T017 [P] [US2] Correct the concrete HTTP parameter typing error without weakening checks in `src/polymarket_insider_tracker/ingestor/gamma_client.py`
- [X] T018 [P] [US2] Correct Web3 filter and `AsyncWeb3` typing without blanket ignores in `src/polymarket_insider_tracker/profiler/funding.py`
- [X] T019 [US2] Replace unlocked pip jobs with a least-privilege, full-SHA-action-pinned workflow using an exact uv 0.11 release, feature-branch push evidence with concurrency cancellation, locked static, Ubuntu 24.04 x86_64 Python 3.11/3.12/3.13 compatibility, matching digest-pinned real services, and stable required-summary jobs in `.github/workflows/ci.yml`
- [X] T020 [US2] Run static plus clean isolated compatibility profiles on all three Apple Silicon interpreters and append commands, versions, counts, durations, and results to `specs/002-reproducible-runtime/evidence/verification.md`

**Checkpoint**: All required local gates pass, every injected gate failure is nonzero, and Linux CI has no
ignored required step.

---

## Phase 5: User Story 3 - Understand the Support Boundary (Priority: P3)

**Goal**: Metadata, lock state, automation, examples, and public instructions expose one finite support
boundary and give actionable failures outside it.

**Independent Test**: Run `uv run python scripts/check_support_contract.py`, confirm zero contradictions,
and prove Python 3.10 and 3.14 are rejected by locked project resolution.

### Tests for User Story 3

- [X] T021 [US3] Add a failing repository-level conformance case covering every enumerated support surface, immutable tool/action/image requirements, the Linux reference wording, and the intentional Ruff/mypy minimum-version exception in `tests/tooling/test_support_contract.py`

### Implementation for User Story 3

- [X] T022 [US3] Align the Python badge, supported platform/version text, uv authority, timing boundary, aggregate commands, URL migration path, and actionable unsupported-version guidance in `README.md`
- [X] T023 [US3] Add an advisory arm64 `macos-14` compatibility job using the same verifier profile, without weakening the stable required Linux summary, in `.github/workflows/ci.yml`
- [X] T024 [US3] Run the repository support-contract checker and unsupported-minor probes, then append redacted commands and results to `specs/002-reproducible-runtime/evidence/verification.md`

**Checkpoint**: User Story 3 independently proves zero cross-surface contradictions and clear rejection of
unsupported interpreters.

---

## Phase 6: Polish & Cross-Cutting Evidence

**Purpose**: Prove the complete slice, close only its owned audit gaps, and leave reviewable evidence.

- [X] T025 Run `uv run python scripts/verify.py --profile all` with local services and append the complete redacted result to `specs/002-reproducible-runtime/evidence/verification.md`
- [X] T026 Time the documented foundation path from a clean Apple Silicon checkout with packages uncached but service images present and append the under-five-minute result to `specs/002-reproducible-runtime/evidence/verification.md`
- [ ] T027 Run the blocking Linux workflow jobs, capture their immutable run URL and per-job conclusions, and append them to `specs/002-reproducible-runtime/evidence/verification.md`
- [ ] T028 Update only G-007 through G-012 and G-013a with implementation and evidence dispositions in `specs/audit/gap-register.md`
- [ ] T029 Re-run the spec checklist, support-contract checker, diff check, secret scan, and full required commands; record any approved deferral before convergence in `specs/002-reproducible-runtime/evidence/verification.md`
- [ ] T030 Record the parent anchor and intended implementation path inventory, create bounded slice-002 implementation commit(s), verify each actual inventory with `git show`, and record their hashes in a separate evidence-only provenance commit in `specs/002-reproducible-runtime/evidence/verification.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1** has no dependency and establishes the resolvable runtime.
- **Phase 2** depends on T001 and blocks every user story.
- **User Story 1** depends on Phase 2 and proves installation/services/migrations.
- **User Story 2** depends on Phase 2; T015 consumes T003 and T011, and T019 follows the green local gates.
- **User Story 3** depends on T003; T022 follows T012, and T023 follows T019 because they edit the same files.
- **Phase 6** depends on all three stories and Patrick's implementation approval.

### User Story Dependencies

- **US1 (P1)**: Starts after Phase 2; delivers a usable local foundation without US2 or US3.
- **US2 (P2)**: Starts after Phase 2 but consumes US1's service entry point when composing `services/all`.
- **US3 (P3)**: Starts after Phase 2; public documentation edits follow US1 to avoid conflicting writes.

### Within Each User Story

- Observe each required test fail for the intended reason before its implementation task.
- URL parsing precedes configuration/engine integration; integration precedes service orchestration.
- Verifier contract tests precede the verifier; local gates are green before CI adopts them.
- Core story behavior is independently demonstrated before cross-cutting evidence is recorded.

### Parallel Opportunities

- T002 and T004 can run in parallel after T001.
- T007 and T008 can run in parallel after the URL foundation exists.
- T016, T017, and T018 touch independent files and can run in parallel after T015's test contract is fixed.
- Evidence-only tasks must not run concurrently against the same evidence file.
- No feature-writing command may run concurrently in this checkout.

## Parallel Example: Foundational Contracts

```text
Task T002: Add support-contract tests in tests/tooling/test_support_contract.py
Task T004: Add database-URL tests in tests/storage/test_database_url.py
```

## Parallel Example: Existing Required-Check Failures

```text
Task T016: Format tests/detector/test_size_anomaly.py
Task T017: Fix src/polymarket_insider_tracker/ingestor/gamma_client.py typing
Task T018: Fix src/polymarket_insider_tracker/profiler/funding.py typing
```

## Implementation Strategy

### MVP First (User Story 1)

1. Complete Phase 1 and Phase 2.
2. Complete T006–T013 in red-green order.
3. Stop and run the US1 independent test before changing CI.

### Incremental Delivery

1. Runtime/dependency foundation → locked interpreter matrix resolves.
2. US1 → clean local services and safe migrations work.
3. US2 → required checks fail closed locally and in Linux CI.
4. US3 → every public support surface is consistent and unsupported use fails clearly.
5. Phase 6 → platform/timing/CI evidence and gap-register convergence.

## Notes

- Every task remains unchecked until implementation evidence proves it complete.
- `[P]` means file-independent, not permission to run concurrent Spec Kit feature-writing commands.
- Reviewer checklist state is controlled by Patrick and is not modified by `$speckit-implement`.
- Stop for a spec/plan revision if implementation cannot preserve legacy URL components, safely isolate
  migration downgrade, or meet the approved platform matrix.
