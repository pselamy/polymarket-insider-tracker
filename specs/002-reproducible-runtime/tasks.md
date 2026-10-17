# Tasks: Reproducible Supported Runtime

> **Superseded mechanism (2026-09-08):** The completed tasks below accurately record the original
> implementation, but the bespoke cross-file support checker and its fixture suite were later removed.
> Runtime declarations now remain with their native owners and are exercised by lock, compatibility,
> service, and focused verifier gates. The reviewer-owned checklist retains all 0/35 marker states; its
> criteria text now names the replacement evidence model. See Phase 8.

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
- [X] T027 Run the blocking Linux workflow jobs, capture their immutable run URL and per-job conclusions, and append them to `specs/002-reproducible-runtime/evidence/verification.md`
- [X] T028 Update only G-007 through G-012 and G-013a with implementation and evidence dispositions in `specs/audit/gap-register.md`
- [X] T029 Re-run the spec checklist, support-contract checker, diff check, secret scan, and full required commands; record any approved deferral before convergence in `specs/002-reproducible-runtime/evidence/verification.md`
- [X] T030 Record the parent anchor and intended implementation path inventory, create bounded slice-002 implementation commit(s), verify each actual inventory with `git show`, and record their hashes in a separate evidence-only provenance commit in `specs/002-reproducible-runtime/evidence/verification.md`

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

## Phase 7: Convergence

- [X] T031 Correct the implemented-state wording and load `.env` in the tracked clean foundation command, then add an executable clean-path regression per FR-006 and US1/AC1 (partial)
- [X] T032 Preserve aggregate prerequisite exit code `2` in human/JSON results while retaining first-failure and not-run behavior per FR-012 and the runtime contract's Exit Status section (contradicts)
- [X] T033 Validate and credential-redact Redis URLs, guard client/engine construction, and guarantee cleanup for direct probe failures with regression tests per FR-012 and the runtime contract's Service and Migration Safety section (partial)
- [X] T034 Make `runtime_services.py --phase migrations` require only `DATABASE_URL` while `probe` and `all` require `REDIS_URL`, with CLI regression coverage per the plan's distinct verification-phase decision (partial)
- [X] T035 Extend the support-contract checker and fixture tests to verify exact verifier profile membership/minimum-version mypy behavior and full `.env.example`/Compose service-setting compatibility per SC-007 and the plan's finite support contract (partial)
- [X] T036 Emit one machine-readable invocation-error object when `--json` accompanies a missing or invalid profile, with regression coverage per the runtime contract's JSON Output and Exit Status sections (partial)
- [X] T037 Align `Gate` with the planned gate-definition contract by using the stable `strict-types` identifier and explicit prerequisite/redaction metadata, with profile and serialization tests per the plan's data-model Gate Definition (contradicts)
- [X] T038 List the exact directly runnable Ruff, mypy, pytest, Alembic, and support-contract commands in `verify.py --help`, with a CLI assertion per the runtime contract's Profiles section (partial)
- [X] T039 Align runtime-service Redis URL validation with the application's exact `redis://` scheme contract and reject verifier-only schemes with regression coverage per FR-006 and SC-007 (adversarial finding)
- [X] T040 Isolate the deterministic pytest gate from both inherited application variables and implicit repository `.env` discovery, then prove the documented `.env`-loaded aggregate path remains offline and under five minutes per FR-011, SC-002, and the runtime contract's Profiles section (adversarial finding)
- [X] T041 Align the Gate Definition redaction policy and Gate Result identifier in `data-model.md` with the implemented/runtime-contract schema, guarded by a repository documentation assertion per SC-007 (convergence finding)

## Phase 8: Repository Hygiene Supersession

- [X] T042 Remove the bespoke cross-file support checker, its synthetic repository fixtures, and its two
  prose assertions; remove the gate from aggregate verification; retain runtime truth in native metadata,
  lock, CI, service probes, focused verifier tests, and contributor documentation.

## Phase 9: Independent Pyright Ratchet

- [X] T043 Pin and lock Pyright in the uv development environment; configure strict Python 3.11 checking
  for the complete production package, with narrow local stubs only for consumed untyped dependencies.
- [X] T044 Add Pyright as a distinct fail-closed verifier gate after strict mypy, and require the static
  profile through the protected CI aggregator.
- [X] T045 Resolve every strict Pyright diagnostic without exclusions, baselines, blanket suppressions,
  or weakened first-party types; retain strict mypy and Ruff.
- [X] T046 Add focused verifier, protocol-boundary, and runtime behavior-guard tests and align the active
  README, specification, plan, data model, quickstart, and runtime contract.
- [X] T047 Run the complete local static, compatibility, service, and aggregate verifier profiles on the
  implementation candidate and retain their results for the reviewer handoff and pull-request description.
- [X] T048 Complete the required immutable Agy and Claude Code/fable reviews, resolve all findings, open
  the non-draft pull request, and verify its blocking GitHub checks without merging it.

## Phase 10: Required Vulture Dead-Code Gate

Branch `quality/vulture-required-gate` from base `a0c0d9945a3a38cec965e09a1ed2d5eb0c71d67f`. Phase 9
completion is not evidence for this phase; each item below is checked only when true for this slice.

- [X] T049 Pin and lock `vulture==2.16` in `pyproject.toml` and regenerate `uv.lock` reproducibly.
- [X] T050 Configure Vulture over `src`, `tests`, `scripts`, `alembic`, and `conftest.py` (every tracked
  repository Python file) at its default confidence with no baseline, allowlist, `ignore_names`,
  `ignore_decorators`, path exclusion, inline suppression, or confidence threshold.
- [X] T051 Add `vulture` as an explicit fail-closed gate after `pyright` in `scripts/verify.py` (`static` and
  `all` profiles), naming its scope on the command line.
- [X] T052 Add an independent required `vulture` job in `.github/workflows/ci.yml` and bind it into the
  `Required checks` aggregator.
- [X] T053 Resolve every default-confidence Vulture finding through genuine dead-code removal or real
  code/test structure, preserving live behavior and recording removed public surface in `CHANGELOG.md`.
- [X] T054 Add tests proving local gate membership/order, per-gate failure propagation, the CI job's command
  binding to the verifier gate, aggregator dependency/order, and fail-closed handling of every non-success
  predecessor result by executing the real aggregator script.
- [X] T055 Align README, specification, plan, data model, runtime contract, quickstart, and tasks with the
  Vulture gate requirements.
- [X] T056 Agy first pass committed as `c596aa74dd87f4eff2cc22a30de5a5d6e17640c8` and preserved unamended as
  the reviewable first-pass anchor.
- [X] T057 Claude Code/fable adversarial review completed on top of the Agy commit: removed `min_confidence`,
  `ignore_decorators`, and `ignore_names`; made the bare scan clean through real structure; bound the CI
  job and aggregator tests to the real workflow and verifier; fixed trailing-blank-line diff errors; updated
  the data model, changelog, and this ledger. Commit hashes are recorded in
  `evidence/verification.md` (Phase 10).
- [X] T058 Codex refute-first review of the fable-corrected branch, with every finding resolved or recorded.
- [X] T059 Open the non-draft pull request for `quality/vulture-required-gate` against `main`.
- [X] T060 Verify that the independent `Vulture dead code check` job and the `Required checks` aggregator pass
  on the pull-request head; record the immutable run and per-job conclusions in `evidence/verification.md`.
- [X] T061 Patrick's approval of the pull request (#115).
- [X] T062 Merge into `main` (merge commit `ff146dccbb37ef90f8784bd3115adf64f206f96d`).
- [X] T063 Post-merge: confirm the `main` workflow run is green with the `vulture` job present in the
  required aggregator, and close this ledger (run `34286515835`).
- [X] T064 Agy corrective pass committed as `fc8ae9009b83b53b7e5033527252e005c43c9557` on checkpoint
  `4df55be84ecb506946263e6e145b95dda726f3d9`: extended the Vulture scope to `alembic` and `conftest.py`,
  exposed the Pytest/Alembic entry points through `__all__`, and removed the test type suppression.
- [X] T065 Claude Code/fable adversarial review of the corrective commit `fc8ae90`, with fixes committed on
  top of it unamended as `2a909a6619ba8091ef8ed85e96f5a64ea7f979db` and the review recorded in
  `evidence/verification.md`.
- [X] T066 Codex refute-first re-review of the fable-corrected corrective head.
- [X] T067 Verify that both push and pull-request workflows pass on reviewed corrective implementation head
  `ac691ab16a57b702d6870842e7479cbaf5b2784f` and record the immutable runs; T060's evidence covers head
  `69654ee` only.

## Phase 11: Required Complexipy Cognitive Complexity Gate

Branch `quality/complexipy-required-gate` from base `ff146dccbb37ef90f8784bd3115adf64f206f96d`. Phase 10
completion is not evidence for this phase; each item below is checked only when true for this slice.

- [X] T068 Pin and lock `complexipy==8.0.1` in `pyproject.toml` and regenerate `uv.lock` reproducibly.
- [X] T069 Configure `[tool.complexipy]` in `pyproject.toml` over `src`, `tests`, `scripts`, `alembic`, and
  `conftest.py` with `max-complexity-allowed = 5`, `no-ignore = true`, covering every tracked repository
  Python file with no exclusions, baselines, or suppressions.
- [X] T070 Add `complexipy` as an explicit fail-closed gate after `vulture` in `scripts/verify.py` (`static`
  and `all` profiles), naming its scope and registering direct gate commands.
- [X] T071 Add an independent required `complexipy` job (`Complexipy complexity check`) in
  `.github/workflows/ci.yml` and bind it into the `Required checks` aggregator.
- [X] T072 Remediate all 61 baseline cognitive complexity hotspots (> 5) across `src`, `tests`, and `scripts`
  to `<= 5`, preserving public interfaces, behavior, and type cleanliness.
- [X] T073 Add contract tests in `tests/tooling/test_complexipy.py`, `tests/tooling/test_verify.py`, and
  `tests/tooling/test_ci_workflow.py` proving scope coverage, suppression comment banning, escape hatch
  rejection, fail-closed aggregator behavior, and verifier integration.
- [X] T074 Align README, specification, plan, runtime contract, tasks, and evidence with the Complexipy
  gate requirements.
- [X] T075 Agy first pass committed as the reviewable first-pass anchor on top of base
  `ff146dccbb37ef90f8784bd3115adf64f206f96d`.
- [X] T076 Claude Code/fable adversarial review of the Agy commit `71cc478`, with fixes committed on top
  of it unamended as one corrective commit and the review recorded in `evidence/verification.md`
  (contract test self-detection, explicit-flag canonical command, corrected hotspot ledger, restored
  behaviour parity with focused tests).
- [X] T077 Codex refute-first review, with every finding resolved or recorded. The review reproduced and
  closed an upstream `ignore-complexity = true` exit-status escape, added the missing Complexipy verifier
  failure/help contracts, and completed the full local static, compatibility, service, aggregate, and
  configuration-independent analyzer verification recorded in `evidence/verification.md`.
- [X] T078 Open non-draft pull request #116 for `quality/complexipy-required-gate` against `main`.
- [X] T079 Verify that the independent `Complexipy complexity check` job and the `Required checks` aggregator
  pass on implementation head `b69ff382972d7e0f9bfe49ce52f8d8862e413c1b`; immutable push run
  `34293412134`, pull-request run `34293638475`, and per-job conclusions are recorded in
  `evidence/verification.md`.
- [X] T080 Patrick's approval of the pull request (Patrick approved reviewed head `3e3375a`).
- [X] T081 Merge into `main` (merge commit `7a4f11cd645dd21b049db3649747bb4e03b652e2` at `2026-09-09T01:30:42Z`; tree `fca6220513027eba30f478deab10191d6547bb22`).
- [X] T082 Post-merge: confirm the `main` workflow run is green with the `complexipy` job present in the
  required aggregator, and close this ledger (root verified all 9 jobs successful in run `34299489433`:
  https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34299489433).
- [X] T083 Correct post-PR adversarial findings without amending the ordered agent commits: isolate
  duplicate funding inserts with transaction savepoints and structured `IntegrityError` identifiers;
  preserve the scorer's base IEEE-754 addition order at the alert threshold; and close Complexipy's
  automatic-snapshot, cwd-exclusion, and omitted-module-control-flow escape hatches with real red-to-green
  regression tests, aligned contracts, and appended evidence.

## Phase 12: Mocks-to-Fakes Test Quality Migration

Branch `quality/fakes-over-mocks` from base `7a4f11cd645dd21b049db3649747bb4e03b652e2`. Inventory: 22 test files
importing `unittest.mock`, 301 mock constructors, and 52 interaction assertions. Ordered agent history: Agy's
incomplete first pass was preserved unchanged by Codex as checkpoint `7b7305aff0ad6a2c5a85bb95dec114a7fce52525`;
Claude Fable's corrective pass is the commit that follows it; Codex reviews independently afterwards.

- [X] T084 Capture baseline test collection (862 tests collected; 860 passed, 2 platform skips) and coverage
  (pytest-cov `TOTAL 91%` with `branch = true`, which is combined statement-and-branch coverage: 4098 statements /
  329 missed, 768 branches / 93 partial) to establish the invariant mapping before migration.
- [X] T085 AST policy regression in `tests/tooling/test_test_quality.py`. Agy's version exempted its own file by
  basename; Fable removed the exemption, added attribute-access detection (`import unittest` +
  `unittest.mock.*`), the root `conftest.py`, and fixtures proving every alias spelling is rejected while
  `pytest.MonkeyPatch`, `httpx.MockTransport`, and string literals are allowed.
- [X] T086 Redis double and shared contract. Agy's 561-line hand-built `FakeRedis` and its fake-only self-tests
  were deleted; Fable adopted the pinned `fakeredis==2.38.0` development dependency, rewrote
  `tests/integration/test_redis_contract.py` as one suite parametrized over `fakeredis` and the real loopback
  Redis (`RUN_SERVICE_TESTS=1`, fail-closed, unique namespace, no `FLUSHDB`), added
  `validate_loopback_redis_url` to `scripts/runtime_services.py`, and added the mandatory `redis-contract` gate to
  the `services` verification profile.
- [X] T087 `tests/test_config.py` uses `pytest.MonkeyPatch` contexts and real model instances (Agy).
- [X] T088 `tests/ingestor/` migrated. Fable replaced call-count paging and fail flags with cursor-keyed pages
  and per-operation error injection in `tests/fakes/clob.py`, used the real `OrderBookSummary`/`OrderSummary`
  SDK values, replaced canned `xreadgroup` overrides with real stream lifecycles (trimmed and deleted entries
  read back as tombstones) plus explicit parser fixtures for the impossible `None` payload, and removed every
  `cast(Any, ...)`.
- [X] T089 `tests/profiler/` migrated. Fable replaced response ladders with a `TransferLogIndex` filtered by
  token, recipient, and block window; runs the tracer over a real `PolygonClient`; uses a real closed-port Redis
  as the cache-failure injector; and surfaced a product defect (`health_check` called the `block_number`
  property as a method) that the faithful `FakeEth` exposes and the previously mocked test could not.
- [X] T090 `tests/detector/` migrated. Fable keyed `FakeWalletAnalyzer` and `FakeMetadataSync` by wallet and
  market instead of call order, removed the subclass-of-product fake, and asserted Redis state through the
  public `DBSIZE` instead of fake internals.
- [X] T091 `tests/alerter/` migrated. Fable replaced the hand-written async HTTP client with real
  `httpx.AsyncClient` instances bound to `httpx.MockTransport` webhook servers that rate-limit or fail, and
  asserted the posted JSON payloads.
- [X] T092 Pipeline, persistence, shutdown, and CLI tests migrated. Fable removed all 26 new `# type: ignore`
  comments, the private `_score_and_alert` spy, and every canned detector/scorer/formatter/dispatcher fake;
  `wire_pipeline` assembles the real pipeline over `fakeredis`, `FakeEth`, `FakeBaseClobClient`, and
  `FakeAlertChannel`, and tests prove persisted assessments, delivered or suppressed payloads, logged
  persistence failures against a real unreachable database, and barrier-proven detector concurrency without
  wall-clock sleeps. `make_test_settings` was corrected to pass detector options by their aliases, which Agy's
  version silently dropped.
- [X] T093 Verification: zero `unittest.mock` usage under `tests/`; all 862 baseline test IDs retained plus the
  new contract, policy, and fidelity tests; Black, Ruff, strict mypy/Pyright, Vulture, Complexipy `<= 5`, and the
  Python 3.11–3.13 compatibility profile pass; the shared Redis contract passes against `fakeredis` and a real
  Redis 8.10.1 (evidence in `evidence/verification.md`).
- [X] T094 Agy's first pass was preserved immutably as checkpoint `7b7305aff0ad6a2c5a85bb95dec114a7fce52525`
  (2 failing tests, no production or gate changes) rather than as a completed hand-off.
- [X] T095 Claude Fable adversarial review and corrective commit on top of the checkpoint (this phase's
  commit), with the SHIP/REVISE verdict and behavior-preservation map recorded outside the repository in the
  follow-up evidence directory and summarized in `evidence/verification.md`.
- [X] T096 Codex independent artifact read, verification rerun, and adversarial review. Root
  accepted corrective commit `e8077f6` after a separate all-profile run on Python 3.13 with
  PostgreSQL 15/Redis 7 and isolated Python 3.11/3.12 compatibility runs; a second independent
  reviewer returned SHIP and confirmed the history test kills all three bound/limit mutants.
- [X] T097 Open pull request #117 after convergence; both push `34307927635` and PR
  `34307929943` passed all nine jobs at reviewed head `408177b` before Patrick approved merging.
- [X] T098 Resolve Codex findings after immutable Fable `ae4e180`: policy alias/dynamic imports,
  Redis SCAN and cleanup, CLOB BookParams fidelity, deterministic settings, real funding path
  assertions, history range/limit coverage, and actual Web3 provider health-check regression.
  Independently verify all 12 gates, 909 tests, Python 3.11–3.13 compatibility, shared Redis 7
  contracts, real PostgreSQL 15 probes/migration cycle and cleanup; append exact evidence.
- [X] T099 Patrick approved PR117; squash-merged as `357c350af2f2b4080fbd90cbddfc01900452c5fe`
  at `2026-09-09T03:52:00Z`. Root confirmed the merged tree equals approved head `408177b`
  and all nine main-CI jobs passed in run `34308759971`.

## Root Agent Guidance Follow-up (Separate PR)

The AG prefix isolates this documentation ledger from the fakes ledger above.

- [X] AG001 Record the root-guidance contract and implementation plan before authoring the root file.
- [X] AG002 Add root `AGENTS.md`, grounded in approved policy and executable repository configuration.
- [X] AG003 Verify relative links and command examples; review policy/implementation distinctions.
- [X] AG004 Run existing static and compatibility profiles; results are in `evidence/agent-guidance.md`.
- [X] AG005 Reconcile against merged PR117 and independently review the guidance diff;
  reviewer returned SHIP and fresh all-profile / Python 3.11–3.12 compatibility passed.
- [ ] AG006 Open the separate PR and verify required checks at its exact head.
- [ ] AG007 Obtain Patrick's merge approval, merge, and verify resulting main CI.

### T5 receipt-binding correction (2026-09-12)

- [X] T5-R1 Prove validator-owned execution with actual collected IDs and phase outcomes;
  reject forged sets, gate/file credit, skipped/missing/failed runs and metadata forgery.
- [X] T5-R2 Bind configured expected revision, actual HEAD/tree and execution input bytes;
  publish no circular or invented final-commit manifest.
- [X] T5-R3 Preserve baseline historical rows/manifests and enforce ordered append-only
  history against immutable Git objects and prior HEAD; rerun native 5/10 gates,
  all supported compatibility minors and owned loopback services before candidate handoff.

Correction evidence: 34 contract cases passed, including validator-owned positive
and skipped/failing runs plus immutable prior-HEAD additions. Native static and
separate cyclomatic <=10 passed; Python 3.11/3.12 full runs passed 1,579 tests.
Python 3.13 full compatibility and real loopback services also passed during this
lane. The final post-commit receipts and exact native command/source/result ledger
are retained in `/home/dev/dispatch-state/pm-t5-binding-fix-r2-20260912-sweep1331/`.
This is a local implementation candidate for fresh independent review; no remote
checks, publication, approval or merge is asserted.

- [X] T5-R2a Reject self-consistent dirty source before crediting a configured committed
  revision, and recheck clean input status after execution. The corrective regression
  recomputes the caller manifest after changing the harness and still requires failure.
  Source-mutating contract cases use dedicated committed local clones.
- [X] T5-R2b Bind claimed tests and execution inputs to immutable committed tree
  bytes independent of Git index flags and ignore rules (R2-INDEX, R2-IGNORED).
  Regressions: assume-unchanged dirty, skip-worktree dirty, ignored untracked
  `tests/.cache/` test. Ignored side-effect files (`__pycache__`/caches) stay
  non-blocking; valid harness credit and ledger history are preserved.
- [X] T5-R2c Correct R1-CI-HISTORY by configuring the blocking Linux compatibility
  and advisory Apple compatibility checkouts with `fetch-depth: 0`. The workflow-bound
  regression creates a real depth-1 local checkout, proves immutable-history validation
  fails while the baseline object is absent, restores history using the configured full-history
  contract, and proves the same validation succeeds. The fail-closed validator and historical
  ledgers remain unchanged. Fresh local static cognitive <=5, separate cyclomatic <=10,
  Python 3.11/3.12/3.13 compatibility, and private loopback PostgreSQL/Redis 7 service gates
  passed; publication and independent review remain pending.


## Native security extension — story5 (P1), preparation approved

Original task/evidence history above is retained. Direction approval permits preparation for review;
it does not approve individual findings, governance ratification, access changes or a merge.
All new human checklist markers remain unchecked. SEC tasks cover FR-023–030 and SC-011–015.

### Setup and foundation

- [X] SEC001 [US5] Preserve direction approval/native receipts and source-bound Phase0 research in `evidence/security-policy/`; clarify tree/history identity and donor preservation (FR-024,027,029).
- [X] SEC002 [US5] Prepare spec/plan/model/contract/quickstart, exact amendment proposal and reviewer-owned `checklists/security-policy.md`; run managed tasks/analyze before implementation (FR-023–030). This box records document preparation only; analysis verdict is separate.
- [X] SEC003 [US5] Write RED parser/schema controls in `tests/tooling/test_secret_report.py` and `tests/tooling/test_secret_schema.py`, covering exit/report contradictions, full schema, duplicate JSON/rows, native coordinate/collision cases and safe output. Put a synthetic private-context sentinel in path/rule/fingerprint/tags and source fields; prove finding/report repr and formatted error/log output do not expose it (FR-024,026,027; SC-011,012).
- [X] SEC004 [US5] Implement immutable safe metadata in `scripts/_secret_models.py`, bounded JSON/schema in `_secret_json.py`/`_secret_schema.py` and pure `_secret_report.py`; no approval, CLI or gate registration (FR-024,026,027).
- [X] SEC005 [US5] Verify exact source objects/line bounds in `scripts/_secret_source.py` and `tests/tooling/test_secret_source.py`; immutable tree/history membership and full-blob identities reject all mismatch/unsupported cases (FR-023,024,026; SC-012,013).

### Native worker and donor preservation

- [ ] SEC006 [US5] Implement `scripts/_secret_worker.py` and `tests/tooling/test_secret_worker.py`: qualified release/config/commands, immutable credential-free snapshots, fresh bounded reports, both scopes/exits and protected execution/scope receipts (FR-023,026,030; SC-012,013).
- [ ] SEC007 [US5] Retain main test IDs/history; port PR129's useful scenarios to explicit auxiliary/stub/native layers without its detector allowlists/fallback success or module rename; prove collection retention in `evidence/security-policy/` (FR-029; SC-013).
- [ ] SEC008 [US5] Execute real native clean/tree/history-only/collision and every file/ref/report/process/scope negative in `tests/tooling/test_secret_native.py`; keep safe source-bound receipts in `evidence/security-policy/` (FR-023,024,026,027; SC-011–013).

### Protected approval and complete acceptance

- [ ] SEC009 [US5] Implement protected adapters in `scripts/secret_policy/` and `tests/tooling/test_secret_policy.py`: native repository/head/base, exact reviewed set digest, independent eligible human, merged-record mapping and revocation (FR-025,028,030; SC-012,014).
- [ ] SEC010 [US5] Qualify worker transport/App isolation and actual hosting revocation/merge-freshness barrier with `tests/tooling/test_secret_hosting.py`; keep nonempty acceptance disabled until conclusive native proof; prepare exact deployment diff/readback plan (FR-025,030; SC-014,015).
- [ ] SEC011 [US5] Obtain exact governed amendment approval, then update `AGENTS.md`, constitution and owned runtime/security contracts consistently; retain every other prohibition and qualify this variation against upstream (FR-028; SC-015).
- [ ] SEC012 [US5] Construct fresh full-field individual proposals from native/source evidence; obtain separately authenticated exact human false-positive decisions and preserve native reports with separate deterministic summaries (FR-024,025,027,028; SC-011,012,015).
- [ ] SEC013 [US5] Integrate fully remediated gate in `scripts/verify.py`, CI and canonical runtime contract; no partial-green bootstrap, synthetic acceptance or weakening of other controls (FR-023,026,028,030; SC-011–015).

### Convergence

- [ ] SEC014 [US5] Run every applicable Relentless native gate, supported3.11–3.13/Linux/Apple compatibility, real disposable PostgreSQL/Redis/migrations, installed behavior, collection retention and fresh committed-head traceability; update owned gap/evidence ledgers (FR-029,030; SC-013,015).
- [ ] SEC015 [US5] Independently review exact code/evidence, legitimately provision separately authorized required App/hosting with native readback, verify exact-head defect-fails/clean-passes controls, obtain merge approval and verify resulting main (FR-025,028,030; SC-014,015).

Dependencies: SEC001→002→analysis/validated preparation→003→004→005→006→008.
SEC007 may run alongside source/worker work in distinct files; SEC009 starts after source/worker
contracts, SEC010 requires009 and real native transport. SEC011/012/013 require the qualified
worker/issuer and their reserved exact approvals;014/015 require the complete implementation.
Analysis can authorize the isolated pure-parser preparation without treating future trust/activation
prerequisites as satisfied. SEC002 is not evidence that analysis or human domain review passed.

Independent parser test: invalid bytes/exit/schema produce a safe domain failure, distinct same-line
identities remain ordered, and findings have no approval path. Full US5 independent test remains
the complete native/trusted/hosted journey in the spec. Parallel examples: donor preservation007 and
source005 can be researched in separate files; only the root writer edits this checkout. Incremental
local commits are review checkpoints, never a partial secrets-gate publication or convergence claim.

### SEC003/004 exact preparation completion

Corrected parser e4c21a434a1244f197e970c8079b2af1802e593e independently reviewed SHIP for these
two tasks only.121 controls pass on each3.11/3.12/3.13, full native static/types/complexity pass,
and committed-source full3.11 compatibility passes1716/3skipped/55warnings in285.62s aggregate.
The three skips are two real-service checks and one Windows-only check, not three service checks.
All1598 baseline IDs and1706 first-pass IDs remain; corrected collection1719 adds13 regressions.
The first-pass REVISE and failing13-control evidence remain preserved. External exact-head review/
verification receipts are in the task outputs; code/report hashes were independently checked.
This documentation-only checkpoint changes no reviewed source or test bytes. SEC005–015 and every
human-owned checklist marker remain unchecked; no finding, full security control or merge is approved.


### SEC005 refinement and verification order

1. Complete/read preserved Phase0 probes; managed plan/model/source-binding contract refinements,
   then read-only managed analysis. Direction approval remains preparation only.
2. Write failing real-Git source/integrity and strict tree/identity/process controls at the plan's
   five test paths before source implementation. Keep every prior native test ID.
3. Implement the five cohesive owned script modules with actual caps/deadlines, raw-chain verification,
   exact tree-byte equality and full identity. No CLI, gate registration or finding acceptance.
4. Run source controls across3.11–3.13, all full native static/type/complexity checks, retained-ID
   comparison, committed-source compatibility and independent exact-artifact adversarial review.
5. Record actual limits and known worker deployment dependencies before checking SEC005 complete.
   SEC006–015, full OS matrix/service/hosting/individual-decision requirements remain open.

### SEC005 exact preparation completion - 2026-10-17

Code96b0faf9d4eb7d4fa21e5b4c2a298c673c0291d4 received independent SHIP for this source-binding
layer only. The three prior REVISE commits/reports remain preserved. The final exact-byte correction
rejects unsupported decoder inputs before their methods run; four controls were observed RED first.
A narrow real-child wait failure additionally proves safe private-error/log handling and child reap.
Root compared25 reviewed source/document hashes against actual files and committed blobs,19 review
evidence hashes and all18 owned source/test files. Source/test files are at most265 physical lines.

Exact committed full locked3.11 compatibility passed1883/3skipped/55warnings (272.54s pytest,
276.39s profile). Two skips require real services and one is Windows-only. All1719 previous IDs
remain, with167 new; all1881 immediately prior IDs remain plus5. Independent exact-head focused
controls pass288 each on3.11/3.12/3.13. Nine-module coverage is604statements/190branches,100% with
zero excluded lines, not whole-repository coverage. Seven currently configured native static gates,
both explicit strict checkers on18files, cognitive5 and full-scope C90110/E902 passed. The earlier
inline guard's cognitive6 failure and timing-sensitive coverage miss are retained as historical
failed results; neither threshold nor required gate changed.

See `evidence/security-policy/source-final-review.md` and `source-verification.md`. This closes only
the declared source-binding layer. Complete source/export enrollment, qualified scanner/Git release
and native execution provenance, full worker isolation, human decisions, supported OS/services,
hosting and SEC006–015 remain open. All11 human-owned checklist markers remain unchecked. Root
governance and individual decisions are unchanged; no acceptance, activation or merge follows.


### SEC006 readiness checkpoint

Phase0 research and root artifact/config measurements are preserved; see native-worker-readiness
contract. Resolve silent native read/process failures with independent design analysis before
implementation. SEC006 remains unchecked, as do SEC007-015 and all human criteria. Tool/config
byte measurements are preparation evidence only and cannot close the native-worker milestone.


### SEC006 native completion correction substeps

These author-owned substeps refineSEC006; checking one cannot close the entire native-worker task.

- [x] NC001 Preserve native upstream baselines; repair two production-engine generator imports without rule changes. Verified a990152f2651c61664e5d3f5e28350c8397cfe58; all275 IDs retained in both full engines; native lifecycle vet warning remains open forNC003.
- [ ] NC002 Add runtime old-parser error-loss counterexamples and strict native completion controls, then implement the opt-in parser endpoint.
- [x] NC003 Add failing native file/read/close/task/callback/cancellation controls, then propagate errors and collect actual source-group results. Independent SHIP atbc836bb1cff5feca40cf3e57131b810c3f59682c; both full engines291 IDs retain275 baseline; both vet modes and four independent probes pass. See evidence/security-policy/native-files-review.md.
- [ ] NC004 Add failing Git parser/child/early-abort/EOF-hang controls, then own/cancel/reap the strict native stream and join terminal results.
- [ ] NC005 Run full native engine/dependency checks, preserve IDs/schema/detectors, record build/module/patch identities and qualify platform behavior honestly.
- [ ] NC006 Independently review exact correction/artifacts, prepare revised import/qualification/worker contract; retain all reserved activation decisions.

OrderNC001→002→003→004→005→006. See `contracts/native-completion-correction.md`.
No native correction code has been accepted; no partial passing security gate is registered.
