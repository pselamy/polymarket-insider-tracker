# Tasks: Safe Observable Operation

**Input**: Design documents from `specs/003-safe-observable-operation/`
**Prerequisites**: `spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/*.md`, `quickstart.md`, and Patrick's direct authorization of slice 003. Reviewer-owned `checklists/operational.md` markers are Patrick's and are left unchecked by agents.

**Tests**: Every behavior change begins with a failing test. `unittest.mock` is prohibited; working fakes, `fakeredis`, and real value objects are used.

## Format: `[ID] [P?] [Story] Description`
- **[P]**: Can run in parallel because it touches different files and has no incomplete dependencies.
- **[Story]**: Maps implementation to `US1`, `US2`, or `US3` from `spec.md`.

---

## Phase 0: Human Validation Gate

- [x] T001 Record Patrick's 2026-09-10 direct user authorization for slice `003-safe-observable-operation` in plan and tasks [Constitution §Development Workflow 3]

---

## Phase 1: Foundational (Storage Schema & External Diagnostics)

- [x] T002 [P] Write failing test for Alembic migration `003_safe_observable_operation` and updated `RiskAssessmentModel` in `tests/storage/test_risk_assessments_schema.py` [FR-012, FR-019, G-033]
- [x] T003 Implement migration `alembic/versions/20260910_0000_safe_observable_operation.py` and update `RiskAssessmentModel` and `RiskAssessment` domain dataclass with `delivery_disposition`, `delivery_channels`, `dry_run`, `volume_available`, `market_daily_volume`, `book_depth_available`, `wallet_tx_count`, and `wallet_age_known` [FR-012, FR-019, G-033]
- [x] T004 [P] Write failing tests for Polygon default endpoint and strict URL parsing diagnostics rejecting malformed strings like `wss://https://` in `tests/test_config.py` [FR-007, G-017, G-030a]
- [x] T005 Update `PolygonSettings.rpc_url` default to `https://polygon-rpc.com` and harden `_validate_http_url` / `_validate_websocket_url` with `urllib.parse.urlsplit` in `src/polymarket_insider_tracker/config.py` [FR-007, G-017, G-030a]

---

## Phase 2: User Story 1 - Know Whether Monitoring Is Actually Working (Priority: P1)

- [x] T006 [P] [US1] Add failing tests for HTTP health server endpoints (`/live`, `/ready`, `/health`, `/metrics`), port override, quiet period distinction, and degraded dependency states in `tests/ingestor/test_health_server.py` [FR-001, FR-002, FR-003, FR-004, SC-001, SC-006, G-014, G-015]
- [x] T007 [US1] Wire health server into `HealthMonitor` and `Pipeline`, implement `/live`, `/ready` (checking PostgreSQL, Redis, and poller), `/health` (separating acquisition and trade freshness), and `/metrics` [FR-001, FR-002, FR-003, G-014, G-015]
- [x] T008 [P] [US1] Add failing tests for `--health-port` flag override and `--config-check` offline syntax clarification in `tests/test_main.py` [FR-004, FR-005, SC-007, G-014]
- [x] T009 [US1] Update `src/polymarket_insider_tracker/__main__.py` to pass effective `--health-port` into settings/pipeline, update config summary, and clarify that `--config-check` validates offline syntax and config shape only [FR-004, FR-005, G-014]
- [x] T010 [P] [US1] Add failing tests for background worker failure propagation to `PipelineState.ERROR`, readiness 503, cleanup, and nonzero CLI exit code (1) in `tests/test_pipeline.py` [FR-006, SC-002, G-016]
- [x] T011 [US1] Implement worker task supervision in `src/polymarket_insider_tracker/pipeline.py` and `__main__.py` ensuring crashed poller triggers error state, stop event, and `EXIT_ERROR` exit code [FR-006, SC-002, G-016]

---

## Phase 3: User Story 2 - Exercise Alerts Without Poisoning Delivery State (Priority: P1)

- [x] T012 [P] [US2] Add failing tests verifying `RiskScorer.assess()` performs zero Redis dedup operations and makes no delivery decisions in `tests/detector/test_scorer.py` [FR-008, G-018]
- [x] T013 [US2] Remove Redis dedup checking and key setting from `src/polymarket_insider_tracker/detector/scorer.py` [FR-008, G-018]
- [x] T014 [P] [US2] Add failing tests for channel-scoped deduplication, dry-run safety (zero HTTP calls, zero dedup keys), failed-channel retry eligibility, and 60-second ambiguity window in `tests/alerter/test_deduplication.py` [FR-009, FR-010, FR-011, FR-017, FR-018, SC-003, SC-004, SC-008, G-018, G-019, G-020]
- [x] T015 [US2] Implement authoritative channel-scoped deduplication in `src/polymarket_insider_tracker/alerter/history.py` and `src/polymarket_insider_tracker/alerter/dispatcher.py` [FR-009, FR-010, FR-011, FR-017, FR-018, G-018, G-019, G-020]

---

## Phase 4: User Story 3 - Reproduce the Whole Monitoring Path Safely (Priority: P2)

- [x] T016 [P] [US3] Add failing test for deterministic end-to-end pipeline execution with fakes in `tests/integration/test_end_to_end.py` [FR-014, FR-020, SC-005, G-021]
- [x] T017 [US3] Implement deterministic end-to-end test harness verifying trade ingest -> profile -> detect -> score -> persist assessment -> dry-run suppression / fake delivery -> health transitions -> failure handling -> clean shutdown [FR-013, FR-014, FR-020, SC-005, G-021]

---

## Phase 5: Quality Gates, Evidence & Convergence

- [x] T018 Run static profile: `uv run python scripts/verify.py --profile static` (Black, Ruff, mypy, Pyright, Vulture, Complexipy <= 5)
- [x] T019 Run compatibility profile on Python 3.11, 3.12, 3.13: `uv run python scripts/verify.py --profile compatibility`
- [x] T020 Run services profile against loopback Redis and PostgreSQL: `uv run --env-file .env.example python scripts/verify.py --profile services`
- [x] T021 Measure line and branch coverage with `pytest --cov`
- [x] T022 Update `specs/audit/gap-register.md` closing G-014 through G-021, G-030a, and G-033
- [x] T023 Update `README.md` and user documentation
- [x] T024 Commit changes cleanly on `feat/slice-003-safe-observable-operation`
- [ ] T025 Generate `PHASE1_RESULT.md` in dispatch-state directory
