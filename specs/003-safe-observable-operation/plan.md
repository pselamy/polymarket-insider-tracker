# Implementation Plan: Safe Observable Operation

**Branch**: `feat/slice-003-safe-observable-operation` | **Date**: 2026-09-10 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/003-safe-observable-operation/spec.md`, authorized by Patrick on 2026-09-10.

## Summary

Deliver safe, observable operation across the tracker pipeline:
1. Wire an HTTP health server exposing `/live`, `/ready`, `/health`, and `/metrics` on the effective port, supporting the `--health-port` override.
2. Propagate background worker failures (such as trade poller crash) to pipeline state, readiness, cleanup, and a nonzero CLI exit code (1).
3. Clarify `--config-check` as strictly offline configuration validation and distinguish it from runtime readiness.
4. Correct the default Polygon RPC endpoint to `https://polygon-rpc.com` and harden URL validators against malformed inputs (G-030a).
5. Decouple risk qualification from delivery deduplication, ensuring dry-run mode never contacts external channels or writes Redis dedup keys.
6. Enforce per-channel delivery deduplication with immediate retry on confirmed failures and a bounded 60-second window for ambiguous timeouts.
7. Unify the persisted risk assessment schema with slice 004 by adding Alembic migration `003_safe_observable_operation` with full upgrade and downgrade paths.
8. Implement a reusable, deterministic end-to-end integration test harness using working fakes and no `unittest.mock`.

## Technical Context

**Language/Version**: CPython 3.11, 3.12, 3.13; syntax baseline 3.11

**Primary Dependencies**: `aiohttp` for the health server; `prometheus-client` for metrics; `redis.asyncio` for deduplication; `sqlalchemy` and `alembic` for persistence; `pydantic-settings` for configuration; `httpx` for channels; `fakeredis` and custom fakes for test doubles. No new external dependencies.

**Storage**: PostgreSQL table `risk_assessments` (Alembic revision `003_safe_observable_operation`); Redis keys under `alert:dedup:{channel}:{wallet}:{market}` and `alert:ambiguous:{channel}:{wallet}:{market}`.

**Testing**: pytest and pytest-asyncio; AST policy strictly forbidding `unittest.mock`; shared Redis contracts; deterministic end-to-end suite with working fakes.

**Target Platform**: Ubuntu 24.04 x86_64 and Apple Silicon macOS.

**Project Type**: Long-running CLI monitoring service.

**Performance Goals**:
- Health probes respond in <100ms.
- Forced worker crash propagates to nonzero process exit within 5s.
- Dedup key lookup and write in <5ms.

**Constraints**:
- Maximum cognitive complexity <= 5 across all files via `complexipy_gate.py`.
- Strict typing via mypy and Pyright.
- Vulture clean.
- Black (100 cols) and Ruff formatted.
- Zero network calls to Discord/Telegram in test or dry-run.

**Scale/Scope**: Closes G-014 through G-021, G-030a, and G-033. Touches `pipeline.py`, `__main__.py`, `config.py`, `ingestor/health.py`, `detector/scorer.py`, `alerter/`, `storage/`, and `alembic/`.

## Constitution Check

*GATE: Passed before Phase 0 research; re-checked after Phase 1 design.*

| Principle | Assessment | Verdict |
|---|---|---|
| I. Research and Monitoring Only | Safe observation, health probes, and alert deduplication only. No trading, no simulation as real. | Pass |
| II. Truthful Contracts | Health routes reflect real states; `--config-check` does not falsely claim runtime readiness; default Polygon URL works. | Pass |
| III. End-to-End Evidence | Reusable deterministic end-to-end test drives trade from ingest to fake delivery and proves failure modes. | Pass |
| IV. Safe Effects & Durable Records | Dry run never contacts webhooks or poisons dedup state; assessments persist with explainable dispositions; persistence failures do not block delivery. | Pass |
| V. Compatibility & Gates | Public CLI and health routes preserved; schema migration includes downgrade; all static, compatibility, and service gates required. | Pass |

## Project Structure

### Documentation (this feature)

```text
specs/003-safe-observable-operation/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── health-endpoints.md
│   ├── delivery-deduplication.md
│   ├── assessment-storage.md
│   └── pipeline-lifecycle.md
├── checklists/
│   ├── requirements.md          # author-owned
│   └── operational.md           # reviewer-owned (unchecked)
└── tasks.md
```

### Source Code

```text
src/polymarket_insider_tracker/
├── __main__.py                  # CLI argument wiring, config-check, health-port, exit code
├── config.py                    # Polygon default RPC, URL validation diagnostics
├── pipeline.py                  # Health server lifecycle, worker task supervision, exit on crash
├── ingestor/
│   └── health.py                # Detailed health report, dependency checks, /live, /ready, /health, /metrics
├── detector/
│   ├── models.py                # Extended RiskAssessment with disposition, availability, reproducibility fields
│   └── scorer.py                # Remove dedup side effects from assess()
├── alerter/
│   ├── dispatcher.py            # Channel-scoped delivery, ambiguous outcome handling, dry-run safety
│   └── history.py               # Canonical Redis deduplication methods per channel
└── storage/
    └── models.py                # Updated RiskAssessmentModel with new columns
alembic/versions/
└── 20260910_0000_safe_observable_operation.py  # Migration 003_safe_observable_operation
tests/
├── integration/
│   └── test_end_to_end.py       # Reusable deterministic end-to-end test
├── alerter/
│   └── test_deduplication.py    # Multi-channel, dry-run, and ambiguity tests
├── ingestor/
│   └── test_health_server.py    # Health, live, ready, metrics HTTP tests
└── test_pipeline.py             # Failure propagation, lifecycle, and CLI exit tests
```

## Phase 0: Research

Completed in [research.md](research.md). Resolves health server routes, failure propagation, deduplication decoupling, multi-channel retry semantics, and unified assessment schema.

## Phase 1: Design & Contracts

Completed in:
- [data-model.md](data-model.md): Table schema, Redis key shapes, health models.
- [contracts/health-endpoints.md](contracts/health-endpoints.md): HTTP routes, schemas, headers.
- [contracts/delivery-deduplication.md](contracts/delivery-deduplication.md): Deduplication lifecycle and retry rules.
- [contracts/assessment-storage.md](contracts/assessment-storage.md): Migration and domain fields.
- [contracts/pipeline-lifecycle.md](contracts/pipeline-lifecycle.md): CLI options, exit codes, and supervisor.
- [quickstart.md](quickstart.md): Step-by-step verification commands.

## Human Validation

Patrick's 2026-09-10 directive authorizes the planning and implementation of slice `003-safe-observable-operation` under the approved constitution and governance boundaries.
