# Operational and Safe Observability Requirements Checklist: Safe Observable Operation

**Purpose**: Review whether the health routes, readiness checks, failure propagation, dry-run safety, deduplication scoping, ambiguity handling, URL validation, and storage migration requirements are complete, clear, consistent, and measurable before implementation.
**Created**: 2026-09-10
**Feature**: [spec.md](../spec.md)

**Note**: This custom checklist is a reviewer-owned requirements-quality review artifact.
**Review Ownership**: Patrick or a human reviewer he designates owns the `[x]` judgment.
**Marker Semantics**: `[x]` means the criterion has been reviewed and satisfied for requirements quality. It does not mean code is complete.
**Implementation Gate**: All newly generated items remain unchecked `[ ]`. Agents must not mark reviewer-owned criteria satisfied.

---

## Health Interfaces & Observability

- [ ] CHK001 Are the four health interfaces (`/live`, `/ready`, `/health`, `/metrics`) and their exact paths, methods, response codes, and schemas specified unambiguously? [Completeness, Spec §FR-001; Contract §Health Endpoints]
- [ ] CHK002 Does the specification define liveness as process-only and readiness as dependency-backed so that downstream recovery does not trigger liveness reboot loops? [Clarity, Spec §FR-002; Research §Decision 1]
- [ ] CHK003 Are acquisition freshness (time since last poll attempt) and trade freshness (time since latest observed trade) separately reported so quiet periods are not misdiagnosed as disconnected feeds? [Measurability, Spec §FR-003, §SC-001; Data Model §DetailedHealthReport]
- [ ] CHK004 Is the `--health-port` CLI argument specified to override both environment variables and defaults consistently in the HTTP listener and configuration summaries? [Consistency, Spec §FR-004, §SC-006; Contract §Pipeline Lifecycle]
- [ ] CHK005 Is `--config-check` explicitly limited to offline validation without network I/O, preventing misleading "ready to run" claims without reachable services? [Truthfulness, Spec §FR-005, §SC-007; Research §Decision 3]

## Failure Propagation & Process Lifecycle

- [ ] CHK006 Is the propagation path from a crashed background ingestion task to `PipelineState.ERROR`, readiness failure (HTTP 503), orderly resource cleanup, and nonzero CLI exit code (1) fully mapped? [Completeness, Spec §FR-006, §SC-002; Contract §Pipeline Lifecycle]
- [ ] CHK007 Is the shutdown timeout bounded (30s) and does graceful termination guarantee that background tasks do not become orphaned? [Safety, Spec §SC-002; Data Model §Pipeline Lifecycle]

## External Defaults & Diagnostic Hardening

- [ ] CHK008 Is the default Polygon RPC endpoint specified with a verified free public RPC URL (`https://polygon-rpc.com`) and fallback so that no-key quickstart runs succeed out of the box? [Truthfulness, Spec §FR-007; Research §Decision 4]
- [ ] CHK009 Are URL parsing and validation hardened with `urllib.parse.urlsplit` to reject malformed multi-protocol values such as `wss://https://` with actionable error diagnostics? [Robustness, Spec §FR-005; Gap G-030a]

## Delivery Deduplication & Safe Effects

- [ ] CHK010 Is risk scoring strictly decoupled from deduplication such that `RiskScorer.assess()` produces zero mutations on Redis or external delivery state? [Separation of Concerns, Spec §FR-008; Research §Decision 5]
- [ ] CHK011 Does dry-run mode guarantee zero calls to Discord/Telegram and zero newly written Redis dedup keys across 100% of qualifying assessments? [Safety, Spec §FR-009, §SC-003; Contract §Delivery Deduplication]
- [ ] CHK012 Is the deduplication identity defined strictly as `channel` + normalized `wallet` + `market`, preserving the deduplication window across side and score variations? [Clarity, Spec §FR-017; Data Model §DeduplicationKey]
- [ ] CHK013 Is deduplication scoped per-channel so that a failed Discord delivery does not suppress an eligible retry even if Telegram succeeded? [Completeness, Spec §FR-010, §SC-004; Contract §Delivery Deduplication]
- [ ] CHK014 Are ambiguous outcomes (such as HTTP timeouts) governed by a 60-second ambiguity window that temporarily suppresses rapid retries and flags eventual retries as possible duplicates? [Edge Case, Spec §FR-018, §SC-008; Research §Decision 5]
- [ ] CHK015 Are competing deduplication mechanisms in `AlertHistory` unified under the authoritative per-channel lifecycle? [Consistency, Spec §FR-011; Research §Decision 5]

## Storage Contract & Migration Unification

- [ ] CHK016 Does Alembic revision `003_safe_observable_operation` own the unified assessment schema covering slice 003 delivery dispositions and slice 004 evidence availability fields? [Storage Contract, Spec §FR-019; Gap G-033; Contract §Assessment Storage]
- [ ] CHK017 Are all newly added columns in `risk_assessments` defined with explicit types, nullability, and sensible defaults? [Data Integrity, Data Model §Table Schema]
- [ ] CHK018 Is the migration downgrade path verified to completely revert added columns and index without data corruption in previous revisions? [Migration Safety, Contract §Assessment Storage]
- [ ] CHK019 Is assessment persistence failure observable in logs and metrics while never blocking an otherwise authorized alert delivery attempt? [Resilience, Spec §FR-013; Constitution §IV]

## Deterministic Verification & End-to-End Harness

- [ ] CHK020 Does the reusable deterministic end-to-end test harness cover the complete trade lifecycle without mocking frameworks or external network calls? [Verification, Spec §FR-014, §FR-020, §SC-005; Research §Decision 7]
- [ ] CHK021 Are all cognitive complexity scores across new and modified code proven <= 5 under the strict fail-closed Complexipy launcher? [Complexity Budget, Constitution §V; AGENTS.md]
