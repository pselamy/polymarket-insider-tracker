# Verification Evidence: Reproducible Supported Runtime

All commands were run from the repository root. Output recorded here is deliberately redacted: database
credentials and the generated disposable-database identifier are omitted.

## User Story 1: Local Foundation

**Date**: 2026-09-07

**Parent implementation anchor**: `7eb69ae56961242642ae9df890f813b3e88ee32d`

**Environment**:

- macOS 26.4 (build 25E246), Apple Silicon `arm64`
- Python 3.13.14
- uv 0.11.26 (`aarch64-apple-darwin`)
- PostgreSQL 15 image `postgres:15@sha256:9b1d34adbce1dd07ee6e94b4a2cf698884b89bd44a6c9c12f5da8f3acbfe4957`
- Redis 7 image `redis:7@sha256:71da9275c5f3fcb97d0fa0c8c5b36cc995327265420f17a04bfd544f458059f7`

**Service integration test**:

```text
RUN_SERVICE_TESTS=1 uv run pytest tests/integration/test_runtime_services.py -q
8 passed in 1.35s
exit: 0
```

**Independent service command**:

```text
uv run python scripts/runtime_services.py --phase all --json
{"async_query_succeeded":true,"cleanup_succeeded":true,
 "migration_states":["002_risk_assessments","001_initial","002_risk_assessments"],
 "phase":"all","postgres_reachable":true,"redis_reachable":true,"status":"passed",
 "temporary_database":"<generated pit_verify_* database>"}
exit: 0
```

The command used the canonical loopback `DATABASE_URL` and local `REDIS_URL` from `.env.example`. A
separate post-run query returned `0` databases matching `pit_verify_%`, independently confirming cleanup.
The configured `polymarket_tracker` application database was neither downgraded nor dropped.

## Pending Evidence

Compatibility, aggregate-profile, clean-checkout timing, Linux CI, unsupported-version, final gap-register,
and provenance evidence are recorded by T020 and T024–T030 after their corresponding implementation gates.
