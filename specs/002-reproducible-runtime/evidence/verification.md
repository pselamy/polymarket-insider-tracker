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

## User Story 2: Required Local Gates

**Static profile**:

```text
uv run python scripts/verify.py --profile static
lock: passed
support-contract: passed
format: passed (81 files)
lint: passed
mypy: passed (42 source files, locked Python 3.11 dependency resolution)
status: passed
duration: 12.29s
exit: 0
```

The verifier itself was launched by Python 3.13.14. Its mypy gate intentionally resolves the locked
Python 3.11 dependency set because mypy and Ruff enforce the minimum supported language/API contract.
All other runtime gates use the verifier's selected interpreter, which prevents an isolated compatibility
run from escaping into a different project `.venv`.

**Apple Silicon compatibility matrix**:

Each command used a fresh uv isolated environment and the checked-in lock:

```text
uv run --isolated --locked --all-extras --python 3.11 python scripts/verify.py --profile compatibility --json
Python 3.11.15; 756 collected, 754 passed, 2 skipped; 74.455s; exit 0

uv run --isolated --locked --all-extras --python 3.12 python scripts/verify.py --profile compatibility --json
Python 3.12.13; 756 collected, 754 passed, 2 skipped; 73.804s; exit 0

uv run --isolated --locked --all-extras --python 3.13 python scripts/verify.py --profile compatibility --json
Python 3.13.14; 756 collected, 754 passed, 2 skipped; 73.439s; exit 0
```

The skips were the platform-specific Windows signal-handler case and the real-service test reserved for
the separately passing service profile. No required compatibility failure was ignored.

## User Story 3: Support Boundary

```text
uv run python scripts/check_support_contract.py
Support contract passed: tracked runtime surfaces are consistent.
exit: 0

uv sync --locked --all-extras --python 3.10 --dry-run
resolved Python 3.10.20; rejected by project requirement >=3.11,<3.14
exit: 2

uv sync --locked --all-extras --python 3.14 --dry-run
resolved Python 3.14.6; rejected by project requirement >=3.11,<3.14
exit: 2
```

`actionlint .github/workflows/ci.yml` also exited `0`. Action pins were resolved from the upstream Git
repositories on 2026-09-07: `actions/checkout` v4.4.0 and `astral-sh/setup-uv` v7.6.0.

The approved advisory label `macos-14` currently maps to GitHub's Apple Silicon arm64 runner. GitHub has
also announced that macOS 14 runner images will become unsupported on 2026-11-02; the workflow must move
to a then-supported arm64 label before that deadline. This lifecycle note does not change the current
green support boundary and is not a claim of evidence from the advisory job itself.

## Pending Evidence

Aggregate-profile, clean-checkout timing, Linux CI, final gap-register, final audit, and provenance evidence
are recorded by T025–T030 after their corresponding implementation gates.
