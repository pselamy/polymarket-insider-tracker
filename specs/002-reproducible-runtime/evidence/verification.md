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
repositories on 2026-09-07: `actions/checkout` v6.1.0 and `astral-sh/setup-uv` v7.6.0. The checkout pin
was advanced from v4.4.0 after the first Linux run reported its Node 20 action runtime as deprecated.
Per-job setup-uv cache suffixes prevent parallel matrix jobs from contending for one save key.

The approved advisory label `macos-14` currently maps to GitHub's Apple Silicon arm64 runner. GitHub has
also announced that macOS 14 runner images will become unsupported on 2026-11-02; the workflow must move
to a then-supported arm64 label before that deadline. This lifecycle note does not change the current
green support boundary and is not a claim of evidence from the advisory job itself.

## Complete Local Profile

The pinned Compose services were healthy before the aggregate command started:

```text
uv run --env-file .env.example python scripts/verify.py --profile all --json
lock: passed (0.016s)
support-contract: passed (0.044s)
format: passed, 81 files (0.046s)
lint: passed (0.021s)
mypy: passed, 42 source files (12.485s)
imports: passed (0.349s)
tests: passed (11.334s)
services: passed (0.224s)
migrations: passed (0.939s)
aggregate: passed in 25.458s
exit: 0
```

No gate was duplicated, skipped, or suppressed. The generated migration database identifier and configured
database credential were omitted from this artifact.

## Clean-Checkout Timing

Commit `709ba6bca8ee508ac961619622339df0e4718f4c` was cloned into a new temporary checkout on Apple Silicon.
The timed path used Python 3.13.14, a newly created empty `UV_CACHE_DIR`, no pre-existing `.venv`, and
already-present PostgreSQL/Redis container images. It included local clone, locked sync, service readiness,
and the complete `all` profile:

```text
uv sync --locked --all-extras --python 3.13
90 locked packages installed from an initially empty package cache

docker compose up -d --wait postgres redis
PostgreSQL healthy; Redis healthy

uv run --env-file .env.example python scripts/verify.py --profile all --json
all nine gates passed in 60.359s

complete clean-checkout path: 92s
target: under 300s
exit: 0
```

The cleanup trap removed only the temporary Compose project and its new volumes, then moved the temporary
checkout to macOS Trash. A post-run check found no matching temporary containers, volumes, or `/tmp`
directory. The original repository's service volumes were retained.

## GitHub Actions Evidence

**Immutable run**: [CI run 34162965900](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34162965900)

**Commit**: `60f443f27bc5d0bbbd33a0b78dcb57be31f03f96`

**Trigger and result**: feature-branch `push`; completed `success` from 2026-09-07T21:23:36Z through
2026-09-07T21:24:29Z.

| Job | Platform contract | Conclusion | Duration | Immutable job evidence |
|---|---|---|---:|---|
| Static required checks | Ubuntu 24.04 x86_64, Python 3.11 tooling | success | 26s | [job 101868373950](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34162965900/job/101868373950) |
| Python 3.11 compatibility | Ubuntu 24.04 x86_64 | success | 28s | [job 101868373844](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34162965900/job/101868373844) |
| Python 3.12 compatibility | Ubuntu 24.04 x86_64 | success | 27s | [job 101868373881](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34162965900/job/101868373881) |
| Python 3.13 compatibility | Ubuntu 24.04 x86_64 | success | 27s | [job 101868373825](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34162965900/job/101868373825) |
| PostgreSQL and Redis required checks | Ubuntu 24.04 x86_64, pinned real services | success | 37s | [job 101868373810](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34162965900/job/101868373810) |
| Apple Silicon advisory compatibility | macOS 14 arm64, Python 3.13 | success | 41s | [job 101868373870](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34162965900/job/101868373870) |
| Required checks | Stable blocking predecessor summary | success | 2s | [job 101868490341](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34162965900/job/101868490341) |

GitHub's check-run annotation API returned zero annotations for every job. This second run supersedes the
earlier green run at `709ba6b`, whose deprecated Node 20 checkout runtime and shared cache-save keys were
corrected before accepting CI evidence.

## Final Pre-Convergence Audit

**Date**: 2026-09-07

```text
SPECIFY_FEATURE_DIRECTORY=specs/002-reproducible-runtime \
  .specify/scripts/bash/check-prerequisites.sh --json --require-tasks --include-tasks
feature directory resolved explicitly; all required design/task documents present
exit: 0

spec clarification marker scan
no [NEEDS CLARIFICATION] markers in spec.md
exit: 0

requirements checklist
16 checked; 0 open

uv run python scripts/check_support_contract.py
Support contract passed: tracked runtime surfaces are consistent.
exit: 0

actionlint .github/workflows/ci.yml
exit: 0

git diff --check
exit: 0

high-confidence credential signatures across working tree and reachable Git history
no private-key, GitHub, AWS, Slack, Google, Discord-webhook, or Telegram-token signature found
exit: 0

uv run --env-file .env.example python scripts/verify.py --profile all --json
all nine gates passed in 25.723s
exit: 0

post-run pit_verify_% database count
0
```

**Approved deferral**: `checklists/runtime.md` remains 0/35 checked because it is reviewer-owned
requirements-quality evidence, not an implementation-completion ledger. Patrick's 2026-09-07 approval of
the recommended plan and its four analysis remediations explicitly authorized implementation while those
markers remain unchanged. No implementation task, required check, or owned gap is deferred.

The macOS 14 runner lifecycle note above is an external time-bounded maintenance obligation; at the
recorded run it remained supported, proved arm64, and passed. It does not convert any current gate to an
approved failure.

## Pending Evidence

Only the provenance inventory required by T030 remains before convergence.
