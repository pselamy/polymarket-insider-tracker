# Verification Evidence: Reproducible Supported Runtime

> **Historical note (2026-09-08):** This file records the commands and outputs used to approve the
> original slice. References below to the cross-file support checker are historical evidence, not a
> current contributor command. The checker was subsequently removed in favor of native configuration
> ownership plus lock, compatibility, service, and focused verifier gates.

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

## Provenance Inventory

**Parent anchor**: `52c5b764206141b80c98150eed8908a30fb1026b` (`docs: approve reproducible runtime plan`). This is the
parent of the first slice-002 implementation commit; earlier Spec Kit adoption, constitution, audit, review,
and approval artifacts are outside this implementation inventory.

**Intended paths**: The approved `plan.md` project structure and `tasks.md` target list bounded changes to
runtime metadata/lock state; database configuration and service definitions; README support/setup text;
CI and verification scripts; storage boundaries plus the two pre-existing type failures; focused tests;
slice evidence/task state; and only G-007–G-012/G-013a in the audit register. `.dockerignore` was the one
setup addition required by the implementation workflow before code changes.

The resulting union, verified with `git diff --name-status 52c5b76..57fa7c0`, is:

```text
A .dockerignore
M .env.example
M .github/workflows/ci.yml
M README.md
M alembic.ini
M alembic/env.py
M docker-compose.yml
M pyproject.toml
A scripts/check_support_contract.py
A scripts/runtime_services.py
A scripts/verify.py
A specs/002-reproducible-runtime/evidence/verification.md
M specs/002-reproducible-runtime/tasks.md
M specs/audit/gap-register.md
M src/polymarket_insider_tracker/config.py
M src/polymarket_insider_tracker/ingestor/gamma_client.py
M src/polymarket_insider_tracker/profiler/funding.py
M src/polymarket_insider_tracker/storage/database.py
A src/polymarket_insider_tracker/storage/database_url.py
M tests/detector/test_size_anomaly.py
A tests/integration/test_runtime_services.py
A tests/storage/test_database.py
A tests/storage/test_database_url.py
M tests/test_config.py
A tests/tooling/test_support_contract.py
A tests/tooling/test_verify.py
M uv.lock
```

Each actual commit inventory was independently read with
`git show --format='COMMIT %H %s' --name-status --no-renames <hash>`:

| Commit | Purpose | Verified actual paths |
|---|---|---|
| `9aa2cd05850ee1dbc04ea910ccec4597df00344a` | Lock supported runtime matrix | `.dockerignore`, `pyproject.toml`, `uv.lock`, task ledger |
| `559240c3b02009651595f44ef2aac102d565da51` | Enforce runtime support contract | support checker, checker tests, task ledger |
| `ef0b978d437a72eb0e651ff4563f647f2a641cd5` | Normalize PostgreSQL runtime URLs | URL module/tests, task ledger |
| `7eb69ae56961242642ae9df890f813b3e88ee32d` | Apply canonical database configuration | config/database code and tests, task ledger |
| `1383923338b5d7bf2f11eeb7eb2419047fb2a744` | Verify disposable local runtime | env/Alembic/Compose/README, service script/tests, evidence, task ledger |
| `6e724e2013958b06af96d00e9a357057d031d205` | Enforce fail-closed verification profiles | CI, verifier/tests, focused format/type fixes, task ledger |
| `709ba6bca8ee508ac961619622339df0e4718f4c` | Converge supported runtime contract | CI/README/verifier/checker tests, evidence, task ledger |
| `60f443f27bc5d0bbbd33a0b78dcb57be31f03f96` | Refresh action runtime and cache isolation | CI, evidence, task ledger |
| `57fa7c069f37bea574d388a3ba122a1c48baec45` | Record verification and close owned gaps | evidence, task ledger, bounded gap-register dispositions |

No rename, deletion, schema migration, product-source integration, alert delivery, or trading path appears
in the actual inventory. This provenance section is committed separately from the implementation hashes it
records; by design it does not attempt to embed its own commit hash.

## Post-Analysis Convergence and Adversarial Review

**Date**: 2026-09-07

**Implementation anchor**: `00d87fad69646719d287373100285230574c053c`

Patrick explicitly requested a sequential independent-agent pass on the approved Phase 7 convergence
work. Agy `gemini-3.8-flash-high` produced the first uncommitted implementation from base `f2d0ff7`.
Claude Code `fable` then reviewed that real diff and finished it without push, PR, or merge authority.
The resulting seven-file artifact was transferred to the local checkout through explicit patches and
verified against the remote artifact by matching Git object hashes before Codex accepted or changed it.

Claude's review rejected the first pass until it corrected Python 3.11/3.12 argparse behavior, malformed
database-URL prerequisite handling, cleanup ordering when a Redis close fails, additional URL/non-URL
credential redaction cases, incomplete environment-loading instructions, and support-checker drift
coverage. Codex then ran a separate refute-first review and found three further contradictions:

1. the service helper admitted `rediss://` and `unix://` values that application `RedisSettings` rejects;
2. the documented `.env`-loaded aggregate let deterministic pytest load live application endpoints,
   opened an outbound TLS connection, and exceeded the five-minute target;
3. `data-model.md` named Gate Result's identifier `gate_id` and described a different redaction-policy
   shape from the runtime contract and implementation.

Each finding was recorded as T039–T041 rather than hidden in the diff. The Redis-scheme regression was
observed red as `2 failed, 47 passed, 1 skipped`; the aggregate-environment runner assertion was observed
red as `1 failed`; and the data-model assertion was observed red as `1 failed`. The contaminated aggregate
was interrupted after it exceeded five minutes and a process/socket sample confirmed a live outbound TLS
read during pytest. The final design scrubs application configuration from the aggregate test subprocess
and runs pytest from an isolated temporary working directory, while the separately named service and
migration gates retain the loaded local service configuration.

**Final focused matrix** (locked isolated environments):

```text
Python 3.11.15: 52 collected; 51 passed, 1 skipped; 7.87s; exit 0
Python 3.12.13: 52 collected; 51 passed, 1 skipped; 6.59s; exit 0
Python 3.13.14: 52 collected; 51 passed, 1 skipped; 6.49s; exit 0
```

The skip is the explicitly opt-in real-service integration case, proven separately below.

**Deterministic suite with the contributor environment loaded**:

```text
uv run --env-file .env pytest -q
775 collected; 773 passed, 2 skipped; 16 warnings; 11.10s
real: 11.58s
exit: 0
```

The warnings are the existing WebSockets deprecation, legacy database-URL migration warnings exercised
by tests, and an existing mocked-coroutine resource warning. No test contacted a configured live endpoint.

**Complete real-service aggregate**:

```text
uv run --env-file .env python scripts/verify.py --profile all
lock: passed (0.01s)
support-contract: passed (0.05s)
format: passed, 81 files (0.06s)
lint: passed (0.07s)
strict-types: passed, 42 source files on locked Python 3.11 (12.13s)
imports: passed (0.24s)
tests: 775 collected; 773 passed, 2 skipped (11.24s)
services: PostgreSQL async query and Redis PING passed (0.22s)
migrations: head -> previous -> head; async query and cleanup passed (0.89s)
aggregate duration: 24.91s
wall clock: 24.97s
exit: 0
```

A separate post-run PostgreSQL query returned no database matching `pit_verify_%`, independently proving
that the generated sibling database was removed. The configured application database and retained local
service volumes were not removed.

Ruff format, Ruff lint, minimum-version mypy, `actionlint`, the support-contract checker, the explicit
feature prerequisite check, clarification-marker scan, diff check, and high-confidence working-diff
secret scan all exited `0`. The reviewer-owned runtime checklist remains intentionally unchanged at 0/35
per the approved baseline.

The final manual Spec Kit convergence rerun found zero open implementation tasks and zero stale
pre-implementation/old-schema phrases. `tasks.md` had SHA-256
`b8ee51d7bd1e6a1f07447ff1cb0f37d058f65235566886ad20c93170531848a2` both before and after the rerun,
so convergence made no further mutation.

### Fresh CI evidence for convergence implementation

**Immutable run**: [CI run 34168055921](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34168055921)

**Commit**: `00d87fad69646719d287373100285230574c053c`

**Trigger and result**: feature-branch `push`; completed `success` from 2026-09-07T22:50:59Z through
2026-09-07T22:51:51Z.

| Job | Conclusion | Duration | Immutable job evidence |
|---|---|---:|---|
| Static required checks | success | 23s | [job 101882927588](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34168055921/job/101882927588) |
| PostgreSQL and Redis required checks | success | 40s | [job 101882927705](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34168055921/job/101882927705) |
| Apple Silicon advisory compatibility | success | 43s | [job 101882927711](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34168055921/job/101882927711) |
| Python 3.11 compatibility | success | 28s | [job 101882927712](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34168055921/job/101882927712) |
| Python 3.12 compatibility | success | 27s | [job 101882927715](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34168055921/job/101882927715) |
| Python 3.13 compatibility | success | 28s | [job 101882927755](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34168055921/job/101882927755) |
| Required checks | success | 4s | [job 101883037803](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34168055921/job/101883037803) |

The check-run annotation API returned zero annotations for all seven jobs. This run supersedes the
expected red-test checkpoint run at `f2d0ff7` and is the authoritative convergence implementation proof.

## Phase 10: Required Vulture Dead-Code Gate

**Date**: 2026-09-08

**Base**: `a0c0d9945a3a38cec965e09a1ed2d5eb0c71d67f` · **Agy first pass**:
`c596aa74dd87f4eff2cc22a30de5a5d6e17640c8`, preserved unamended as the reviewable anchor.

**Environment**: Linux 6.8 x86_64, uv 0.11.21, verifier launched by CPython 3.13.14. Floor-pinned gates
(mypy, Pyright, Vulture) ran in the locked isolated Python 3.11 environment.

### Claude Code/fable adversarial review of the Agy first pass

Bare Vulture over the base tree with no configuration reported 51 findings. The Agy tree removed the
genuine dead code but hid the remaining 36 findings behind `min_confidence = 60`,
`ignore_decorators = ["@field_validator", "@pytest.fixture"]`, and
`ignore_names = ["model_config", "side_effect"]`. Findings and dispositions:

1. **Filtered scan (blocker)**: all three settings removed; `[tool.vulture]` now names only `paths`. The
   36 exposed findings were resolved through real structure: the four `@field_validator` methods became
   module-level functions attached with `Annotated[..., AfterValidator(...)]`; 21 `mock.side_effect = value`
   attribute stores became `configure_mock(side_effect=value)`; the nine `model_config` declarations are
   read by a contract test that pins the shared `.env` loading policy per settings group, backed by a
   behavioral test loading one shared `.env` through every group; the session autouse fixture now yields
   its isolated directory and `tests/test_harness_isolation.py` proves the runtime-contract isolation
   guarantee; the config cache fixture yields the reset used by the cache-reload test.
2. **Scope not named on the command (blocker)**: the verifier gate, `--help`, CI job, README, plan, and
   contract now run `vulture src tests scripts`, mirroring the Pyright explicit-scope precedent.
3. **Workflow simulation (blocker)**: the fail-closed test re-typed the aggregator script. Replaced by
   `tests/tooling/test_ci_workflow.py`, which parses the real `ci.yml`, binds the `vulture` job's command to
   `GATES["vulture"].command_text` and `DIRECT_GATE_COMMANDS`, asserts `if: always()` and the exact `needs`
   order, and executes the real aggregator `run:` script under GitHub's `bash --noprofile --norc -eo
   pipefail` for every blocking job × {failure, cancelled, skipped} plus the all-success case.
4. **`git diff --check` (blocker)**: trailing blank lines in this file and `tasks.md` removed.
5. **Deletions re-evaluated**: every Agy removal had no caller, test, export consumer, or documentation
   reference in history. `DatabaseManager.init_schema*`, `GracefulShutdown(exit_on_timeout=)`,
   `ShutdownTimeoutError`, and `RiskAssessmentRepository.get_by_assessment_id` were public surface, so the
   `CHANGELOG.md` gained a `Removed` section with migration notes; the shutdown docstring no longer claims
   force-exit on timeout, which was never implemented.
6. **Weak guard test**: the `traced_at` test now asserts an aware UTC timestamp bounded by the test clock.
7. **Undeclared test dependency**: `yaml` was imported through a transitive pre-commit dependency;
   `pyyaml>=6.0.0` is now declared in the dev extra (lock changed only in metadata entries).
8. **Documentation drift**: `data-model.md` profile table and gate identifier enum lacked `vulture`; FR-016,
   plan, contract, README, and the task ledger now state the unfiltered default-confidence contract; the
   static CI step name names Vulture; the Phase 10 ledger records Agy, fable, Codex, PR, CI, approval,
   merge, and post-merge states factually.

### Commands and results on the corrected tree

```text
uv run --isolated --locked --all-extras --python 3.11 vulture src tests scripts --config /dev/null
exit: 0 (no configuration file, default confidence, no ignore mechanism)

uv run --isolated --locked --all-extras --python 3.11 vulture src tests scripts
exit: 0

uv run python scripts/verify.py --profile static
lock: passed; format: passed; lint: passed
strict-types: passed (Success: no issues found in 41 source files)
pyright: passed (0 errors, 0 warnings, 0 informations)
vulture: passed
status: passed; duration: 7.12s; exit: 0

uv run python scripts/verify.py --profile compatibility
lock: passed; imports: passed
tests: 803 passed, 2 skipped, 16 warnings in 11.38s
status: passed; exit: 0

uv run --isolated --locked --all-extras --python 3.11 python scripts/verify.py --profile compatibility --json
status: passed; exit: 0
uv run --isolated --locked --all-extras --python 3.12 python scripts/verify.py --profile compatibility --json
status: passed; exit: 0

uv lock --check                       exit: 0
actionlint .github/workflows/ci.yml   exit: 0
git diff --check                      exit: 0
```

The two skips are the platform-specific Windows signal-handler case and the opt-in real-service integration
case. The 16 warnings are the pre-existing WebSockets deprecation, legacy database-URL migration warnings
exercised by tests, and the mocked-coroutine resource warning.

**Advisory complexity scan** (`complexipy --max-complexity-allowed 10 --failed` on changed behavior-heavy
files): every changed function is within budget. `scripts/verify.py` `run_verification` (15) and
`render_human` (11) exceed the budget but are unchanged by this slice; refactoring them is out of scope.

**Service profile on this host**: `uv run --env-file .env.example python scripts/verify.py --profile services`
exited `1`. The PostgreSQL listener on `127.0.0.1:5432` is not the Compose stack (password authentication
failed for user `tracker`), Redis on `6379` refused the connection, and the Docker socket denied access to
this user. Service and migration evidence for this slice is therefore not claimed locally and is left to
the orchestrator and the CI service job.

**Not performed**: no pull request, push, merge, or repository-setting change. Items T058–T063 remain open.

### Provenance

| Commit | Purpose |
|---|---|
| `c596aa74dd87f4eff2cc22a30de5a5d6e17640c8` | Agy first pass (reviewed, unamended) |
| `efd42073aa8f7ddecbf9373731479b811fa06758` | Claude Code/fable review fixes listed above |

This provenance entry is committed separately from the fix commit it records and does not embed its own hash.

### Codex refute-first review of the fable-corrected tree

Codex independently reviewed immutable fable head
`3e6a4bdde17373f7c5955f5886f604dbe0213b76` and found no unresolved blocker. The review covered the
complete base-to-head diff, every production deletion, the Pydantic validator rewrite, the canonical
verifier and direct-command contract, the real CI workflow and fail-closed aggregator tests, dependency
and lock metadata, active documentation, and the factual task state. The unfiltered policy was also
checked mechanically: no Vulture confidence override, ignored name/decorator, allowlist, baseline,
suppression, or path exclusion is present in the implementation.

Independent reproduction on macOS arm64 with uv 0.11.26 and CPython 3.13.14 produced:

```text
git diff --check a0c0d9945a3a38cec965e09a1ed2d5eb0c71d67f..HEAD      exit: 0
actionlint .github/workflows/ci.yml                                   exit: 0
uv lock --check                                                       exit: 0
uv run python scripts/verify.py --profile static                      status: passed
uv run --isolated --locked --all-extras --python 3.11 vulture
  src tests scripts --config /dev/null                                exit: 0
uv run --isolated --locked --all-extras --python 3.11 vulture
  src tests scripts                                                   exit: 0
uv run --isolated --locked --all-extras --python 3.11 python
  scripts/verify.py --profile compatibility --json                    status: passed
uv run --isolated --locked --all-extras --python 3.12 python
  scripts/verify.py --profile compatibility --json                    status: passed
uv run --isolated --locked --all-extras --python 3.13 python
  scripts/verify.py --profile compatibility --json                    status: passed
uv run pytest -q                                                      803 passed, 2 skipped
uv run --env-file .env.example python scripts/verify.py
  --profile services                                                  status: passed
```

The live service run reached PostgreSQL and Redis, then exercised the disposable Alembic sequence
`002_risk_assessments -> 001_initial -> 002_risk_assessments`, completed an async query, dropped its
temporary database, and left no `pit_verify_%` database behind. At the time of this local review, pull
request, GitHub CI, approval, merge, and post-merge evidence were not yet claimed; T059–T063 were open.

### Pull request CI checkpoint

Pull request [#115](https://github.com/pselamy/polymarket-insider-tracker/pull/115) was opened non-draft
from `quality/vulture-required-gate` into `main`. At the checkpoint below, its immutable base was
`a0c0d9945a3a38cec965e09a1ed2d5eb0c71d67f` and its implementation head was
`69654eee7528bbdec1bc67aff6dab2490df1c04a`.

Pull-request workflow run
[`34273501118`](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118) completed
with conclusion `success` on that exact head:

| Job | Conclusion | Duration | Immutable job |
|---|---|---:|---|
| PostgreSQL and Redis required checks | success | 56s | [102220735416](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118/job/102220735416) |
| Apple Silicon advisory compatibility | success | 37s | [102220735703](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118/job/102220735703) |
| Python 3.11 compatibility | success | 29s | [102220735874](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118/job/102220735874) |
| Python 3.12 compatibility | success | 32s | [102220735783](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118/job/102220735783) |
| Python 3.13 compatibility | success | 40s | [102220735899](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118/job/102220735899) |
| Static required checks | success | 36s | [102220735892](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118/job/102220735892) |
| Vulture dead code check | success | 20s | [102220735828](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118/job/102220735828) |
| Required checks | success | 4s | [102221051318](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34273501118/job/102221051318) |

The protected `main` branch remained strict and required the `Required checks` context, which passed only
after its four blocking predecessors (`static`, `vulture`, `compatibility`, and `services`) succeeded. At
this checkpoint the PR was open, non-draft, mergeable, and reported `CLEAN`; it had no review decision and
was not merged. Patrick's approval, merge, and post-merge confirmation remain pending (T061–T063).

### Agy corrective implementation pass

**Date**: 2026-09-08 · **Commit**: `fc8ae9009b83b53b7e5033527252e005c43c9557` (sole parent
`4df55be84ecb506946263e6e145b95dda726f3d9`), preserved unamended as the reviewable corrective anchor.
The sections above record what actually ran on the earlier heads and are not restated in this scope.

A fresh corrective pass was implemented on `quality/vulture-required-gate` starting from immutable checkpoint
`4df55be84ecb506946263e6e145b95dda726f3d9` to resolve two independently reproduced blockers:

1. **Tracked repository Python files omitted from Vulture scope (blocker)**:
   The Vulture gate claimed complete repository dead-code coverage but scanned only `src`, `tests`, and
   `scripts`. Exactly four tracked repository-owned Python files were omitted: root `conftest.py`,
   `alembic/env.py`, and the two migration revisions under `alembic/versions/`
   (`20260104_0000_initial_schema.py` and `20260522_1130_risk_assessments.py`).
   The bare scan `vulture conftest.py alembic --config /dev/null` reported 14 Pytest/Alembic convention-based
   entry points (2 in `conftest.py` and 6 in each migration revision).
2. **Type suppression in CI contract tests (blocker)**:
   `tests/tooling/test_ci_workflow.py` contained `# type: ignore[union-attr]`, violating the
   suppression-free repository contract.

#### Findings and dispositions

1. **Canonical explicit scope extended everywhere to `src tests scripts alembic conftest.py`**:
   The explicit five-path scope was applied to `pyproject.toml` `[tool.vulture].paths`,
   `scripts/verify.py` `GATES["vulture"]` command tuple, `DIRECT_GATE_COMMANDS`, `--help` epilog,
   `.github/workflows/ci.yml` `vulture` job step, `README.md`, and all Spec Kit Phase 10 artifacts
   (`spec.md` FR-016, `plan.md`, `contracts/runtime-verification.md`, `tasks.md`, and this file).
   No loose glob pattern is used.
2. **Framework-consumed entry points made visible through real Python structure**:
   Explicit `__all__` exports were added in root `conftest.py` (`event_loop_policy`, `pytest_plugins`),
   `alembic/versions/20260104_0000_initial_schema.py` (`branch_labels`, `depends_on`, `down_revision`,
   `downgrade`, `revision`, `upgrade`), and `alembic/versions/20260522_1130_risk_assessments.py`
   (`branch_labels`, `depends_on`, `down_revision`, `downgrade`, `revision`, `upgrade`).
   `alembic/env.py` was verified clean under Vulture (0 findings) as its runner functions are invoked
   directly at module level. No baseline, allowlist, ignore name/decorator, confidence threshold,
   exclusion, inline suppression, noqa, or grandfathering mechanism was used. Both the bare
   `--config /dev/null` scan and the configured scan over all five paths exit 0 with zero findings.
3. **Type suppression removed**:
   Replaced `# type: ignore[union-attr]` in `tests/tooling/test_ci_workflow.py` with an ordinary
   `_matched_job` helper function containing an explicit regex match assertion. Strict typing and
   readability are preserved with zero type suppressions.
4. **Drift-catching tests added**:
   - `test_vulture_scope_matches_between_ci_pyproject_and_verifier`: verifies that `pyproject.toml`,
     `verify.py` gate tuple, direct command help, and the CI workflow all share the exact canonical
     scope and order (`src tests scripts alembic conftest.py`).
   - `test_all_tracked_python_files_are_covered_by_vulture_scope`: dynamically queries `git ls-files "*.py"`
     and asserts that 100% of tracked repository-owned Python files are covered by the explicit scope,
     and every scope entry covers at least one tracked file.
   - `test_vulture_job_is_independent_and_runs_the_canonical_gate_command` and fail-closed aggregator
     tests in `test_ci_workflow.py` verify that the independent job and protected aggregator remain
     fail-closed.
5. **Task ledger and review state kept factual**:
   PR #115 and earlier CI run `34273501118` remain recorded for head `69654ee`. CI on the corrective
   head, re-reviews, approval, merge, and post-merge confirmation remain pending (T065–T067 and
   T061–T063 open).

#### Verification commands and results

```text
git diff --check                                                      exit: 0
actionlint .github/workflows/ci.yml                                   not available locally
uv lock --check                                                       exit: 0
uv run --isolated --locked --all-extras --python 3.11 vulture
  src tests scripts alembic conftest.py --config /dev/null            exit: 0
uv run --isolated --locked --all-extras --python 3.11 vulture
  src tests scripts alembic conftest.py                               exit: 0
uv run python scripts/verify.py --profile static                      status: passed (exit 0)
uv run python scripts/verify.py --profile compatibility               status: passed (exit 0, 805 passed, 2 skipped)
uv run --isolated --locked --all-extras --python 3.11 python
  scripts/verify.py --profile compatibility --json                    status: passed (exit 0)
uv run --isolated --locked --all-extras --python 3.12 python
  scripts/verify.py --profile compatibility --json                    status: passed (exit 0)
uv run --isolated --locked --all-extras --python 3.13 python
  scripts/verify.py --profile compatibility --json                    status: passed (exit 0)
```

### Claude Code/fable adversarial review of the Agy corrective commit

**Date**: 2026-09-08 · **Reviewed**: `fc8ae9009b83b53b7e5033527252e005c43c9557` (sole parent
`4df55be84ecb506946263e6e145b95dda726f3d9`, clean worktree), left unamended; fixes below are committed on
top of it. Environment: Linux 6.8 x86_64, uv 0.11.21, verifier launched by CPython 3.13.14; floor-pinned
gates ran in the locked isolated Python 3.11 environment.

Independently reproduced before reviewing the fixes: at `4df55be` exactly four tracked Python files sat
outside the Vulture scope (`conftest.py`, `alembic/env.py`, and the two migration revisions), and the bare
scan `vulture conftest.py alembic --config /dev/null` reported 14 default-confidence findings (2 + 6 + 6)
with `alembic/env.py` clean. At `fc8ae90` the exact pinned command over `src tests scripts alembic
conftest.py` exits 0 with and without `--config /dev/null`. Findings and dispositions:

1. **Historical evidence rewritten (blocker)**: the corrective commit changed the earlier "Claude
   Code/fable adversarial review of the Agy first pass" finding 2 and the "Commands and results on the
   corrected tree" block from `vulture src tests scripts` to the five-path command, although only the
   three-path command ever ran on heads `efd4207`, `3e6a4bd`, and `69654ee`. Both hunks are restored to the
   text recorded at `4df55be`; the wider scope is described only in the dated corrective sections.
2. **Corrective section unprovenanced (blocker)**: the "Agy corrective implementation pass" section named
   no date or commit and pointed open work at `T061–T063`, which do not cover the corrective-head CI run
   or re-reviews. The section now records its date, commit, and parent; the ledger gained `T064`
   (Agy corrective commit, done), `T065` (this review), `T066` (Codex re-review), and `T067` (CI on the
   final corrective head, noting that `T060` covers head `69654ee` only). `T065–T067` and `T061–T063` are
   open.
3. **`__all__` exports verified as real framework structure**: `alembic.script.ScriptDirectory`
   loads both revisions with every exported name defined; `alembic upgrade head --sql` and
   `alembic downgrade head:base --sql` each emit the full 17-statement DDL sequence in offline mode; root
   `conftest.py` still registers `pytest_asyncio` and the session `event_loop_policy` override, and Pytest
   collects 807 tests. No baseline, allowlist, `ignore_names`, `ignore_decorators`, confidence option, path
   exclusion, inline suppression, `noqa`, wrapper filtering, or grandfathering exists in the implementation
   or the changed tests (`grep` over `pyproject.toml`, `scripts/verify.py`, `conftest.py`, `alembic/`, and
   `tests/tooling/`). Ruff reports no undefined `__all__` member in `alembic/` or `conftest.py`.
4. **Coverage test reviewed**: `git ls-files "*.py"` matches at any depth (git pathspec `*` crosses `/`),
   lists only tracked files, runs from the repository root under `actions/checkout`, and the exactly-one
   match assertion rejects overlapping or duplicate entries while the non-empty assertion rejects unused
   or empty entries. The `typings/*.pyi` stubs are excluded deliberately: they describe third-party APIs,
   and Vulture's directory discovery collects only `*.py`.
5. **Test readability**: `tomllib` is imported at module level in both tooling test modules; the
   canonical scope and command constants sit with the other workflow constants instead of between tests;
   the 100+ column f-string assertion became a named constant. Two pre-existing strict Pyright errors in
   the same file (a `Literal`-keyed `dict.fromkeys` result passed as `Mapping[str, str]`) are fixed with an
   explicit `dict[str, str]` annotation; `tests/tooling` is now clean under strict Pyright and contains
   no type suppression.
6. **Active documentation**: README, FR-016, plan, contract, and `T050` now state that the five paths
   hold every tracked repository Python file, and the contract names the coverage test; over-long lines
   introduced by the corrective commit are re-wrapped. No three-path claim remains outside historical
   sections. `CHANGELOG.md`, `data-model.md`, and `quickstart.md` make no scope claim and are unchanged.
7. **Fail-closed CI**: the independent `vulture` job has no `needs`, `if`, or `continue-on-error`; the
   aggregator keeps `if: always()`, the exact `needs` order, and the per-job `test ... = success` script,
   and the real-script tests still fail it for every job × {failure, cancelled, skipped}.

Out of scope and unchanged: the Ruff lint gate still names `src tests scripts`, so `alembic/` and
`conftest.py` are formatted by Black but not linted by Ruff in the gate; extending it is a separate slice.

#### Commands and results on the fable-corrected corrective tree

```text
git diff --check                                                      exit: 0
uv run --isolated --locked --all-extras --python 3.11 vulture
  src tests scripts alembic conftest.py --config /dev/null            exit: 0
uv run --isolated --locked --all-extras --python 3.11 vulture
  src tests scripts alembic conftest.py                               exit: 0
uv run python scripts/verify.py --profile static
  lock, format, lint, strict-types (41 source files), pyright
  (0 errors), vulture                                                 status: passed, exit 0
uv run pytest -q                                                      805 passed, 2 skipped, 16 warnings
uv run --isolated --locked --all-extras --python 3.11 python
  scripts/verify.py --profile compatibility --json                    status: passed, exit 0
uv run --isolated --locked --all-extras --python 3.12 python
  scripts/verify.py --profile compatibility --json                    status: passed, exit 0
uv run --isolated --locked --all-extras --python 3.13 python
  scripts/verify.py --profile compatibility --json                    status: passed, exit 0
pyright tests/tooling (advisory, outside the gate)                    0 errors
ruff check alembic conftest.py (advisory, outside the gate)           exit: 0
alembic upgrade head --sql / downgrade head:base --sql (offline)      exit: 0, 17 statements each
complexipy tests/tooling --max-complexity-allowed 10 --failed         none over budget
actionlint .github/workflows/ci.yml                                   not installed on this host
uv run --env-file .env.example python scripts/verify.py
  --profile services                                                  exit: 1
```

The service profile failed at the probe: the PostgreSQL listener on `127.0.0.1:5432` is not the Compose
stack (password authentication failed for user `tracker`), Redis on `6379` refused the connection, and the
Docker socket denied access to this user. Live service and migration evidence for the corrective head is
therefore not claimed here and is left to the CI `services` job. No pull request, push, review, merge, or
repository-setting change was performed; `T066–T067` and `T061–T063` remain open until true.

#### Provenance

| Commit | Purpose |
|---|---|
| `fc8ae9009b83b53b7e5033527252e005c43c9557` | Agy corrective pass (reviewed, unamended) |
| `2a909a6619ba8091ef8ed85e96f5a64ea7f979db` | Claude Code/fable review fixes listed above |

This provenance entry is committed separately from the fix commit it records and does not embed its own hash.
