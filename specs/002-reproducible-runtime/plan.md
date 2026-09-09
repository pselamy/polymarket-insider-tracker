# Implementation Plan: Reproducible Supported Runtime

**Branch**: `codex/spec-kit-brownfield-adoption` | **Date**: 2026-09-06 | **Spec**: [spec.md](spec.md)

**Input**: Approved feature specification and recommended implementation plan from
`specs/002-reproducible-runtime/spec.md`; implementation authorized by Patrick on 2026-09-07

## Summary

Make Python 3.11, 3.12, and 3.13 reproducible on Linux and Apple Silicon by closing the
dependency contract, standardizing PostgreSQL access on one Psycopg 3 URL usable by both synchronous
Alembic and the asynchronous application, and regenerating the universal uv lock. Add a typed,
testable verification entry point that composes lock freshness, formatting, lint, strict type,
deterministic test, service-connectivity, and disposable-database migration gates. CI uses that same
entry point for a blocking Linux matrix and service job plus an advisory Apple Silicon job.

## Technical Context

**Language/Version**: CPython 3.11, 3.12, and 3.13; lowest supported syntax remains Python 3.11

**Primary Dependencies**: uv `>=0.11,<0.12` project/lock workflow; SQLAlchemy 2.x with its `asyncio`
extra; Psycopg 3 with binary distribution; Alembic 1.x; redis-py 5+; pytest, Black, Ruff, mypy,
Pyright `1.1.411`, Vulture `2.16`, and Complexipy `8.0.1`

**Storage**: PostgreSQL 15 and Redis 7; no persisted schema change in this slice

**Testing**: pytest and pytest-asyncio; table-driven unit tests for verification behavior; real
PostgreSQL/Redis integration tests for connectivity and migrations

**Target Platform**: Linux support verified on the blocking Ubuntu 24.04 x86_64 reference runner for
Python 3.11/3.12/3.13; Apple Silicon macOS as a supported contributor/release platform, with `macos-14`
arm64 advisory automation and local full evidence

**Project Type**: Single Python package providing a long-running CLI monitoring service

**Performance Goals**: After the documented prerequisites are installed and service container images are
locally available, the clean-checkout foundation path—including locked Python dependency installation—
completes service readiness, migrations, and aggregate verification in under five minutes

**Constraints**: Required gates use the checked-in lock; diagnostics redact credentials; migration
downgrade/re-upgrade runs only against a disposable loopback database; no external Polymarket, Polygon,
Discord, or Telegram call is part of deterministic verification

**Scale/Scope**: 38 source files, 27 test files, 703 collected baseline tests, two migrations, one CI
workflow, and the seven owned audit gaps G-007 through G-012 plus G-013a

## Constitution Check

*GATE: Passed before Phase 0 research; re-checked after Phase 1 design.*

| Rule | Pre-design evidence | Result |
|---|---|---|
| Research and monitoring only | Verification exercises local dependencies and fakes; it sends no alerts and places no trades. | Pass |
| Truthful, source-grounded contracts | Driver and async-extra choices are grounded in current SQLAlchemy documentation and reproduced failures. | Pass |
| End-to-end evidence over isolated coverage | This foundation slice proves the runtime and real services; slices 001 and 003 retain ownership of full monitoring-path E2E proof. | Pass |
| Safe effects and durable records | Migration downgrade is restricted to an auto-created disposable loopback database and is always cleaned up. | Pass |
| Compatibility and reproducibility | The plan preserves public environment names, supplies a URL migration path, locks dependencies, and makes required Linux gates blocking. | Pass |
| Spec Kit workflow | Slice 002 was explicitly activated; scope, clarification, analysis remediations, and implementation were approved in sequence. | Pass |

No constitution exception is requested.

## Project Structure

### Documentation (this feature)

```text
specs/002-reproducible-runtime/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── runtime-verification.md
├── checklists/
│   ├── requirements.md
│   └── runtime.md
└── tasks.md
```

### Source Code (repository root)

```text
pyproject.toml                       # Python/dependency/tool support contract
uv.lock                              # universal locked resolution
.env.example                        # canonical local service settings
docker-compose.yml                  # digest-pinned local PostgreSQL/Redis services
alembic.ini
alembic/env.py                       # synchronous migration entry point
.github/workflows/ci.yml             # blocking Linux + advisory Apple matrix
README.md                            # supported setup and aggregate commands
scripts/
├── runtime_services.py              # safe real-service and disposable migration proof
└── verify.py                        # aggregate verification entry point
src/polymarket_insider_tracker/
├── config.py                        # URL parsing, compatibility, redaction
├── storage/database.py              # sync/async engines using canonical URL
└── ...                              # unchanged product modules
tests/
├── integration/
│   └── test_runtime_services.py     # real PostgreSQL/Redis and migration smoke
├── tooling/
│   └── test_verify.py
├── test_config.py
├── ingestor/
├── profiler/
├── detector/
├── alerter/
└── storage/
specs/002-reproducible-runtime/evidence/
└── verification.md                  # captured platform, timing, and required-gate evidence
```

**Structure Decision**: Retain the existing single-package `src/` layout. Contributor verification
belongs in root `scripts/`, while tests mirror whether they exercise tooling, real services, or product
modules. No new service, package, or migration is needed.

## Design Decisions

### 1. Finite support contract

- Change `requires-python` and the lock boundary to `>=3.11,<3.14`.
- Declare `[tool.uv] required-version = ">=0.11,<0.12"`; CI installs a reviewed exact uv 0.11 release,
  while the repository contract rejects unsupported uv versions.
- Keep Black, Ruff, mypy, and Pyright configured for Python 3.11 because it is the minimum accepted
  syntax/API level. Pyright independently checks the complete production package in strict mode after mypy.
- Keep each runtime declaration in its native authority: package support in `pyproject.toml`, resolved
  support in `uv.lock`, executable platform evidence in CI, and contributor guidance in `README.md`.
  Validate those surfaces through their real consumers instead of reparsing them in a bespoke checker.
- Treat Ubuntu 24.04 x86_64 as the blocking Linux reference environment. Documentation may state Linux
  support but MUST NOT imply that every distribution, libc, or architecture is separately certified.
- Treat Python 3.14+ as unsupported until a later spec adds it after the complete locked matrix passes.

### 2. One safe PostgreSQL contract

- Replace bare SQLAlchemy with `sqlalchemy[asyncio]` so `greenlet` is required on every platform.
- Add `psycopg[binary]` as the PostgreSQL runtime driver.
- Document `postgresql+psycopg://...` as the canonical `DATABASE_URL`. SQLAlchemy selects the sync
  implementation for `create_engine()`/Alembic and the async implementation for
  `create_async_engine()` from this same URL.
- Preserve existing bare `postgresql://` values by normalizing them to the canonical dialect with an
  actionable deprecation warning. Treat `postgresql+asyncpg://` as a legacy input and normalize it with
  the same warning rather than silently depending on an undeclared driver. Preserve driver-neutral
  components and portable query parameters; reject incompatible driver-specific options before engine
  creation with an actionable redacted migration error. Never log the full URL.
- Make Alembic require `DATABASE_URL`; remove the misleading fallback and stale
  `SQLALCHEMY_DATABASE_URL` comment.

### 3. Verification profiles and failure semantics

- `scripts/verify.py` exposes the profiles defined in
  [contracts/runtime-verification.md](contracts/runtime-verification.md).
- Every gate is an explicit ordered record with one command, prerequisites, and exit status. The
  aggregate exits nonzero and names the first failed gate while preserving that command's output.
- Unit tests inject a fake command runner and exercise one failure per required gate. There is no
  production flag that fabricates success or weakens a gate.
- Keep `strict-types` as the existing mypy gate, keep `pyright` immediately after it, keep `vulture`
  immediately after `pyright`, and add a separate `complexipy` cognitive complexity gate immediately after
  `vulture`. The exact Complexipy command uses the locked Python 3.11 environment and names its scope on
  the command line
  (`uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore --ignore-complexity=false --snapshot-ignore=true --snapshot-create=false --exclude=. --check-script=true`),
  covering functions and module-level control flow in every tracked repository Python file. Complexipy
  enforces a maximum cognitive complexity of 5 with inline ignores, report-only mode, automatic snapshot
  use/creation, and cwd-config exclusions explicitly neutralized; no ignore comments, escape hatches,
  baselines, grandfathering, diff modes, or real path exclusions are permitted. The launcher validates
  the exact visible arguments, resolves the scope to absolute paths, and invokes the pinned CLI from a
  fresh configuration-free working directory.
- The service profile invokes separately identifiable `services` and `migrations` gates through
  `scripts/runtime_services.py --phase probe` and `--phase migrations`; the helper defaults to `all` for
  contributors. The probe performs an async SQLAlchemy query and Redis `PING`. The migration phase refuses
  non-loopback hosts, creates a unique sibling database, applies migrations to head, downgrades one
  revision, re-upgrades, confirms head, and drops the database in cleanup.

### 4. CI topology

- Grant only `contents: read` workflow permission. Pin every third-party action to a reviewed full commit
  SHA with a release-comment annotation.
- Install an exact approved uv 0.11 release from the SHA-pinned setup action and run
  `uv sync --locked --all-extras`; stop using an independent pip resolution.
- Run a blocking static job containing both independent type gates, Vulture, and Complexipy, an independent
  blocking `vulture` job on `ubuntu-24.04`, an independent blocking `complexipy` job on `ubuntu-24.04`,
  a blocking Linux compatibility matrix for 3.11/3.12/3.13, and a blocking
  PostgreSQL/Redis service job on `ubuntu-24.04`. Remove `continue-on-error` from strict type checking.
- Pin PostgreSQL 15 and Redis 7 service images by reviewed multi-architecture digest in both Compose and
  CI so local and automated evidence use the same immutable image identities.
- Add a stable final required job that fails unless every blocking predecessor succeeds (including `vulture`
  and `complexipy`).
  Contract tests read the real workflow file, bind the `vulture` and `complexipy` jobs' commands to the
  verifier's gate definitions, and execute the real aggregator script under GitHub's Bash options for every
  non-success predecessor result, so the workflow cannot drift from the verifier or the fail-closed contract silently.
- Run the same blocking workflow on feature-branch pushes, with concurrency cancellation for stale runs,
  so immutable Linux evidence exists before the constitution permits pull-request preparation.
- Run the compatibility profile on GitHub's arm64 `macos-14` runner as an advisory job. Full Apple
  release evidence uses the same verifier with local Docker services because hosted macOS does not
  support the container topology used by the service profile.
- Coverage upload remains informational; inability to upload coverage does not turn tested code red or
  green and is not itself a required product gate.

### 5. Documentation and migration path

- Replace the open-ended Python badge and quick-start wording with the finite support matrix.
- Document uv as the reproducible path. A pip editable install may remain an explicitly unsupported
  convenience, but MUST NOT be presented as equivalent release evidence.
- Replace the sub-two-minute claim with the approved under-five-minute target excluding only initial
  service-image downloads; locked Python dependency installation remains measured.
- Explain that existing bare/asyncpg URLs are accepted temporarily and normalized, while new setups use
  the Psycopg 3 spelling. This is the compatibility path required by the constitution.

### 6. Test Quality: Working Fakes Over Mocks

- **Baseline inventory**: 22 test files importing `unittest.mock`, 301 mock constructors, 52
  interaction assertions, 862 collected tests (860 passed, 2 platform skips), pytest-cov `TOTAL 91%`
  with `branch = true` (combined statement and branch coverage: 4098 statements / 329 missed, 768
  branches / 93 partial).
- **Strategy**: real product code wherever it runs offline; real values for settings, models, SDK
  and HTTP responses; small working fakes only at external boundaries; every baseline test ID kept.
- **Redis decision**: use the pinned `fakeredis==2.38.0` development dependency instead of a
  hand-built Redis. Rationale recorded from the official documentation and PyPI metadata on
  2026-09-09: `FakeAsyncRedis` is a `redis.asyncio.Redis` subclass emulating Redis 7 semantics by
  default; the release supports redis-py 4.6 through 8.x (the lock resolves `redis==7.1.0`) and
  Python 3.10+ (the project supports 3.11–3.13); streams, consumer groups, pending entries,
  transactions with `WATCH`, and sorted sets are implemented. Known divergences observed while
  building the shared contract: `XTRIM` on a missing key creates an empty stream key in `fakeredis`
  but not in Redis, and approximate `XTRIM`/`XADD MAXLEN ~` trims exactly in `fakeredis` while Redis
  trims by radix-tree node. The contract therefore asserts only behavior both implementations share
  and the product never depends on either divergence. Lua scripting is not used.
- **Shared contract**: `tests/integration/test_redis_contract.py` is parametrized over `fakeredis`
  and, under `RUN_SERVICE_TESTS=1`, the real loopback Redis; the `redis-contract` gate in the
  `services` profile runs it in CI. `scripts/runtime_services.py` gained
  `validate_loopback_redis_url` so the suite refuses any non-loopback `REDIS_URL`.
- **Boundary fakes** (`tests/fakes/`): `FakeAlertChannel`, `FakeWebhookServer` transports,
  `FakeBaseClobClient`, `FakeEth`/`FakeAsyncWeb3`/`TransferLogIndex`, `FakeMetadataSync`, and the
  `wire_pipeline` assembler that builds the real pipeline over those boundaries.
- **Forbidden**: generic mock frameworks, `return_value`/`side_effect` ladders, response queues keyed
  by call order, `__getattr__` attribute trees, subclass-of-product fakes, casts or `# type: ignore`
  added for doubles, new skips or xfails, gate exclusions or suppressions.
- **Regression enforcement**: AST policy test over every file in `tests/` and the root
  `conftest.py`, with fixtures for aliases, attribute access, and benign look-alikes.
- **Product defect surfaced**: a faithful `eth.block_number` property (web3 exposes it as a
  property, not a method) showed `PolygonClient.health_check` calling
  `_execute_with_retry("block_number")`, which raises `TypeError` against real web3. The fix uses
  the real `get_block_number` RPC method; the previously mocked test could not detect this.
- **Quality gates**: Black, Ruff, strict mypy/Pyright, default-confidence Vulture, and Complexipy
  `<= 5` for functions and modules over the whole tree, unchanged.

## Phase Outputs

- [research.md](research.md): resolved driver, dependency, lock, CI, and safety decisions
- [data-model.md](data-model.md): non-persistent support, gate, result, and service-evidence concepts
- [contracts/runtime-verification.md](contracts/runtime-verification.md): executable command contract
- [contracts/test-quality.md](contracts/test-quality.md): working fakes over mocks contract
- [quickstart.md](quickstart.md): post-implementation clean-checkout validation sequence

## Post-Design Constitution Re-check

All pre-design gates still pass. Phase 1 introduces no trading behavior, real notification, persistent
schema change, undocumented compatibility break, or unjustified complexity. The disposable-database rule
strengthens safe-effects compliance, and the explicit split between foundation verification and later
monitoring E2E proof preserves slice ownership.

## Complexity Tracking

No constitution violation requires justification.
