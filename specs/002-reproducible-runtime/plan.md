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

## Root Agent Guidance Follow-up Plan

This documentation-only follow-up is a separate PR from the mocks-to-fakes implementation.

1. Record the requested guidance contract and trace it to the constitution, current runtime gates,
   and Patrick's explicit quality/test-double instructions. No new governance is introduced.
2. Add a concise root `AGENTS.md` with relative links, an explicitly activated slice example, the
   canonical verifier commands, and the exact fail-closed Complexipy command. Preserve the managed
   `.agents/skills/speckit-*` integration; do not recreate `.claude/skills` or copy global skills.
3. Check all local links and command examples against actual tracked scripts/configuration. Review
   for misleading statements about type-checker scope, advisory CI, fake coverage, or completed work.
4. Run the existing static and compatibility profiles. No runtime change is intended; preserve the
   service and migration contracts unchanged. Record exact evidence and limitations separately.
5. Reconcile with the completed fakes PR before publishing this subsequent PR. Keep human-owned
   reviewer criteria unchecked and obtain Patrick's approval before merging.

No new dependencies, scripts, application behavior, CI gates, or schema changes are needed.

### T5 R1/R2/R3 corrective binding (2026-09-12)

The bounded corrective lane authorizes validator-owned real pytest receipts,
independent checkout/revision/input binding, and immutable Git history comparison.
See `contracts/traceability.md`. Preserve old JSON and prose evidence unchanged;
append fresh receipts externally after committing the candidate to avoid circular
self-head manifests. Regression evidence starts with the actual independent
`probe.py` (all seven unsupported claims accepted on the supplied baseline), then
real passing/skipped/missing/failing executions and individually targeted document
mutations. Native quality/compatibility/service gates remain unchanged.


## Native secret-policy preparation — 2026-10-16 (Phase0)

Branch: `codex/polymarket-secrets-reconciliation`, baseline523b0a35466e799c36e85e3bc5ea046cf3263c31.
Original plan and delivered foundation remain intact. This appendix implements story5,
FR-023–FR-030 and SC-011–SC-015 under the direct approved direction. No actual finding is accepted.

### Technical context and constitution check

- Language/runtime: existing locked Python3.11–3.13; standard-library parser/identity/source adapters
  where sufficient; current native formatting, lint, independent types and complexity gates retained.
- Native scanner: Gitleaks8.30.1, protected qualified configuration, findings-only exit42, full tree
  and retained history. Five real disposable CLI controls distinguish clean0/findings42/fatal1.
- Native approval: adapt the pinned reference's real protected-base/App/native-review machinery,
  rather than accept candidate approval files. Exact human principal/proposal lifecycle, report
  execution binding and bootstrap require Phase0 source research before Phase1 design.
- No trading, provider calls, notifications, access changes, App provisioning, history rewrite,
  real finding acceptance or merge is authorized by this preparation.
- The existing no-baseline/allowlist rule remains active. The draft amendment will show the exact
  exception for trusted post-scan false positives. Direction approval authorizes preparing code and
  text for review; it is not ratification of an arbitrary amendment or individual disposition.
- Required checks remain blocking. The future policy defines a valid post-scan result; it does
  not turn arbitrary failed checks into success or grant exclusions to other quality controls.

### Phase0 research dispatched

1. Trusted boundary: protected-base source, native human identity/review, proposal/merged-policy
   lifecycle, dismissal/supersession, revocation reevaluation, worker/App isolation and honest bootstrap.
2. Native boundary: complete report schema, all native coordinates, duplicate fingerprints, exact
   source blobs and report/execution/exit binding. Existing adjudication is evidence, not a registry.
3. PR129 reconciliation: preserve useful negative/redaction obligations, replace incomplete/custom
   fallback and detector allowlists, retain historical main IDs/evidence and original donor refs.

Research is recorded in `research.md` once consolidated. Phase1 data model/contracts/quickstart and
final tasks/analyze follow only after every research question has a concrete disposition. Unknown
live deployment values may remain unconfigured dependencies with fail-closed activation; an
unresolved security design cannot be relabeled as completed research.

### Research disposition and Phase1 design

Research notes are preserved in `evidence/security-policy/research/`. The native parser, exact
identity/worker boundary, protected approval lifecycle and activation conditions are specified in
`contracts/native-secret-policy.md`. The reference is reusable metadata/App machinery, not an
existing authenticated finding-disposition system. Do not transplant its matrix or trust claims.

Conservative design choices resolve policy latitude without granting additional acceptance:
tree decisions bind the exact head (no cross-head carry), approval is independent/non-contributor,
eligible human IDs default empty, native columns remain opaque positive metadata, unknown schema
fails, and unqualified merge methods/freshness mechanisms cannot activate nonempty acceptance.
Deployment identities and native merge-freshness proof are explicit later activation prerequisites,
not guessed values or a claimed completed trust service. The first increment has no approval path.

Implementation order:

1. A side-effect-free `scripts/__init__.py` gives new imports one strict-checker module identity;
   existing direct script entry points remain. Pure parser under `scripts/_secret_models.py`,
   `_secret_json.py`, `_secret_schema.py` and
   `_secret_report.py`, with `tests/tooling/test_secret_report.py` plus focused schema cases. New
   code uses frozen typed values, stdlib JSON bounds and constant errors. Arbitrary native identity
   text stays restricted and is omitted from repr/normal output. Its ordered result is
   untrusted metadata and cannot certify full scanning, source resolution or approve any row.
2. Independently bound source objects and native worker under `scripts/_secret_source.py` and
   `scripts/_secret_worker.py`, with exact-policy/artifact/scope receipts and real native controls.
   Preserve donor obligations and all old main test IDs. Do not register a partial passing gate.
3. Protected `scripts/secret_policy/` adapters and worker transport with separate credentials,
   real native review/merged-record/revocation controls and conclusive merge-freshness proof.
   Proposed governance text lives in `contracts/security-amendment-proposal.md`; ratified root
   guidance/constitution do not change merely because this preparation exists.
4. Complete remediation and gate/hosting enrollment, exact individual decisions, fresh traceability,
   native matrix/services and independent review, then separately approved merge/main validation.

Post-design constitution check: pure preparation preserves research-only/safe effects, compatibility,
all required native gates and governance. No baseline/allowlist is created. The proposed exception
is not active. Existing direction approval permits preparing code for review; no new acceptance or
access assumption is treated as approved. Full extension completion remains gated by SEC tasks.

Each new file stays <=399 physical lines, cognitive <=5 and cyclomatic <=10. Both strict mypy and
Pyright explicitly inspect new scripts in locked Python3.11, even though current Pyright's normal
repository gate covers src only. Preserve full native gates; focused checks are incremental evidence.
No added runtime dependency, network/product side effect, schema or public product API change.


## SEC005 detailed source-binding design — 2026-10-17

The direct approved preparation covers this dependency-ordered increment. See
`contracts/native-source-binding.md` for exact inputs, raw commit/tree/blob integrity, actual tree-byte
comparison, canonical identity and bounded native process behavior. Phase0 research and safe probe
summaries are preserved under `evidence/security-policy/research/source-binding/`. No public product
contract, supported platform or live acceptance changes. Native source/data types confer no authority.

Code remains in owned scripts: `_secret_process.py` isolates/caps native Git commands, `_secret_git.py`
verifies raw native object bodies, `_secret_tree.py` parses verified tree entries, `_secret_identity.py`
owns frozen declared scope/canonical identity, and `_secret_source.py` joins exact source with finding
metadata. Tests use `tests/tooling/secret_source_fixtures.py` for real disposable Git construction plus
`test_secret_source.py`, `test_secret_tree.py`, `test_secret_identity.py`, `test_secret_process.py` and
`test_secret_git.py`. No inherited env, custom fallback detector or arbitrary candidate script runs.

Git2.46+ no-lazy capability is a known additional worker dependency: unsupported executables fail
visibly. Local Apple Git2.50.1 controls do not qualify a release artifact or Ubuntu. Whole approved
OS/Python scope, protected worker and all full controls remain required; no platform is excluded.
The prior native LF-position bound preserves a possible terminal empty position (LF count+1), not
Git-only physical-line counting. Native columns remain opaque. Bounded actual stdout and per-resolution
budgets fail without partial identity; worker sandbox/provenance/completeness is not inferred.

Constitution check: research-only preparation, zero external effects; exact data/identity semantics,
no approval/disposition path, root governance unchanged; all native gates/compatibility remain.
Before code, run managed analysis against this refinement and add failing meaningful controls.
Independent exact-code adversarial review and committed-source verification precede SEC005 completion.


## SEC006 native-worker readiness research - 2026-10-17

SEC005 is verified at96b0faf; its doc checkpoint75d1332 closes that layer only. The managed Phase0
worker research and independent root measurements are preserved in `evidence/security-policy/research/worker/`.
See `contracts/native-worker-readiness.md` for the pinned artifact/config, fixed complete-history
format, immutable owned inputs and strict observation/authority boundaries. Extensions hooks are
absent; explicit slice002 setup/prerequisites selected the existing plan without replacing it.

A complete worker is not yet implementation-ready: unmodified scanner file opens can fail silently,
so exit/report/diagnostic checks plus before/after manifests alone do not establish native read
completion. Independent adversarial design analysis must resolve this before worker coding. No
byte-only helper will be substituted for the SEC006 milestone. Retain all supported platforms and
every original goal requirement. The proposed removal-only config and archive/executable measurements
confer no active policy, protected origin or signer/build provenance.
