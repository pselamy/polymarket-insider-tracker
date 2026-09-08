# Research: Reproducible Supported Runtime

**Date**: 2026-09-06

**Scope**: Resolve all implementation choices needed by slice 002 without changing monitoring behavior.

## Evidence at the Audit Anchor

- `pyproject.toml` advertises Python `>=3.11`; CI checks only 3.11.
- The uv lock contains SQLAlchemy but does not install `greenlet` on Apple Silicon.
- Clean isolated Apple Silicon probes on Python 3.11, 3.12, and 3.13 each collected 23 targeted tests,
  passed five, and produced 18 async storage errors because `greenlet` was absent.
- Alembic receives bare `postgresql://...` but no PostgreSQL DBAPI is declared.
- `mypy src` reports five errors in two files; the CI type job is allowed to fail.
- `ruff format --check src tests` identifies one unformatted test file.
- Existing tests use SQLite or fakes, so the CI PostgreSQL and Redis services do not prove a real path.

## Decision 1: Support exactly Python 3.11–3.13

**Decision**: Declare `requires-python = ">=3.11,<3.14"`, regenerate one universal uv lock, run blocking
Linux compatibility jobs on 3.11/3.12/3.13, and produce Apple Silicon evidence with the same verifier.

**Rationale**: This is the approved finite boundary. The lower bound preserves the current code contract;
the upper bound prevents metadata from silently promising unverified future interpreters.

**Alternatives considered**:

- Keep `>=3.11`: rejected because it promises every future Python minor without evidence.
- Support only 3.11: rejected because all three approved interpreters are locally available and later
  dependencies already publish compatible artifacts; narrowing the approved scope would be dishonest.
- Add 3.14 now: rejected because it is an explicit approved deferral and has not passed the complete gate.

## Decision 2: Use the uv lock as the installation authority

**Decision**: CI and documented verification use `uv sync --locked --all-extras` and `uv run`; a lock
freshness gate rejects dependency metadata changes that were not resolved into `uv.lock`. Project metadata
requires uv `>=0.11,<0.12`, and CI installs a reviewed exact uv 0.11 release.

**Rationale**: The repository already tracks uv's cross-platform lock. Current uv guidance recommends
locked synchronization in GitHub Actions, so pip must not independently choose a second dependency graph.

**Alternatives considered**:

- Continue `pip install -e ".[dev]"` in CI: rejected because it ignores the tracked lock.
- Maintain separate requirements lock files: rejected as duplicate dependency authority.

**Source**: [Using uv in GitHub Actions](https://docs.astral.sh/uv/guides/integration/github/)

**Source**: [uv `required-version` setting](https://docs.astral.sh/uv/reference/settings/)

## Decision 3: Require SQLAlchemy's asyncio extra

**Decision**: Declare `sqlalchemy[asyncio]` rather than relying on SQLAlchemy's platform-conditional base
installation.

**Rationale**: SQLAlchemy documents `greenlet` as required by the asyncio extension and prescribes the
`asyncio` extra to require it across platforms, including Apple Silicon. The three clean matrix failures
reproduce exactly that missing contract.

**Alternatives considered**:

- Add `greenlet` directly: viable but rejected because the SQLAlchemy extra expresses why it is required
  and remains aligned with upstream dependency metadata.
- Avoid SQLAlchemy asyncio: rejected as an unrelated redesign of the storage layer.

**Source**: [SQLAlchemy asyncio platform installation notes](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#asyncio-platform-installation-notes-including-apple-m1)

## Decision 4: Standardize on Psycopg 3 for sync and async PostgreSQL

**Decision**: Add `psycopg[binary]` and use `postgresql+psycopg://...` as the canonical database URL.

**Rationale**: SQLAlchemy's Psycopg 3 dialect selects its synchronous implementation for
`create_engine()` and asynchronous implementation for `create_async_engine()` using the same dialect URL.
That gives Alembic and the application one declared driver and one configuration value.

**Alternatives considered**:

- Psycopg 2: rejected because the default bare dialect is synchronous only and caused the current missing
  driver failure.
- `asyncpg` plus a second synchronous driver/URL: rejected because it creates two configuration contracts
  and more dependencies.
- Rewrite Alembic to run an async environment: technically possible but adds lifecycle complexity while
  Psycopg 3 already supports both paths.

**Source**: [SQLAlchemy Psycopg 3 dialect](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg)

## Decision 5: Normalize legacy URLs with an explicit warning

**Decision**: Continue accepting bare `postgresql://` and `postgresql+asyncpg://` values, parse them with
SQLAlchemy's URL type, normalize only the driver name to `postgresql+psycopg`, and emit a redacted
deprecation warning. Preserve driver-neutral components and portable query parameters. Reject incompatible
driver-specific query options before engine creation with an actionable redacted migration error. New
examples use the canonical value.

**Rationale**: The constitution forbids an undocumented compatibility break. Normalization preserves
host, port, database, username, password, and query parameters while making the runtime executable.

**Alternatives considered**:

- Reject both legacy spellings immediately: rejected because existing public configuration would break.
- Silently rewrite: rejected because users could reasonably believe their requested driver is active.
- Pass every asyncpg-specific query option through to Psycopg: rejected because syntactic preservation can
  produce a later opaque driver failure rather than a safe compatibility path.
- Declare separate migration and application variables: rejected because a single portable URL works.

## Decision 6: Use a typed Python verifier

**Decision**: Add a small standard-library orchestrator in `scripts/verify.py` and keep individual tools
directly runnable. Profiles compose gates without hiding their commands or output.

**Rationale**: A Python verifier works on both supported operating systems, can be unit-tested with an
injected command runner, and gives contributors and CI one aggregate contract.

**Alternatives considered**:

- Makefile: rejected because task failure injection and structured results become shell-specific.
- Shell script: rejected because portable error handling/redaction is harder to test.
- New task-runner dependency: rejected as unnecessary dependency surface.

## Decision 7: Exercise migrations in a disposable loopback database

**Decision**: The service profile refuses non-loopback database hosts, creates a uniquely named sibling
database, runs upgrade → one-step downgrade → re-upgrade → current checks, and drops it in guaranteed
cleanup. It never downgrades the configured application database.

**Rationale**: The required downgrade proof is destructive because the latest migration drops a table.
Running it against the contributor's normal database would violate safe-effects discipline.

**Alternatives considered**:

- Downgrade the configured database: rejected because it can destroy local research data.
- Test only offline SQL generation: rejected because it does not prove the driver or live PostgreSQL DDL.
- SQLite migration smoke: rejected because the production migrations contain PostgreSQL-specific behavior.

## Decision 8: Separate required Linux evidence from advisory Apple automation

**Decision**: Linux static, compatibility, and service jobs are blocking. An arm64 `macos-14` compatibility
job is advisory; full Apple release evidence is captured locally with Docker Desktop and the same verifier.

**Rationale**: GitHub currently lists `macos-14` as an arm64 hosted runner, but hosted macOS cannot run the
same Docker service topology. Advisory automation catches dependency/import/test drift; the local service
profile proves the remaining Apple path.

**Alternatives considered**:

- Treat Apple as documentation-only: rejected because it is an approved supported platform.
- Make the incomplete hosted Apple profile blocking evidence for all Apple claims: rejected because that
  would overstate service coverage.
- Add a self-hosted runner now: rejected because runner operations are outside this repository slice.

**Source**: [GitHub-hosted runners reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)

## Decision 9: Preserve exact gate semantics in tests and CI

**Decision**: Table-driven verifier tests force each gate to fail and require a nonzero aggregate result;
the support-contract test also rejects `continue-on-error` on required jobs and version/command drift.

**Rationale**: A green workflow file is not proof that a required command can block. Failure-path tests
make gate propagation deterministic without committing deliberately broken source.

**Alternatives considered**:

- Manual failure injection only: rejected because it is not repeatable per pull request.
- Trust shell step defaults: rejected because the baseline already contains an explicit ignored failure.

## Decision 10: Make CI and service substrates reviewably immutable

**Decision**: Give the workflow only `contents: read`, pin third-party actions to reviewed full commit SHAs
with release comments, and pin the PostgreSQL 15 and Redis 7 images to reviewed multi-architecture digests
in both `docker-compose.yml` and CI.

**Rationale**: A locked Python graph is insufficient if action code or service images can change under the
same reference. Full action SHAs and container digests make the exact verification substrate auditable;
matching references across local Compose and CI avoids two nominally identical but materially different
service stacks.

**Alternatives considered**:

- Floating action major tags: rejected because they can move without a repository diff.
- Mutable image tags alone: rejected because a tag can resolve to different bytes across runs.
- Different local and CI images: rejected because it weakens reproducibility and failure diagnosis.

**Sources**: [GitHub secure-use guidance](https://docs.github.com/en/code-security/tutorials/secure-your-organization/protect-against-threats),
[Docker Compose trust model](https://docs.docker.com/compose/trust-model/)

## Decision 11: Use one explicit Linux reference environment

**Decision**: The repository supports Linux through blocking evidence on Ubuntu 24.04 x86_64. It does not
claim that every distribution, libc, or Linux architecture has separate certification.

**Rationale**: “Linux” is otherwise broader than any finite automation matrix. Naming the reference runner
makes the approved platform promise testable while retaining a useful Linux compatibility contract.

**Alternatives considered**:

- Claim only Ubuntu: rejected because the approved public scope is Linux and the Python package has no
  intentional Ubuntu-specific behavior.
- Claim all Linux variants: rejected because that cannot be substantiated by this repository's evidence.

## Decision 12: Produce remote Linux evidence before pull-request preparation

**Decision**: Run the blocking workflow on feature-branch pushes as well as `main` and pull requests, and
cancel superseded in-progress runs for the same workflow/ref.

**Rationale**: The current default-branch and pull-request-only triggers cannot produce branch evidence
before the constitution's pull-request gate. A branch-push trigger removes that circular dependency;
concurrency cancellation bounds redundant work.

**Alternatives considered**:

- Open a draft pull request solely to start CI: rejected because the constitution allows pull-request
  preparation only after required checks and convergence pass.
- Rely only on local Apple evidence: rejected because the approved plan requires immutable blocking Linux
  workflow evidence.

## Resolved Unknowns

All Phase 0 technical unknowns and post-analysis remediations are resolved. No `NEEDS CLARIFICATION` item remains.
