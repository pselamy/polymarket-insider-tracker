# Feature Specification: Reproducible Supported Runtime

**Feature Branch**: `codex/spec-kit-brownfield-adoption`

**Created**: 2026-09-06

**Status**: Scope and recommended implementation plan approved by Patrick on 2026-09-07

**Input**: User description: "Make clean-checkout setup, database migrations, supported Python versions, and all required quality gates reproducible and honest."

## Clarifications

### Session 2026-09-06

- Q: Which runtime and platform support boundary should this slice make enforceable? → A: Python
  3.11–3.13 on Linux and Apple Silicon macOS, with a blocking Linux matrix, equivalent Apple Silicon
  release evidence, and advisory or scheduled Apple automation when a suitable runner exists.
- Q: What finite evidence boundary makes the Linux promise reviewable? → A: Linux compatibility is
  verified on Ubuntu 24.04 x86_64 as the blocking reference environment; the project does not claim
  separate certification of every distribution, libc, or Linux architecture.
- Q: How should legacy asyncpg URLs with driver-specific query options migrate to Psycopg? → A: Preserve
  driver-neutral URL components and portable query parameters; reject incompatible driver-specific
  parameters before engine creation with an actionable, credential-redacted migration error.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Reach a Ready Local Installation (Priority: P1)

As a new contributor, I can follow one documented path from a clean checkout to an installed tracker
whose local database, cache, migrations, and configuration validation all work on a supported machine.

**Why this priority**: The advertised monitoring workflow is not usable when the documented install
omits required runtime components or gives the application and migration tool incompatible settings.

**Independent Test**: On a clean supported Linux or Apple Silicon macOS environment, follow only the
tracked setup instructions and verify dependency installation, service startup, migration, downgrade
and re-upgrade, and readiness validation without an undeclared package or hand edit.

**Acceptance Scenarios**:

1. **Given** a clean checkout and the documented prerequisites, **When** the contributor runs the
   documented install, **Then** all application, asynchronous database, migration, and test dependencies
   are present on every supported platform.
2. **Given** the example local service configuration, **When** the contributor applies all migrations,
   **Then** the database reaches the current schema without changing the supplied database setting.
3. **Given** the current schema, **When** the contributor performs the documented migration smoke cycle,
   **Then** the latest revision can downgrade and re-upgrade without data-definition errors.

---

### User Story 2 - Trust Required Checks (Priority: P2)

As a maintainer, I can rely on pull-request checks to fail for formatting, lint, type, test, migration,
or supported-version regressions instead of receiving a misleading green result.

**Why this priority**: A non-blocking or incomplete gate transfers defects from automation to users.

**Independent Test**: Run each documented gate from a clean environment, then introduce one controlled
failure per gate and verify that the aggregate check reports failure.

**Acceptance Scenarios**:

1. **Given** the proposed branch, **When** all required local checks run, **Then** format, lint, strict
   mypy, strict Pyright, Vulture dead-code verification, tests, service probes, and migrations all pass with no ignored required failure.
2. **Given** a deliberate mypy or Pyright type error, dead code finding, formatting error, test failure, or migration
   failure, **When** the corresponding automated check runs, **Then** the pull request is blocked.
3. **Given** any supported Python version, **When** the compatibility suite runs, **Then** installation
   and the required test subset pass on that version.

---

### User Story 3 - Understand the Support Boundary (Priority: P3)

As an operator or contributor, I can tell which language versions, operating systems, services, and
commands are supported, and I receive an actionable failure when I am outside that boundary.

**Why this priority**: An unbounded version declaration is a promise; leaving it untested makes setup
failures unpredictable.

**Independent Test**: Compare project metadata, the lockfile, local instructions, example configuration,
and automated checks; verify that they name one consistent support matrix and reject unsupported use clearly.

**Acceptance Scenarios**:

1. **Given** project metadata, documentation, dependency resolution, and automation, **When** their
   supported versions are compared, **Then** the lower and upper bounds agree exactly.
2. **Given** an unsupported language version, **When** installation is attempted, **Then** resolution
   stops with a clear version incompatibility rather than a later runtime failure.
3. **Given** a clean example environment, **When** configuration is loaded, **Then** secrets are not
   committed or printed and all required values are explained.

### Edge Cases

- Apple Silicon may not receive optional asynchronous database dependencies unless they are explicit.
- A database connection string can be syntactically PostgreSQL yet select a driver unsuitable for one
  of the application or migration paths.
- A legacy asyncpg URL can contain a driver-specific query option that Psycopg cannot interpret safely.
- A lockfile can resolve successfully on one interpreter while metadata claims additional versions.
- Service containers may be healthy before the schema is current, or the schema may be current while
  application connectivity is broken.
- A check can appear green because its step is allowed to fail or because the real service is never used.
- Downloading container images can dominate first-run wall time and is not controlled by the project.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The repository MUST declare one explicit, finite set of supported Python minor versions,
  and project metadata, dependency resolution, documentation, and automated checks MUST agree.
- **FR-002**: The supported set for this slice MUST be Python 3.11, 3.12, and 3.13 inclusive; versions
  outside that set MUST NOT be advertised as supported.
- **FR-003**: A clean installation MUST include every dependency required by asynchronous database use,
  PostgreSQL runtime access, migrations, tests, linting, formatting, and both strict type checkers on Linux
  and Apple Silicon macOS.
- **FR-004**: One documented database configuration value MUST be usable by both the running tracker and
  migration workflow, or the documentation MUST expose and validate separate values explicitly. An
  implicit driver mismatch is prohibited. Legacy URL migration MUST preserve driver-neutral components
  and portable parameters while rejecting incompatible driver-specific parameters before engine creation
  with an actionable redacted error.
- **FR-005**: The example environment MUST contain working local defaults for the documented service stack,
  identify truly required values, and contain no real credential.
- **FR-006**: The foundation quick start MUST include dependency installation, local service startup,
  migration, and bounded local configuration/service validation in the order a clean checkout requires.
  End-to-end dry-run startup belongs to slices 001 and 003 and MUST NOT be claimed by this foundation slice.
- **FR-007**: Every migration MUST retain a downgrade path, and automation MUST exercise upgrade to head,
  one-step downgrade, and re-upgrade using the supported PostgreSQL driver.
- **FR-008**: Required format, lint, strict mypy, strict Pyright, Vulture dead-code check, test, migration, and compatibility checks MUST be blocking.
  Any approved exception MUST be explicit, time-bounded, owned, and visible in the gap register.
- **FR-009**: Automated tests MUST use the declared locked dependency set rather than resolving an
  unrelated environment on each run.
- **FR-010**: At least one required check MUST exercise the real local PostgreSQL and Redis services used
  by the quick start; starting unused services does not satisfy this requirement.
- **FR-011**: The repository MUST provide one documented aggregate verification command or entry point
  that runs the same required gates contributors are expected to satisfy before review.
- **FR-012**: Installation and gate output MUST identify the failing prerequisite or command without
  exposing credential-bearing URLs.
- **FR-013**: All current formatting, strict mypy, and strict Pyright failures at the audit anchor MUST be resolved; they
  MUST NOT be hidden through exclusions, blanket ignores, or non-blocking status.
- **FR-014**: README, contributor commands, example configuration, automation, and project metadata MUST
  use the same setup and support terminology.
- **FR-015**: Pyright MUST remain an independent, exactly pinned and locked strict checker of the complete
  `src/polymarket_insider_tracker` package at the Python 3.11 compatibility floor. It MUST NOT use a
  baseline, diff-only mode, blanket suppression, broad exclusion, or downgraded diagnostics.
- **FR-016**: Vulture MUST remain an independent, exactly pinned and locked dead-code checker across `src`,
  `tests`, `scripts`, `alembic`, and `conftest.py` (every tracked repository Python file) at the Python 3.11
  compatibility floor, running at Vulture's default confidence. It
  MUST NOT use a baseline, whitelist/allowlist file, `ignore_names`, `ignore_decorators`, path exclusion,
  inline suppression, minimum-confidence threshold, or non-blocking status. Names that frameworks consume by
  convention MUST be made visible through real code and tests rather than exempted.

### Key Entities

- **Support Matrix**: The finite set of supported Python minors and host operating-system families, with
  the required verification performed for each.
- **Runtime Environment**: The resolved application, migration, and development dependencies associated
  with a supported Python version and lock state.
- **Local Service Stack**: The documented PostgreSQL and Redis instances, their connection settings,
  readiness state, and schema revision.
- **Required Gate**: A named reproducible check with a pass/fail result that contributes to merge readiness.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A clean supported Linux checkout and a clean Apple Silicon macOS checkout install without
  undeclared packages and complete the database test suite with zero dependency-related errors.
- **SC-002**: The documented migration smoke cycle succeeds against PostgreSQL: upgrade to head, downgrade
  one revision, and re-upgrade to head, all using tracked configuration.
- **SC-003**: Formatting, lint, strict mypy, strict Pyright, Vulture dead-code verification, and the full deterministic test suite each report
  zero failures from a clean locked environment.
- **SC-004**: Installation and the required compatibility test subset pass on 100% of Python 3.11, 3.12,
  and 3.13 jobs.
- **SC-005**: Injecting one controlled failure into each required gate causes that gate and the aggregate
  verification result to fail in every case.
- **SC-006**: After prerequisites and service images are available, a contributor can reach successful
  local configuration, database, cache, and migration validation from a clean checkout in under 5 minutes
  using only tracked instructions.
- **SC-007**: Runtime support is declared in native project metadata and exercised by locked installation,
  the complete Python compatibility matrix, and real service verification. No bespoke checker reparses
  source files or README prose as a second configuration authority.

## Assumptions

- Python 3.11 through 3.13 inclusive is the approved support window. Python 3.14 and later are excluded
  until their complete dependency and test matrix is verified; Python 3.10 and earlier remain unsupported.
- Linux and Apple Silicon macOS are the supported contributor platforms for this slice. Linux runs the
  blocking 3.11/3.12/3.13 compatibility matrix on the Ubuntu 24.04 x86_64 reference environment; this does
  not imply separate certification of every distribution, libc, or Linux architecture. Apple Silicon runs
  the same repository verification entry point as release evidence and SHOULD have an advisory or scheduled
  automated job when a suitable runner is available; Windows-specific support is not currently promised.
- The local quick start uses containerized PostgreSQL and Redis, while the application itself runs on
  the host. Initial container image download time is excluded from the five-minute target.
- One canonical database setting is preferred because the current public contract exposes one. The plan
  may choose separate validated settings only if one portable value cannot serve both workflows safely.
- A real external Polymarket, Polygon, Discord, or Telegram dependency is not required for deterministic
  quality gates; those checks belong to bounded live-safe verification or explicitly authorized release work.
- This slice executes first even though its numeric feature prefix is `002`, because every later slice
  depends on a reproducible database, dependency set, and blocking local/CI gates.
