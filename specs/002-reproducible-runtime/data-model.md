# Data Model: Reproducible Supported Runtime

This slice changes no persisted application schema. Its entities are configuration and verification
concepts consumed by contributor tooling, CI, tests, and documentation.

## Support Contract

Represents the repository-wide compatibility promise.

| Field | Type | Rule |
|---|---|---|
| `python_specifier` | string | Exactly `>=3.11,<3.14` in project metadata and lock state |
| `python_minors` | ordered tuple | Exactly `3.11`, `3.12`, `3.13` |
| `uv_specifier` | string | Exactly `>=0.11,<0.12`; CI uses one reviewed exact 0.11 release |
| `blocking_os` | string | Ubuntu 24.04 x86_64 Linux reference environment |
| `supported_os` | ordered tuple | Linux through its reference environment, and Apple Silicon macOS |
| `apple_automation` | enum | Advisory arm64 hosted job; full local release evidence |
| `database_scheme` | string | Canonical `postgresql+psycopg` |
| `postgres_major` | integer | 15 for the documented local/CI stack |
| `redis_major` | integer | 7 for the documented local/CI stack |
| `lock_authority` | string | Checked-in `uv.lock` |
| `service_images` | ordered tuple | PostgreSQL 15 and Redis 7 references pinned by matching multi-architecture digests in Compose and CI |

### Validation

- Every declared Python set MUST equal `python_minors`; open-ended or partial sets fail.
- The running uv version MUST satisfy `uv_specifier`, and the CI installer version MUST be exact.
- Ruff/mypy may target the lowest supported minor and MUST be documented as syntax/type baselines rather
  than treated as incomplete matrices.
- Linux documentation MUST name the reference environment and MUST NOT imply certification of every Linux
  distribution, libc, or architecture.
- Third-party workflow actions MUST use reviewed full commit SHAs, workflow permissions MUST be explicit
  and minimal, and local/CI service image digests MUST match.
- Documentation, environment examples, CI, and command contracts MUST name the canonical database scheme.
- A credential value is never part of diagnostic or serialized contract output.

## Database URL Input

Represents a user-provided `DATABASE_URL` before engine creation.

| Field | Type | Rule |
|---|---|---|
| `original_driver` | enum | `postgresql`, `postgresql+psycopg`, or legacy `postgresql+asyncpg` |
| `canonical_driver` | constant | `postgresql+psycopg` |
| `host` | string | Required for runtime; loopback required for destructive verification setup |
| `port` | integer | 1–65535; default 5432 when omitted |
| `database` | string | Non-empty |
| `username` | string or null | Preserved during normalization |
| `password` | secret or null | Preserved internally; never rendered in diagnostics |
| `query` | immutable mapping | Driver-neutral and portable values are preserved; incompatible driver-specific keys fail before engine creation |
| `deprecated` | boolean | True for bare or asyncpg spelling |

### State transition

```text
raw input → parsed → validated → canonicalized → sync or async engine
              └── invalid → actionable redacted error
```

Normalization changes only the driver name when query parameters are portable. A deprecated input emits one
warning that names the old and new schemes without rendering credentials. An incompatible driver-specific
parameter transitions to `invalid` with a redacted migration error before either engine is created.

## Verification Profile

An ordered collection of required gates.

| Profile | Gates | Service requirement |
|---|---|---|
| `static` | lock, support-contract, format, lint, strict-types | None |
| `compatibility` | lock, dependency/import smoke, deterministic tests | None |
| `services` | `services` probe gate, then independent `migrations` gate | Loopback PostgreSQL and Redis |
| `all` | static + compatibility + services | Loopback PostgreSQL and Redis |

Profiles do not weaken their constituent gates. A gate appears once per invocation even when selected by
multiple composition paths.

## Gate Definition

| Field | Type | Rule |
|---|---|---|
| `id` | stable enum | `lock`, `support-contract`, `format`, `lint`, `strict-types`, `imports`, `tests`, `services`, `migrations` |
| `command` | argument tuple | Executed without an interpolating shell |
| `needs_services` | boolean | True only for service or migration gates |
| `redaction_policy` | enum | No URL, URL-with-hidden-password, or no sensitive input |

Gate order is deterministic. Required gates cannot carry an ignore-failure attribute. The `services` and
`migrations` identifiers are distinct even though the contributor helper can compose both in one direct
invocation.

## Gate Result

| Field | Type | Rule |
|---|---|---|
| `gate_id` | Gate identifier | Required |
| `status` | enum | `passed`, `failed`, `not-run` |
| `exit_code` | integer or null | Zero only when passed; null only when not run |
| `duration_seconds` | non-negative decimal | Monotonic elapsed time |
| `summary` | string | Names gate and disposition; contains no secret |

### Aggregate transition

```text
pending → running → passed
                  └→ failed → remaining gates not-run → aggregate nonzero
```

## Service Evidence

Ephemeral proof emitted by the `services` profile.

| Field | Type | Rule |
|---|---|---|
| `postgres_reachable` | boolean | True only after a live query succeeds |
| `redis_reachable` | boolean | True only after `PING` returns `PONG` |
| `temporary_database` | redacted label | Generated name without host/user/password |
| `migration_states` | ordered tuple | `head`, `head-minus-one`, `head` |
| `async_query_succeeded` | boolean | True only through the canonical async engine |
| `cleanup_succeeded` | boolean | Temporary database removed or absence confirmed |

Service evidence is diagnostic output, not a persisted product record. Cleanup failure is a failed gate.
