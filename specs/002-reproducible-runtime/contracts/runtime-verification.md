# Contract: Runtime Verification Entry Point

## Command

```text
uv run python scripts/verify.py --profile PROFILE [--json]
```

`PROFILE` is required and is one of `static`, `compatibility`, `services`, or `all`.

## Profiles

| Profile | Required gates | Intended caller |
|---|---|---|
| `static` | lock freshness, Black formatting, Ruff lint/import rules, strict mypy, strict Pyright, Vulture dead code detection | Contributor and Linux quality job |
| `compatibility` | lock freshness, dependency/import smoke, full deterministic pytest suite | Linux version matrix and advisory Apple job |
| `services` | `services` connectivity/async-query gate, then independent `migrations` disposable-cycle gate | Linux service job and local release evidence |
| `all` | Every gate above, without duplicate execution | Contributor pre-review/release evidence |

Individual Black, Ruff, mypy, Pyright, Vulture, pytest, and Alembic commands remain directly runnable and are
listed by `--help`; the aggregate entry point does not hide their output.

The independent `pyright` gate runs after `strict-types` (mypy) and before `vulture`:

```text
uv run --isolated --locked --all-extras --python 3.11 pyright src/polymarket_insider_tracker
```

`pyproject.toml` configures Pyright `1.1.411` in strict mode for Python 3.11 and includes the complete
`src/polymarket_insider_tracker` production package. The command repeats that package path so its scope
is visible and fail-closed at invocation. There is no exclusion, baseline, diff-only mode, diagnostic
downgrade, or error-suppression setting. `typings/` owns narrow local interfaces for only the untyped
third-party APIs the production package consumes; it is not a substitute for checking first-party files.

The independent `vulture` gate runs after `pyright` and before runtime imports:

```text
uv run --isolated --locked --all-extras --python 3.11 vulture src tests scripts alembic conftest.py
```

`pyproject.toml` pins Vulture `2.16` and names the `src`, `tests`, `scripts`, `alembic`, and `conftest.py` scope; the command repeats
those paths so its scope is visible and fail-closed at invocation. Vulture runs at its default confidence with
no baseline, allowlist, `ignore_names`, `ignore_decorators`, path exclusion, inline suppression, or
minimum-confidence setting. Names that frameworks consume by convention (Pydantic `model_config`,
`unittest.mock` `side_effect`, autouse fixtures) are made visible through real code and tests, not exempted.
The independent CI `vulture` job runs this exact command and is bound to it by contract tests.

The aggregate `tests` gate removes application and service configuration inherited from a loaded `.env`
before starting pytest. The test harness also runs from an isolated temporary working directory so Pydantic
cannot implicitly rediscover the repository `.env`. Together these boundaries keep the compatibility suite
deterministic and prevent unit tests from contacting configured live endpoints. The separately identified
`services` and `migrations` gates inherit the loaded service configuration and provide the real integration
proof.

## Inputs

| Input | Required | Contract |
|---|---|---|
| `--profile` | Yes | Exact enum; unknown values fail before running a gate |
| `--json` | No | Emit one JSON object to stdout; human diagnostics otherwise |
| `DATABASE_URL` | Services/all only | Canonical or supported legacy PostgreSQL URL whose host resolves to loopback |
| `REDIS_URL` | Services/all only | Redis URL reachable from the current host |

No failure-suppression, skip-required-gate, or production failure-injection flag exists.

## Runtime Service Helper

```text
uv run python scripts/runtime_services.py [--phase probe|migrations|all] [--json]
```

The default phase is `all`. `probe` checks PostgreSQL through the canonical async engine and checks Redis
with `PING`; `migrations` performs only the disposable database cycle. The aggregate verifier invokes the
two phases separately so `services` and `migrations` retain independent gate identity and failure evidence.

## Exit Status

| Code | Meaning |
|---|---|
| `0` | Every gate selected by the profile passed |
| `1` | A selected gate ran and failed |
| `2` | Invocation or prerequisite error; no destructive step began |

The first failed gate stops the aggregate run. Remaining gates are reported `not-run`; a failure is never
converted to success. Cleanup after a started migration cycle runs regardless of the primary result, and a
cleanup failure makes the aggregate fail.

## Human Output

Each gate emits:

```text
[RUN ] <gate-id>: <command description>
[PASS] <gate-id> (<duration>s)
```

or:

```text
[FAIL] <gate-id> (exit <code>, <duration>s): <actionable redacted summary>
```

The exact subprocess command and its normal stdout/stderr remain visible. Credential-bearing URLs are
rendered with the password hidden, and raw environment dictionaries are never printed.

## JSON Output

When `--json` is present, stdout contains exactly one object:

```json
{
  "profile": "static",
  "status": "passed",
  "exit_code": 0,
  "duration_seconds": 12.34,
  "gates": [
    {
      "id": "lock",
      "status": "passed",
      "exit_code": 0,
      "duration_seconds": 0.12,
      "summary": "lock is current"
    }
  ]
}
```

Diagnostics that would invalidate JSON are sent to stderr. Field ordering is stable for reproducible test
snapshots, but consumers MUST use field names rather than ordering.

## Configuration Ownership

Runtime support has no cross-file checker. `pyproject.toml` owns the supported Python and uv ranges plus
the strict Pyright target/scope,
`uv.lock` owns the resolved dependency set, CI owns the executable platform matrix, and `README.md` is
contributor guidance. The lock, compatibility, and service gates validate those declarations through the
tools that consume them. Repository code MUST NOT parse these files or prose to create a second authority.

Legacy asyncpg URLs are accepted only when their query parameters are portable to Psycopg. An incompatible
driver-specific option fails before engine creation and names the offending key without printing the URL or
any credential value.

## Service and Migration Safety

Before any migration cycle, the service helper parses `DATABASE_URL` and proves the host is loopback. It then:

1. connects to the configured local PostgreSQL server;
2. creates a unique empty sibling database with a generated `pit_verify_` name;
3. passes a redacted canonical URL for that database to Alembic;
4. upgrades to head and verifies the current revision;
5. downgrades exactly one revision and verifies the previous revision;
6. re-upgrades to head and verifies it;
7. executes a query through SQLAlchemy's async engine;
8. terminates remaining connections and drops the temporary database in guaranteed cleanup.

The configured application database is never downgraded or dropped. A non-loopback host, insufficient
database privilege, mismatched revision, Redis failure, or cleanup failure fails the gate with an
actionable redacted message.

## Gate-Failure Proof

Unit tests replace the subprocess runner with a deterministic fake. For every required gate, a table row
forces that gate to return nonzero and asserts:

- the aggregate exit is nonzero;
- the failed gate is named;
- no later gate runs;
- no secret appears in human or JSON output.

Integration tests—not fakes—prove PostgreSQL/Redis connectivity and the disposable migration lifecycle.
