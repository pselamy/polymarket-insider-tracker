# Quickstart Validation: Reproducible Supported Runtime

This is the target validation sequence for slice 002. Commands validate the implemented reproducible runtime.

## Prerequisites

- Git
- uv
- Python 3.11, 3.12, or 3.13
- Docker Desktop on Apple Silicon macOS, or Docker Engine with Compose on Linux
- A clean checkout with no pre-existing `.venv` or `.env`

Initial container-image download time is excluded from the five-minute success target. Locked Python
dependency installation remains inside the measured foundation path.

## Clean Foundation Path

From the repository root:

```bash
uv sync --locked --all-extras --python 3.13
cp .env.example .env
docker compose up -d --wait postgres redis
uv run --env-file .env python scripts/verify.py --profile all
```

Expected result:

- installation uses only the checked-in lock;
- PostgreSQL and Redis answer real probes;
- migrations reach head, downgrade one revision in a disposable database, return to head, and clean up;
- an asynchronous query succeeds through the same canonical database URL;
- lock, Black format, Ruff lint, strict mypy, strict Pyright, Vulture dead-code check, Complexipy cognitive complexity check, and deterministic test gates all pass;
- the aggregate process exits `0` without contacting external market, chain, or notification services.

The normal application database named in `.env` is never downgraded or dropped.

## Individual Profiles

```bash
uv run python scripts/verify.py --profile static
uv run python scripts/verify.py --profile compatibility
uv run --env-file .env python scripts/verify.py --profile services
uv run --env-file .env python scripts/verify.py --profile all --json
```

The `services` and `all` profiles read `DATABASE_URL` and `REDIS_URL`; `--env-file .env` loads them
from the copied example configuration. The `static` and `compatibility` profiles need no service
configuration. `uv run python scripts/verify.py --help` lists the directly runnable command behind
every gate.

The directly runnable Complexipy contract is:

```bash
uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore --ignore-complexity=false --snapshot-ignore=true --snapshot-create=false --exclude=. --check-script=true
```

It checks function and module-level control flow and explicitly neutralizes inline ignores,
report-only success, automatic snapshots, snapshot creation, cwd-config exclusions, and cwd diff modes.

See [contracts/runtime-verification.md](contracts/runtime-verification.md) for exact gate membership,
output, exit status, and migration safety behavior.

## Compatibility Matrix

Run the non-service compatibility evidence in a clean isolated environment for each supported minor:

```bash
uv run --isolated --locked --all-extras --python 3.11 python scripts/verify.py --profile compatibility
uv run --isolated --locked --all-extras --python 3.12 python scripts/verify.py --profile compatibility
uv run --isolated --locked --all-extras --python 3.13 python scripts/verify.py --profile compatibility
```

On Apple Silicon, record `uname -m` with release evidence and require `arm64`. The repository's advisory
`macos-14` job must use the same compatibility profile. Full Apple release evidence additionally runs the
`services` profile locally with Docker Desktop.

## Expected Failure Examples

- Python 3.10 or 3.14+: `uv sync --locked` rejects the project version boundary.
- Stale `uv.lock`: the lock gate exits nonzero and names the lock mismatch.
- Bare or asyncpg database URL: accepted temporarily, normalized to Psycopg, and reported with a redacted
  deprecation warning.
- Asyncpg URL with an incompatible driver-specific query option: rejected before engine creation with the
  offending key and a redacted migration instruction.
- Missing `DATABASE_URL` or `REDIS_URL` for `services`: exit `2` naming the missing variable before any
  service is contacted.
- Non-loopback or malformed database URL for `services`: exit `2` before creating or migrating a database.
- Missing PostgreSQL/Redis: exit `1`, name the unreachable service, and print no credential-bearing URL.
- Any Black, Ruff, mypy, Pyright, Vulture, Complexipy, pytest, service, or migration failure: aggregate exit is nonzero;
  later gates are not run.

## Cleanup

After validation:

```bash
docker compose down
```

This stops containers without removing the contributor's normal volumes. The verifier separately proves
that its generated migration-test database was removed.
