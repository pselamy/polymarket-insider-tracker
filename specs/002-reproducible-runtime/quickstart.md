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
- PostgreSQL and Redis answer real probes, and the shared Redis behavioral contract passes against both `fakeredis` and the real loopback Redis;
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


## Native security preparation validation

The pure parser is an internal preparation increment, not a registered secrets gate. With the locked
development environment, run `uv run --isolated --locked --all-extras --python 3.11 python -m pytest
tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py` once those files exist.
Expected: native clean0/empty and findings42/nonempty metadata parse; all schema/exit/duplicate/
coordinate negatives fail with safe domain errors. Parsing findings is not approval. The untouched
real product reports still fail native scanning and no individual finding is approved.

Before calling the complete security extension usable, additionally execute and record:

1. Qualified pinned native artifact/config and immutable tree/all-retained-history commands on clean,
   tree-finding, removed/history-only, same-line collision and approved-one/unapproved-other controls.
2. Exact source/blob/head/policy changes, omitted refs/donor history, unreadable or excluded authored
   files, dirty snapshots, stale/missing/symlink/oversized reports, process fatal/timeout/swallowed exit,
   inline allows and Git-ignore defects. Each must fail; complete clean controls must pass.
3. Real protected transport/issuer controls: candidate artifact/policy forgery, Bot/non-independent
   human, wrong repository/head/base/run/App, merged-record provenance, review dismissal after green,
   role/policy drift and the actual native stale-approval-before-merge barrier. Fixture reevaluation
   alone is insufficient. Test credential isolation without exposing/placing a real token in a worker.
4. Exact governed amendment and individually authenticated dispositions, complete supported native
   quality/matrix/services and installed behavior, retained test IDs/fresh traceability, actual required
   hosting readback, independent exact-head review, separately approved merge and resulting-main CI.

Do not run App provisioning, access/credential changes, real notifications or external messages as a
quickstart side effect. Prepare those concrete changes for their applicable separate authorization.


## SEC005 source-binding controls

Once implemented, run `uv run --isolated --locked --all-extras --python 3.11 python -m pytest
 tests/tooling/test_secret_source.py tests/tooling/test_secret_tree.py tests/tooling/test_secret_identity.py
 tests/tooling/test_secret_process.py tests/tooling/test_secret_git.py` (one shell line).
Use an absolute capable Git and owned disposable repositories. Expected: exact declared tree/history
source binds to complete blob digests; corrupted commit/root/nested tree/blob, missing/unsupported source,
changed tree bytes, malformed raw trees, invalid/missing scope, process limits and inherited-config
attacks fail with constant context-suppressed errors. No network transport is permitted.
Repeat controls on3.12/3.13; run existing full native gates and exact-code independent review. These
controls do not prove protected worker provenance, complete source enrollment or actual finding approval.
