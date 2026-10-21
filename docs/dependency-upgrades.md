# Dependency upgrade inventory (osv advisories by package family)

Generated 2026-10-21 by the pinned scanner the security gate uses
(`osv-scanner` 2.6.0 from `mise.toml`), against `uv.lock` (129 packages):

```bash
mise --yes --locked exec -- osv-scanner scan source --lockfile uv.lock \
  --config quality/osv-scanner.toml --all-vulns --all-packages --format json \
  --output-file <report>
```

"Minimum fixed" is the lowest non-beta release that clears every advisory on the
package (the highest of each advisory's lowest fixed version above the locked one).
Scope is `runtime` when the package is reachable without the `dev` extra/group
(`uv tree --locked --no-dev`, confirmed per package with `--invert`), else `dev-only`.
Advisory counts include GHSA/PYSEC aliases as separate osv records. `docs/security.md`
counts osv alias groups instead (one per underlying advisory): 60 groups in 17 packages on
`main` before family 1, 14 groups in 8 packages after it, 8 groups in 6 packages after family 6.

## Families

| # | Family | Package | Locked | Minimum fixed | Advisories | Scope |
|---|--------|---------|--------|---------------|-----------:|-------|
| 1 | HTTP | aiohttp | 3.13.3 | 3.14.4 | 56 | runtime (direct) |
| 1 | HTTP | urllib3 | 2.6.2 | 2.8.0 | 12 | runtime (via requests) |
| 1 | HTTP | requests | 2.32.5 | 2.33.0 | 2 | runtime (via py-builder-signing-sdk) |
| 1 | HTTP | multidict | 6.7.0 | 6.9.1 | 2 | runtime (via aiohttp) |
| 1 | HTTP | idna | 3.11 | 3.15 | 2 | runtime (via httpx, requests, aiohttp) |
| 1 | HTTP | certifi | 2026.1.4 | 2026.10.19 | 2 | runtime (via httpx, requests) |
| 1 | HTTP | anyio | 4.12.0 | 4.14.2 | 4 | runtime (via httpx) |
| 1 | HTTP | h2 | 4.3.0 | 4.4.1 | 2 | runtime (httpx `http2` extra) |
| 1 | HTTP | hpack | 4.1.0 | 4.2.0 | 2 | runtime (via h2) |
| 2 | web3 | web3 | 7.14.0 | 7.15.0 | 2 | runtime (direct) |
| 3 | settings/env | pydantic-settings | 2.12.0 | 2.14.2 | 1 | runtime (direct) |
| 3 | settings/env | python-dotenv | 1.2.1 | 1.2.2 | 2 | runtime (direct) |
| 4 | alembic stack | mako | 1.3.10 | 1.4.2 | 6 | runtime (via alembic) |
| 5 | pytest/pygments | pytest | 9.0.2 | 9.0.3 | 2 | runtime (via poly-eip712-structs); also dev pin |
| 5 | pytest/pygments | pygments | 2.19.2 | 2.20.0 | 2 | runtime (via pytest) |
| 6 | dev tooling | virtualenv | 20.35.4 | 21.7.13 | 10 | dev-only (via pre-commit), done: locked 21.7.13 |
| 6 | dev tooling | filelock | 3.20.2 | 3.20.3 | 2 | dev-only (via virtualenv), done: locked 3.32.7 |

No advisory lacks a fixed release. h11, httpx and httpcore have no advisories; they are
not upgraded. Packages not listed (redis, sqlalchemy, websockets, ...) carry no advisories.

Family 1 notes: aiohttp and urllib3 have fixes on several release lines; the minimums
above are the lines reachable without a major change (aiohttp 3.13.x -> 3.14.x is a
minor bump; urllib3 2.6 -> 2.8). requests 2.33.0 is a minor bump. The `aiohttp`,
`httpx` and `requests` direct bounds in `pyproject.toml` are floors (`>=`), so the
lock alone is enough; no pin edits are expected for family 1.

## Proposed PR order (smallest risk first)

1. **Family 1, HTTP stack** (this branch): aiohttp, urllib3, requests, multidict, idna,
   certifi, anyio, h2, hpack. Largest advisory count and the network-facing surface;
   scoped to ingestor/alerter/profiler HTTP clients, covered by existing tests.
2. **Family 6, dev tooling** (done): virtualenv 20.35.4 -> 21.7.13, filelock 3.20.2 -> 3.32.7,
   plus new transitive python-discovery 1.6.4. Dev-only, no runtime effect. filelock lands above
   its minimum fix (3.20.3) because virtualenv 21.7.13 requires a newer filelock; pinning 3.20.3
   is unsatisfiable. Clears 12 osv records (6 alias groups); `pyproject.toml` is unchanged.
3. **Family 3, settings/env**: pydantic-settings, python-dotenv. Small, touches config loading.
4. **Family 5, pytest/pygments**: patch/minor bumps, test-runner only in practice.
5. **Family 4, mako** (alembic template engine): 1.3 -> 1.4 is a minor line change; run
   migration tests against disposable databases only.
6. **Family 2, web3**: runtime chain client; highest behavioural risk, needs the
   profiler/funding RPC tests and a changelog review, so it goes last.
