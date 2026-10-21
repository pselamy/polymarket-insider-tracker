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
`main` before family 1, 14 groups in 8 packages after it, 8 groups in 6 packages after family 6, 6 groups in 4 packages after family 3, 4 groups in 2 packages after family 5, 1 group in 1 package after family 4, 0 groups after family 2. The campaign is complete: osv is now enforced by the security gate (`docs/security.md`), and a new advisory is handled by a scoped upgrade PR.

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
| 2 | web3 | web3 | 7.14.0 | 7.15.0 | 2 | runtime (direct), done: locked 7.15.0 |
| 3 | settings/env | pydantic-settings | 2.12.0 | 2.14.2 | 1 | runtime (direct), done: locked 2.14.2 |
| 3 | settings/env | python-dotenv | 1.2.1 | 1.2.2 | 2 | runtime (direct), done: locked 1.2.2 |
| 4 | alembic stack | mako | 1.3.10 | 1.4.2 | 6 | runtime (via alembic), done: locked 1.4.2 |
| 5 | pytest/pygments | pytest | 9.0.2 | 9.0.3 | 2 | runtime (via poly-eip712-structs); also dev pin, done: locked 9.0.3 |
| 5 | pytest/pygments | pygments | 2.19.2 | 2.20.0 | 2 | runtime (via pytest), done: locked 2.20.0 |
| 6 | dev tooling | virtualenv | 20.35.4 | 21.14.7 | 14 | dev-only (via pre-commit), done: locked 21.14.7 (21.7.13 first, then 21.14.7 for four later advisories) |
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
   Follow-up (osv published GHSA-5vjq-rrrf-7h2q, GHSA-8rjx-v5ww-45pp, GHSA-c947-3pg5-gm8q and
   GHSA-vxj4-q4g9-crj7 against 21.7.13, fixed in 21.14.4, 21.14.2, 21.14.2 and 21.14.7): virtualenv
   21.7.13 -> 21.14.7, the minimum release clearing all four (`uv lock --upgrade-package
   virtualenv==21.14.7`), plus transitive distlib 0.4.0 -> 0.4.4 and a new `packaging` edge on
   virtualenv (already locked). Dev-only (pre-commit activation scripts); `pyproject.toml` unchanged.
   `python -m quality.security_main` exits 0 with 0 advisory groups.
3. **Family 3, settings/env** (done): pydantic-settings 2.12.0 -> 2.14.2, python-dotenv 1.2.1 -> 1.2.2,
   the minimum fixed releases. New transitive dependency: none (`uv.lock` changes only those two
   package entries). Clears 3 osv records (2 alias groups); `pyproject.toml` is unchanged. Config
   tests (`tests/test_config.py`, `tests/test_harness_isolation.py`) pass unchanged.
4. **Family 5, pytest/pygments** (done): pytest 9.0.2 -> 9.0.3, pygments 2.19.2 -> 2.20.0, the minimum
   fixed releases. New transitive dependency: none (`uv.lock` changes only those two package entries).
   Clears 4 osv records (2 alias groups); `pyproject.toml` is unchanged (`pytest>=7.0.0` is a floor).
   Full suite passes unchanged and collection is identical (3640 tests before and after).
5. **Family 4, mako** (alembic template engine, done): mako 1.3.10 -> 1.4.2, the minimum fixed
   release (`uv lock --upgrade-package mako==1.4.2`; a plain upgrade would resolve 1.4.3). New
   transitive dependency: none (`uv.lock` changes only the mako entry). Clears 6 osv records
   (3 alias groups); `pyproject.toml` is unchanged. Alembic's `script.py.mako` renders a
   byte-identical revision under mako 1.3.10 and 1.4.2 (compared with the Create Date masked).
   The Postgres migration tests are service-gated (`RUN_SERVICE_TESTS`) and were not run here.
6. **Family 2, web3** (runtime chain client, done): web3 7.14.0 -> 7.15.0, the minimum fixed
   release per the inventory table above (`uv lock --upgrade-package web3==7.15.0`). New transitive dependency: none (`uv.lock`
   changes only the web3 entry); `pyproject.toml` is unchanged (`web3>=6.0.0` is a floor). Clears
   2 osv records (1 alias group); `python -m quality.security_main` now reports 0 advisory groups.
   Full suite passes unchanged (3639 passed, 11 skipped). The upstream 7.14.0 -> 7.15.0 changelog
   was NOT reviewed (no network in the upgrade environment). The profiler chain and funding RPC
   tests exercise web3 only through fakes, so as a substitute an offline smoke check built a real
   `AsyncWeb3(AsyncHTTPProvider(...))`, `to_checksum_address`, `eth.contract(...).functions`, and
   confirmed `eth.get_logs`/`get_block`/`block_number` exist. No live RPC call was made. Treat the
   changelog review as outstanding for the human reviewer.
