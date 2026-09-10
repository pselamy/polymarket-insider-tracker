# Verification Record: Supported Trade Ingestion

**Recorded**: 2026-09-10 by Claude Fable 5.1 (corrective/first implementation pass; the Agy first
pass was attempted but its account quota rejected the run before any edit)

**Baseline**: planning commit `3b537da` (`docs(specs): plan supported trade ingestion (#120)`)

**Host**: `dev@selamy-core`, Ubuntu 24.04 x86_64, uv 0.11.21, CPython 3.11.15 / 3.12.3 / 3.13.14

**Commit hashes**: recorded during T027 (review sequence); the implementation commit is named in the
handoff.

## Required gates

| Gate | Command | Result |
|---|---|---|
| static | `uv run python scripts/verify.py --profile static` | passed: lock, format, lint, strict-types, pyright, vulture, complexipy (9.45 s) |
| compatibility 3.11 | `uv run --isolated --locked --all-extras --python 3.11 python scripts/verify.py --profile compatibility` | passed: lock, imports, tests (1092 passed, 2 skipped) |
| compatibility 3.12 | `uv run --isolated --locked --all-extras --python 3.12 python scripts/verify.py --profile compatibility` | passed (1092 passed, 2 skipped) |
| compatibility 3.13 | `uv run --isolated --locked --all-extras --python 3.13 python scripts/verify.py --profile compatibility` | passed (1092 passed, 2 skipped) |
| full profile with `.env.example` | `cp .env.example .env && uv run --env-file .env python scripts/verify.py --profile all` | static and compatibility gates passed; `services` failed at `runtime-probe`: the host PostgreSQL rejects the Compose credentials (`password authentication failed for user "tracker"`) and the `dev` user has no Docker socket; `redis-contract` and `migrations` not run after that failure (exit 1). The temporary `.env` was removed afterwards. |
| redis-contract (substitute evidence) | `RUN_SERVICE_TESTS=1 REDIS_URL=redis://127.0.0.1:6390 uv run pytest tests/integration/test_redis_contract.py -q` against a disposable Redis 8 built from source on loopback | 29 passed (fake and real implementations, including the new checkpoint hash, identity trim, loss-event list, and transactional advance scenarios) |
| migrations | not exercised locally (no PostgreSQL available to this user); this slice adds no schema or migration; the CI services job remains the authority |
| Complexipy fail-closed launcher | `uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore --ignore-complexity=false --snapshot-ignore=true --snapshot-create=false --exclude=. --check-script=true` (inside the static profile) | passed; no baseline, ignore, exclusion, or suppression added |
| `complexipy . --diff main` | local `main` ref is `ff146dc` (#115), older than the branch base | Net 12 regressed, 72 improved, 830 new, 39 removed; every regression is in a file this slice did not touch (differences between `ff146dc` and `3b537da`) |
| `complexipy . --diff 3b537da` | against the actual branch base | 2 regressed (`tests/fakes/clob.py::FakeBaseClobClient::get_simplified_markets` 0→1, `tests/fakes/pipeline.py::wire_pipeline` 1→3), 1 improved, 481 new; every function and module at or below 5 |
| mock AST policy | `uv run pytest tests/tooling/test_test_quality.py -q` | 29 passed; no `unittest.mock` spelling anywhere under `tests/` or `conftest.py` |
| whitespace | `git diff --check` | clean |
| Vulture | `uv run vulture src tests scripts alembic conftest.py` | no findings |

## Red-green witnesses per behavior

Every command below was run exactly as shown; "red" is the observed failure before the production
change, "green" the observed pass afterwards.

| Task | Red command and observed failure | Green |
|---|---|---|
| T003 Redis parity | `uv run pytest tests/integration/test_redis_contract.py -q` (contract scenarios; no red by design, they pin real-service behavior) | 16 passed on fakeredis; 29 passed with `RUN_SERVICE_TESTS=1 REDIS_URL=redis://127.0.0.1:6390` |
| T004 → T005 rows | `uv run pytest tests/ingestor/test_trade_rows.py -q` → `ModuleNotFoundError: polymarket_insider_tracker.ingestor.trade_rows` | 49 passed |
| T006 → T007 source | `uv run pytest tests/ingestor/test_trades_source.py -q` → `ModuleNotFoundError: ...trades_source` | 36 passed |
| T008 → T009 boundary | `uv run pytest tests/ingestor/test_observation_boundary.py -q` → `ModuleNotFoundError: ...observation_boundary` | 20 passed (22 after the `ignored` identities scenario, added together with its implementation during T017; not witnessed red) |
| T019 → T020 config | `uv run pytest tests/test_config.py tests/test_main.py tests/ingestor/test_websocket.py -q` → `ImportError: cannot import name 'TradesCoverage'`, `cannot import name 'WebSocketSettingDeprecationWarning'`, `DID NOT WARN` for `TradeStreamHandler` | 115 passed |
| cache-only metadata lookup | `uv run pytest tests/ingestor/test_metadata_sync.py -k get_cached_market` → `AttributeError: 'MarketMetadataSync' object has no attribute 'get_cached_market'` | passed |
| T010 → T012 poller | `uv run pytest tests/ingestor/test_trade_poller.py -q` → `ModuleNotFoundError: ...trade_poller` | 11 passed |
| T011 → T013 pipeline | `uv run pytest tests/test_pipeline.py -k TestIngestionWiring` → `TimeoutError` (start blocked on the metadata crawl), `condition was not reached` (nothing polled), `assert 0 == 1` (no ingestion-state hook) | 24 passed |
| T014/T015 → T016/T017 failure and recovery | `uv run pytest tests/ingestor/test_trade_poller.py -q` → 7 failed: invalid-row counting, restart beyond horizon, recovery page, frozen boundary, gap close, horizon expiry, continuity mismatch | 31 passed. Honesty note: the transient/terminal, empty-response, restart-inside-horizon, graceful-stop, and crash tests passed on their first run because the T012 cycle wrapper already classified transient and terminal errors; they were not witnessed red. |
| T018 restart through the pipeline | `uv run pytest tests/test_pipeline.py -k TestIngestionRestart` → `assert 4 == 3` (one re-delivery because the pipeline cancelled the poller task mid-callback) | passed after `TradePoller.stop()` joins the running loop when called from another task |
| FR-007 pre-anchor replay (found by the live run) | `uv run pytest tests/ingestor/test_trade_poller.py -k TestPreStartHistory` → `assert [1788983599, 1788983721] == [1788983721]` | passed after bounding candidates by the oldest retained identity |
| T022 → T023 smoke | `uv run pytest tests/tooling/test_trades_smoke.py -q` → `FileNotFoundError: scripts/trades_smoke.py` | 21 passed |

Deterministic proof from the quickstart:

```bash
uv run pytest tests/ingestor/test_trade_rows.py tests/ingestor/test_trades_source.py \
  tests/ingestor/test_observation_boundary.py tests/ingestor/test_trade_poller.py \
  tests/tooling/test_trades_smoke.py tests/test_config.py tests/test_pipeline.py -q
```

Full deterministic suite: 1092 passed, 2 skipped on each supported interpreter. No test contacts a
provider: `wire_pipeline` now injects the shared `FakeGammaClient`; before this slice, starting the
metadata sync through `wire_pipeline` would have called the real gamma API (observed once and fixed
before any test was kept).

## Live-safe evidence

See [smoke.md](smoke.md). Verdict: product stop gate condition 3 tripped by the provider's
`outcomeIndex` placeholder; T024 is incomplete pending Patrick's decision on the identity tuple.

## Not done

- T024 stopped at the product stop gate (see above).
- T025 services profile: PostgreSQL probe and migrations could not run on this host; Redis parity was
  proven against a disposable real Redis instead.
- T027 and T028 are reserved for the independent review and PR preparation.

## Approved correction and final local gates (2026-09-10)

Patrick approved removing mutable `outcomeIndex` from identity and treating missing or out-of-range
indexes as unknown, repairable enrichment. The corrective pass also resolved every independent
review finding: stable emission floor, retry-safe restart re-anchor, all-valid-row outcome counting,
terminal-state preservation, Redis key-type/WATCH atomicity, anonymous non-redirecting HTTP requests,
transport-error redaction, saturated-page quality, and the six new typing suppressions.

Two independent final reviews returned `SHIP`. Their focused suites passed 196 and 201 tests; the
second reviewer also ran all 37 shared Redis scenarios against both fakeredis and the actual loopback
Redis service.

```bash
uv run --env-file .env.example python scripts/verify.py --profile all
```

Result: passed in 40.11 seconds. Black, Ruff, mypy, Pyright, Vulture, and Complexipy (maximum 5) all
passed; the compatibility suite passed with 1,113 tests and 2 skips; both runtime services were
reachable; all 37 fake/real Redis contract cases passed; and the PostgreSQL migration sequence
`002_risk_assessments -> 001_initial -> 002_risk_assessments` plus an async query and cleanup passed.
This supersedes the earlier host-prerequisite failure recorded above.

The corrected live run is recorded in `evidence/smoke.md`; all six product stop conditions held.
The coverage-enforcement gap is intentionally reserved for its own focused follow-up PR: this slice
does not claim that the current test command collects or enforces near-100% line and branch coverage.
