# Brownfield Gap Register

**Audit date**: 2026-09-06
**Upstream anchor**: `main` at `b54dbd3596ab7075ba28676f3194461ae4b48cd5`
**Adoption branch**: `codex/spec-kit-brownfield-adoption`
**Scope**: repository structure, public promises, runtime wiring, dependencies, storage,
tests, CI, current provider contracts, and a safe end-to-end monitoring workflow

## Evidence Baseline

The audit used the current upstream checkout, repository history and issue tracker,
deterministic local commands, and bounded read-only provider probes. No real alert was
sent and no trade was placed.

| Evidence | Result |
|---|---|
| `ruff check src tests` | Pass |
| `ruff format --check src tests` | Fail: `tests/detector/test_size_anomaly.py` |
| `mypy src` | Fail: 5 errors; CI currently ignores this failure |
| Clean `uv sync --all-extras --python 3.11 && uv run pytest -q` on Apple Silicon | 679 passed, 1 skipped, 23 errors because the async database runtime lacks `greenlet` |
| Same test command after diagnostic-only `greenlet` install | 702 passed, 1 skipped |
| Full suite coverage after diagnostic-only install | 88% total; orchestration and database paths remain materially lower |
| Documented PostgreSQL migration command | Fail: no PostgreSQL driver is installed |
| Current public trade source probe | Official public trade query returns wallet-bearing trade rows |
| Configured WebSocket protocol probe | No trade event; subscription envelope does not match the configured channel contract |
| Default primary Polygon endpoint probe | HTTP 401; the documented fallback endpoint responds |
| Latest upstream CI run at the anchor commit | Failed overall because format checking failed |

Primary external references:

- [GitHub Spec Kit guide for existing projects](https://github.com/github/spec-kit/blob/main/docs/guides/existing-projects.md)
- [GitHub Spec Kit agentic SDD workflow](https://github.com/github/spec-kit/blob/main/docs/reference/agentic-sdd.md)
- [Polymarket public trades contract](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets)
- [Polymarket Market WebSocket contract](https://docs.polymarket.com/api-reference/wss/market)
- [Polymarket changelog](https://docs.polymarket.com/changelog/predictions)
- [SQLAlchemy asynchronous installation notes](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#asyncio-platform-installation-notes-including-apple-m1)
- [Latest failed upstream CI run](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/27736522053)
- [Open issue #93](https://github.com/pselamy/polymarket-insider-tracker/issues/93)

## Severity and Disposition

- **Blocker**: the advertised primary workflow cannot run or cannot receive the data it needs.
- **High**: the workflow can report success while materially failing, can suppress a valid alert,
  or makes an important public promise that the runtime does not implement.
- **Medium**: reproducibility, support, or documentation is misleading but does not alone stop all use.
- **Disposition**: every gap maps to a proposed slice or an explicit deferral. “Document” means align
  the public contract with verified behavior, not hide a defect.

## Gap Inventory

| ID | Severity | Layer | Observed gap | Evidence | Proposed disposition |
|---|---|---|---|---|---|
| G-001 | Blocker | Ingestion | The configured endpoint and subscription envelope implement different protocols, so the tracker does not receive the wallet-bearing trade events its pipeline requires. | `.env.example`, `config.py`, `ingestor/websocket.py`; official Market WebSocket contract; live-safe probe | Slice 001 |
| G-002 | High | Public contract | README names a second WebSocket host, while settings and tests default to the CLOB Market channel; neither current contract supports the implemented anonymous `activity/trades` subscription. | `README.md`, `.env.example`, `config.py`, tests | Slice 001 |
| G-003 | High | Provider drift | Current Polymarket documentation limits RTDS to comments and crypto prices. The code depends on a legacy activity-trade topic. | Official changelog dated 2026-01-16 | Slice 001 |
| G-004 | High | Startup | Trade ingestion waits for a full sequential market metadata crawl. A report in issue #93 shows 85,594 markets taking 204.35 seconds before the failed connection attempt. | `metadata_sync.py`; issue #93 screenshot | Slice 001 |
| G-005 | High | Ingestion correctness | Repeated reads, overlapping time windows, equal timestamps, restarts, and provider caching have no specified replay or duplicate-processing contract. | Current stream-only implementation; official trade query pagination/window contract | Slice 001 |
| G-006 | Medium | Compatibility | `POLYMARKET_WS_URL` is a public setting, but a supported replacement source needs a deliberate migration/deprecation path rather than silent reinterpretation. | README/config/tests | Slice 001 |
| G-007 | Blocker | Setup/database | The documented `DATABASE_URL` migration command cannot import a PostgreSQL driver. The running pipeline creates an async engine from the same sync-style URL. | `.env.example`, `alembic/env.py`, `storage/database.py`; reproduced `ModuleNotFoundError: psycopg2` | Slice 002 |
| G-008 | Blocker | Dependencies | A clean Apple Silicon install omits `greenlet`, causing 23 async database test errors. SQLAlchemy documents the async extra as the portable installation contract. | `pyproject.toml`; clean test run; SQLAlchemy official docs | Slice 002 |
| G-009 | High | CI | The latest main build is red because the repository is not format-clean. | CI run 27736522053; local format check | Slice 002 |
| G-010 | High | Type safety | Strict type checking has five current errors and is explicitly non-blocking in CI. | `mypy src`; `.github/workflows/ci.yml` | Slice 002 |
| G-011 | High | Version support | Metadata promises every Python version from 3.11 upward, while CI validates only 3.11 and a default local sync selected 3.13 without an explicit support contract. | `pyproject.toml`; CI workflow; local setup | Slice 002; Patrick decision |
| G-012 | High | Migration verification | CI starts PostgreSQL and Redis but unit tests use fakes/SQLite; migrations and the documented database driver are not exercised against the service. | CI workflow and test fixtures | Slice 002 |
| G-013 | Medium | Quick start | “Only database and Redis” and “under two minutes” are not reproducible because required drivers are absent, ingestion is invalid, and startup blocks on metadata. | README plus G-001/G-004/G-007 | Slices 001–002 |
| G-014 | High | Readiness | `--config-check` validates shapes and notification presence, then says “ready to run” without reaching the database, Redis, Polygon, or trade source. | `__main__.py` | Slice 003 |
| G-015 | High | Health | Health/metrics code exists but is not wired into the pipeline; `--health-port` is parsed and ignored. | `ingestor/health.py`, `pipeline.py`, `__main__.py` | Slice 003 |
| G-016 | High | Failure propagation | A terminal background ingestion failure increments an error counter but leaves the pipeline in `RUNNING`; the CLI can wait forever while monitoring nothing. | `pipeline.py` | Slice 003 |
| G-017 | Medium | External defaults | The default primary Polygon endpoint currently returns HTTP 401, adding avoidable retries and contradicting a no-key quick start. | `config.py`, README, bounded JSON-RPC probe | Slice 003 |
| G-018 | High | Effect safety | A threshold-passing dry run sets the delivery dedup key before delivery is skipped, so a later real run can suppress the alert. | `detector/scorer.py` then `pipeline.py` ordering | Slice 003 |
| G-019 | High | Effect safety | A failed multi-channel delivery also retains the dedup key, suppressing retry for the configured window. | scorer/pipeline/dispatcher ordering | Slice 003 |
| G-020 | Medium | Ownership | `AlertHistory` and `RiskScorer` implement separate dedup concepts, but only the scorer path is live, obscuring the authoritative delivery lifecycle. | `alerter/history.py`, `detector/scorer.py`, `pipeline.py` | Slice 003 |
| G-021 | High | End-to-end evidence | There is no deterministic test that drives a trade through ingestion, enrichment/profiling, detection, scoring, persistence, and safe delivery suppression. | Test inventory; orchestration coverage | Slice 003 |
| G-022 | High | Detection dataflow | Market daily volume is cached, but the live pipeline does not pass it into size analysis; real volume impact is therefore always zero and niche classification can fall back to “unknown.” | `metadata_sync.py`, `size_anomaly.py`, `pipeline.py` | Slice 004 |
| G-023 | High | Signal semantics | README promises wallet age under 48 hours, but for nonzero nonces the Polygon client cannot discover the first transaction without an indexer; the live decision is generally nonce-based with age unknown. | README, `profiler/chain.py`, `profiler/analyzer.py`, fresh-wallet detector | Slice 004 |
| G-024 | High | Scoring contract | README and the tracked skill document a default threshold of 0.6; runtime and changelog use 0.80. | README, docs skill, config/scorer/changelog | Slice 004 |
| G-025 | High | Scoring contract | The 3+ signal bonus is unreachable because the live signal bundle counts at most fresh-wallet and size-anomaly signals; niche only contributes weight, not signal count. | `detector/scorer.py` | Slice 004 |
| G-026 | High | Capability wiring | `SniperDetector` has isolated tests but is not instantiated by the pipeline; “ML + heuristics” presents it as operational. | detector package/tests vs `pipeline.py` | Slice 004; proposed explicit experimental status |
| G-027 | High | Capability wiring | Funding chains are traced and stored only after a fresh-wallet signal; funding suspiciousness is absent from scoring, persisted risk inputs, and alert output despite the public “funding chain analysis” claim. | `pipeline.py`, signal/assessment models, README | Slice 004; proposed enrichment-only contract |
| G-028 | Medium | Research claims | Changelog describes stored assessments as ground truth for future backtests and implies backtest scripts, but no backtest workflow exists in the repository. | CHANGELOG and file inventory | Explicitly defer; separate future spec required |
| G-029 | Medium | Documentation | The tracked prediction-market skill repeats obsolete ingestion, threshold, signal-count, and operational-capability claims. | `docs/skill-tracking-prediction-market-flow.md` | Align within owning slices |
| G-030 | Medium | Issue hygiene | Issue #93 remains open with an endpoint containing both `wss://` and `https://`; its deeper ingestion/startup symptoms remain valid even though the literal URL may be user configuration. | GitHub issue #93 | Reproduce/resolve through slices 001/003; no issue mutation before review |

## Proposed Slice Map

| Order | Specification | Owns | Explicitly does not own |
|---|---|---|---|
| 1 | `001-supported-trade-ingestion` | G-001–G-006, ingestion portion of G-013/G-030 | Scoring changes; authenticated/private feeds; trading |
| 2 | `002-reproducible-runtime` | G-007–G-013 | Feature behavior beyond setup, migrations, and required checks |
| 3 | `003-safe-observable-operation` | G-014–G-021, operational portion of G-030 | New detector algorithms; real notification smoke tests |
| 4 | `004-truthful-detection-contract` | G-022–G-029 | New uncalibrated scoring signals; backtesting; trading recommendations |

## Proposed Deferrals

| Item | Rationale | Owner / revisit gate |
|---|---|---|
| Backtesting and calibration workflow | No executable workflow exists and implementing a trustworthy one requires an outcome dataset, evaluation horizon, and cost model that are not present. Stored assessments remain future-compatible. | Patrick; separate approved specification |
| Sniper-cluster signal in live scoring | The library exists, but its live data, state retention, calibration, false-positive, and replay contracts are undefined. Calling it operational would be misleading. | Patrick; separate approved specification after core convergence |
| Funding-chain contribution to risk score | Funding is useful research context, but there are no approved weights or validated labels. Keep it as persisted enrichment until calibrated. | Patrick; separate approved specification |
| Book-depth impact in core scoring | A complete all-market depth contract would expand ingestion substantially. Daily volume can make current size scoring truthful without that expansion; unavailable book depth must be explicit. | Patrick; consider after supported ingestion is stable |
| Real Discord/Telegram smoke delivery | The constitution forbids it without explicit authorization. Deterministic fakes and dry-run evidence are sufficient for these slices. | Patrick; optional manual release check |

## Verified Non-Gaps and Boundaries

- Both existing migrations define downgrade functions; the gap is that supported-driver execution is untested.
- Secrets are represented with secret-aware configuration fields and the printed summary redacts URL passwords.
- The public trade endpoint currently returns the wallet, market, side, price, size, timestamp,
  outcome, asset, and transaction fields needed by the existing trade model.
- The Market WebSocket is a valid public source for order-book and last-price events, but its
  documented last-trade event does not contain the trader wallet required by this product.
- The codebase contains substantial unit coverage. The gap is behavioral integration and required
  environment coverage, not absence of tests.
- Trading execution, UI work, monetization, and accusations of actual insider conduct remain outside scope.

## Audit Limitations

- Provider behavior was checked with short, read-only probes on 2026-09-06; no availability promise
  can eliminate future external drift. The specifications therefore require explicit degraded state.
- No production credentials, private endpoints, real webhook destinations, or production data stores
  were used.
- The issue #93 screenshot is evidence of a historical run, not proof that every current user has the
  same malformed environment value.
