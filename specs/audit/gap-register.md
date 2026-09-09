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
- [Polymarket API rate limits](https://docs.polymarket.com/api-reference/rate-limits)
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
| G-007 | Blocker | Setup/database | The documented `DATABASE_URL` migration command cannot import a PostgreSQL driver. The running pipeline creates an async engine from the same sync-style URL. | `.env.example`, `alembic/env.py`, `storage/database.py`; reproduced `ModuleNotFoundError: psycopg2` | **Closed by slice 002**: locked Psycopg 3, canonical URL normalization, Alembic, async query, and disposable real-PostgreSQL proof; see [verification](../002-reproducible-runtime/evidence/verification.md). |
| G-008 | Blocker | Dependencies | A clean Apple Silicon install omits `greenlet`, causing 23 async database test errors. SQLAlchemy documents the async extra as the portable installation contract. | `pyproject.toml`; clean test run; SQLAlchemy official docs | **Closed by slice 002**: `sqlalchemy[asyncio]` locks `greenlet` on every supported interpreter; all Apple/Linux matrices pass; see [verification](../002-reproducible-runtime/evidence/verification.md). |
| G-009 | High | CI | The latest main build is red because the repository is not format-clean. | CI run 27736522053; local format check | **Closed by slice 002**: the formatter delta is applied and the blocking static job is green; see [CI evidence](../002-reproducible-runtime/evidence/verification.md#github-actions-evidence). |
| G-010 | High | Type safety | Strict type checking has five current errors and is explicitly non-blocking in CI. | `mypy src`; `.github/workflows/ci.yml` | **Closed by slice 002**: all five concrete errors are fixed, strict mypy reports 42 clean source files, and CI fails closed; see [verification](../002-reproducible-runtime/evidence/verification.md). |
| G-011 | High | Version support | Metadata promises every Python version from 3.11 upward, while CI validates only 3.11 and a default local sync selected 3.13 without an explicit support contract. | `pyproject.toml`; CI workflow; local setup | **Closed by slice 002**: metadata, lock, docs, local evidence, and blocking Linux jobs agree on Python 3.11/3.12/3.13; 3.10 and 3.14 are rejected; see [verification](../002-reproducible-runtime/evidence/verification.md). |
| G-012 | High | Migration verification | CI starts PostgreSQL and Redis but unit tests use fakes/SQLite; migrations and the documented database driver are not exercised against the service. | CI workflow and test fixtures | **Closed by slice 002**: local and blocking Linux service gates probe pinned real services and prove upgrade/downgrade/re-upgrade plus cleanup in a disposable database; see [verification](../002-reproducible-runtime/evidence/verification.md). |
| G-013a | Medium | Quick-start setup | “Only database and Redis” and “under two minutes” are not reproducible because required database dependencies are absent. | README plus G-007/G-008 | **Closed by slice 002**: the locked non-destructive foundation path is documented and completed from an uncached clean checkout in 92s under the approved five-minute boundary; see [timing evidence](../002-reproducible-runtime/evidence/verification.md#clean-checkout-timing). |
| G-013b | High | Quick-start operation | The documented first run cannot reach useful monitoring promptly because ingestion is invalid and startup blocks on metadata. | README plus G-001/G-004 | Slice 001 |
| G-014 | High | Readiness | `--config-check` validates shapes and notification presence, then says “ready to run” without reaching the database, Redis, Polygon, or trade source. | `__main__.py` | Slice 003 |
| G-015 | High | Health | Health/metrics code exists but is not wired into the pipeline; `--health-port` is parsed and ignored. | `ingestor/health.py`, `pipeline.py`, `__main__.py` | Slice 003 |
| G-016 | High | Failure propagation | A terminal background ingestion failure increments an error counter but leaves the pipeline in `RUNNING`; the CLI can wait forever while monitoring nothing. | `pipeline.py` | Slice 003 |
| G-017 | High | External defaults | The default primary Polygon endpoint currently returns HTTP 401, preventing default wallet profiling until retries/fallback and contradicting a no-key quick start. | `config.py`, README, bounded JSON-RPC probe | Slice 003; explicit dependency of slice 004 |
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
| G-029a | Medium | Ingestion documentation | The tracked prediction-market skill repeats the obsolete WebSocket ingestion contract. | `docs/skill-tracking-prediction-market-flow.md` | Slice 001 |
| G-029b | Medium | Detection documentation | The tracked prediction-market skill repeats stale threshold, signal-count, and operational-capability claims. | `docs/skill-tracking-prediction-market-flow.md` | Slice 004 |
| G-030a | Medium | Configuration diagnostics | Issue #93 contains a source URL with both `wss://` and `https://`; current validation would accept or poorly diagnose similar malformed-but-prefixed values. | GitHub issue #93 | Slice 003; no issue mutation before review |
| G-030b | High | Ingestion/startup | Issue #93's deeper obsolete-protocol and 204.35-second blocking-startup symptoms remain reproducible concerns independent of the malformed user value. | GitHub issue #93 plus code trace | Slice 001; no issue mutation before review |
| G-031 | Blocker | Source coverage | The public trades query is newest-first and bounded to 10,000 rows per reachable page range. A 2026-09-06 live-safe probe returned the full 10,000 rows even for a five-second requested window; only 83 taker-only or 259 all-participant rows were inside that exact window, and some rows fell outside documented `start`/`end` bounds. Without saturation and boundary detection, polling can silently lose trades or trust ineffective filters. | Official pagination contract plus bounded live-safe aggregate probe; no wallet data retained | Slice 001 |
| G-032 | High | Detection coverage | The public trades query defaults to `takerOnly=true`; the product has no explicit decision on monitoring only takers versus all publicly returned participants. A bounded probe with `takerOnly=false` showed multiple wallet rows per transaction, which are distinct research observations rather than simple transport duplicates. | Official parameter contract plus bounded aggregate probe | Slice 001, with detection semantics documented by slice 004 |
| G-033 | High | Storage contract | Slices 003 and 004 both require new delivery dispositions, evidence availability, and reproducibility fields in persisted assessments. Without one owned target schema, independent plans will create migration churn and incompatible records. | Specs 003 FR-012 and 004 FR-003; current risk-assessment schema | Slice 003 owns the target record/migration; slice 004 contributes required evidence fields before planning |
| G-034 | High | Test quality | Baseline test suite relies on `unittest.mock` across 22 test files with 301 constructors and 52 interaction assertions, coupling tests to implementation details rather than observable behavior. | Test inventory, baseline audit, `fakes-followup-plan.md` | Slice 002 owns it; the user-authorized mocks-to-fakes migration (working boundary fakes, `fakeredis` with a shared real/fake Redis contract in the services profile, real values, and an AST anti-mock regression) is implemented on branch `quality/fakes-over-mocks` and closes when that pull request merges. |

## Proposed Slice Map

| Execution order | Specification | Owns | Explicitly does not own |
|---|---|---|---|
| 1 | `002-reproducible-runtime` | G-007–G-012, G-013a, and G-034 | Product behavior beyond setup, migrations, required checks, and test-double quality |
| 2 | `001-supported-trade-ingestion` | G-001–G-006, G-013b, G-029a, G-030b, G-031, and G-032 | Scoring changes; authenticated/private feeds; trading |
| 3 | `003-safe-observable-operation` | G-014–G-021, G-030a, and G-033 | New detector algorithms; real notification smoke tests |
| 4 | `004-truthful-detection-contract` | G-022–G-028 and G-029b; consumes G-017/G-032/G-033 outcomes | New uncalibrated scoring signals; backtesting; trading recommendations |

The numeric feature prefix records specification creation order, not execution order. Before every
Spec Kit command, the operator MUST activate the intended slice explicitly via
`SPECIFY_FEATURE_DIRECTORY=specs/<feature-directory>`; the ignored local `.specify/feature.json`
pointer MUST NOT be trusted across slices or clones.

## Proposed Deferrals

| Item | Rationale | Owner / revisit gate |
|---|---|---|
| Backtesting and calibration workflow | No executable workflow exists and implementing a trustworthy one requires an outcome dataset, evaluation horizon, and cost model that are not present. Stored assessments remain future-compatible. | Patrick; separate approved specification |
| Sniper-cluster signal in live scoring | The library exists, but its live data, state retention, calibration, false-positive, and replay contracts are undefined. Calling it operational would be misleading. | Patrick; separate approved specification after core convergence |
| Funding-chain contribution to risk score | Funding is useful research context, but there are no approved weights or validated labels. Keep it as persisted enrichment until calibrated. | Patrick; separate approved specification |
| Book-depth impact in core scoring | A complete all-market depth contract would expand ingestion substantially. Daily volume can make current size scoring truthful without that expansion; unavailable book depth must be explicit. | Patrick; consider after supported ingestion is stable |
| Real Discord/Telegram smoke delivery | The constitution forbids it without explicit authorization. Deterministic fakes and dry-run evidence are sufficient for these slices. | Patrick; optional manual release check |
| Indexer-backed wallet age | Wallet age is recoverable through an additional history/indexer provider, but that adds a new external contract, credential/rate-limit decisions, and failure modes. The core will label unknown age honestly first. | Patrick; separate approved specification after core convergence |
| Python 3.14+ support | The currently declared open upper bound is unverified. Newer minors enter the supported matrix only after locked installation and required gates pass. | Patrick; revisit when the dependency matrix is green on the new minor |

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
  can eliminate future external drift. One aggregate-only five-second trade probe saturated a 10,000-row
  response and found returned timestamps outside the requested bounds, so source-window assumptions now
  require explicit feasibility, saturation, ordering, and loss detection before implementation.
- No production credentials, private endpoints, real webhook destinations, or production data stores
  were used.
- The issue #93 screenshot is evidence of a historical run, not proof that every current user has the
  same malformed environment value.
