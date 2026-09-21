# Contract: Test Quality and Working Fakes Over Mocks

## Purpose

This contract governs test doubles for `polymarket-insider-tracker`. Tests verify observable
behavior and state through real product code, real values, and small working fakes at external
boundaries. `unittest.mock` and generic mock frameworks are prohibited.

## 1. Principles

1. **State over interaction**: tests assert return values, persisted rows, Redis state, delivered
   payloads, logged failures, and stats counters, not call sequences or argument tuples.
2. **Real values**: settings, models, signals, assessments, SDK responses, and HTTP responses are
   the real types (`Settings`, `TradeEvent`, `RiskAssessment`, `OrderBookSummary`, `httpx.Response`).
3. **Boundaries only**: a fake stands in for something the test cannot run: alert delivery, the
   py-clob-client SDK, the Polygon JSON-RPC surface, webhook servers, a WebSocket, a callback the
   product invokes. Product classes between the test and that boundary are real.
4. **Shared contracts for reusable doubles**: the Redis double is `fakeredis`, and the same
   behavioral suite runs against it and against a real loopback Redis in the services profile.
5. **Recorded effects are contracts, not spies**: a channel keeps delivered payloads, a webhook
   server keeps its requests, a log index keeps queried block windows. Those are the externally
   observable outputs of the product. Counting them is valid when the count is the contract
   (retries, deliveries, RPC chunking). The context-manager delegation test narrowly checks its
   public promise to start and stop the pipeline; ordinary success-path tests use the real
   components. Address-keyed trace failure injection exercises the batch error path that lower
   RPC layers otherwise absorb, without changing successful traces or depending on call order.

## 2. Prohibited

- Any spelling of `unittest.mock` or the `mock` backport: `import unittest.mock`,
  `from unittest import mock`, aliases, `import mock`, and attribute access through `import unittest`.
- Generic callable or attribute-generating doubles, `return_value`/`side_effect` style
  configurators, response ladders keyed by call order, and assertion helpers that mirror calls.
- Fakes that subclass the product class they replace, or that accept impossible inputs
  (for example an empty stream record) to satisfy a fixture.
- Casts, `# type: ignore`, skips, or xfails introduced to make a double fit.
- Weakening or deleting a baseline scenario; every baseline test ID is retained.

## 3. Allowed

- `pytest.MonkeyPatch` for environment variables, replacing a boundary factory
  (`httpx.AsyncClient`, `BaseClobClient`, `ws_connect`, `get_settings`, `run_pipeline`), and
  deliberate failure injection.
- Real `httpx.AsyncClient` bound to `httpx.MockTransport` handlers that behave like the remote
  server (`tests/fakes/alerts.py`).
- Real unreachable services as failure injectors: a `redis.asyncio.Redis` or `DatabaseManager`
  bound to a closed loopback port fails exactly as production would.
- Real in-memory SQLite engines behind the real `DatabaseManager`, repositories, and sessions.
- Boundary fakes in `tests/fakes/`:
  - `FakeAlertChannel`: keeps delivered `FormattedAlert` payloads; `accepting` models failure.
  - `FakeWebhookServer` (`discord_webhook`, `telegram_bot_api`): a rate-limiting or failing
    webhook/Bot API server behind a real transport.
  - `FakeBaseClobClient`: py-clob-client responses (cursor-keyed pages, real `OrderBookSummary`).
  - `FakeEth` / `FakeAsyncWeb3` / `TransferLogIndex`: the Polygon RPC surface with transient or
    permanent faults and an `eth_getLogs` index filtered by token, recipient, and block window.
  - `FakeMetadataSync`: market metadata keyed by condition ID with per-market failures.
  - `wire_pipeline`: assembles the real `Pipeline` (real Polygon client, analyzer, tracer,
    metadata sync, detectors, scorer, formatter, dispatcher) over those boundaries.
- Small file-local fakes for a callback or connection the product consumes
  (`RecordingTradeCallback`, `FakeWebSocket`, `FakeWalletAnalyzer`, `FakeClobClient`).

## 4. Redis

Redis is never hand-built. Unit tests use `fakeredis.FakeAsyncRedis` (pinned `fakeredis==2.38.0`,
a `redis.asyncio.Redis` subclass) through the `fake_redis` fixture. Fidelity for every operation the
product exercises is proven by `tests/integration/test_redis_contract.py`, which runs identical
scenarios against `fakeredis` always and against the real loopback Redis named by `REDIS_URL` when
`RUN_SERVICE_TESTS=1` selects it. The `redis-contract` gate of the `services` profile makes the real
run mandatory in CI. The suite covers string values and bytes encoding, `SET NX EX` and TTL rules,
key-type exclusivity, sorted-set ranges and ordering, transactional pipelines, stream
append/consumer-group/pending/ack/trim lifecycles including deleted-entry tombstones, error types, and
pattern scans. Every scenario works inside a unique namespace that it deletes; a shared service is
never flushed, and an unreachable or non-loopback service fails the gate instead of skipping.

## 5. Enforcement

1. `tests/tooling/test_test_quality.py` parses every file under `tests/` (itself included) and the
   root `conftest.py` and rejects every spelling of `unittest.mock`; fixtures prove aliases and
   attribute access are caught while `pytest.MonkeyPatch`, `httpx.MockTransport`, and string
   literals are allowed.
2. Black, Ruff, strict mypy and Pyright, default-confidence Vulture over `tests/` (no unused fake
   methods or recorded attributes), and fail-closed Complexipy `<= 5` for functions and modules.

## 6. Mutation testing (fault-detection dimension)

The mutation job adds the one missing quality dimension — fault detection — over the
secret-redaction path, without touching any gate in section 5. Authority is executable
configuration: `.github/workflows/mutation.yml` (job `mutation`,
`Mutation (mutmut redaction target)`) and the `[tool.mutmut]` section of `pyproject.toml`.
Tool-selection rationale lives in [research.md](../research.md) Decision 13; this contract
states only the enforceable test requirements:

1. Scope: mutation is fenced to `src/polymarket_insider_tracker/redaction.py`, exercised
   by the existing `tests/test_redaction.py` adversarial suite. The scoping mechanism is
   defined once in [research.md](../research.md) Decision 13: `source_paths` selects the
   redaction module as the mutation set and `only_mutate` is the redundant glob fence on
   the same file, so nothing else in the widened `also_copy` package is mutated;
   `also_copy` closes the `mutants/` import graph so collection succeeds.
2. Baseline-green precondition: the baseline `tests/test_redaction.py` run must exit 0
   first; syntax, import, or collection errors never count as kills.
3. Non-empty mutant-set gate: `mutmut export-cicd-stats` must produce a non-empty
   `mutants/mutmut-cicd-stats.json`; a zero `total` fails the job, so misconfiguration
   never passes silently.
4. Display-only triage: `mutmut results` runs as a separate informational step with its
   exit code preserved (no `|| true`); it is non-gating display.
5. No score threshold and no requiredness are claimed. The gate is "runner executed +
   baseline green + mutants generated", not a kill-rate claim.
