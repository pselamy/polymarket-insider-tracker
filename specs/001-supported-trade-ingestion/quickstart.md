# Quickstart Validation: Supported Trade Ingestion

This is the target validation sequence for slice 001 after implementation. Commands prove the
supported ingestion contract; nothing here places a trade, sends a real alert, or requires a
Polymarket credential. Until Patrick authorizes implementation, only the feasibility record and the
planning artifacts exist.

## Prerequisites

- The slice 002 foundation: `uv sync --locked --all-extras --python 3.13`, `cp .env.example .env`,
  `docker compose up -d --wait postgres redis`, and a green
  `uv run --env-file .env python scripts/verify.py --profile all`.
- No `POLYMARKET_WS_URL` in `.env` (the example file no longer sets one).

## Deterministic Proof (no network)

```bash
uv run pytest tests/ingestor/test_trade_rows.py tests/ingestor/test_trades_source.py \
  tests/ingestor/test_observation_boundary.py tests/ingestor/test_trade_poller.py \
  tests/tooling/test_trades_smoke.py tests/test_config.py tests/test_pipeline.py -q
```

Expected result:

- overlap, equal-second, out-of-order, malformed-row, throttling, timeout, transient-recovery,
  terminal-failure, first-start, restart-inside-horizon, restart-beyond-horizon, saturation with
  recovery page, saturation without recovery, continuity mismatch, and graceful-stop scenarios pass;
- every distinct fixture observation reaches the pipeline callback once and every exact repeat is
  suppressed (SC-001, SC-004);
- the pipeline starts the poller before the metadata crawl completes (SC-002);
- the seven named smoke cases produce the documented verdicts with `retained_wallet_identifiers: false`
  (SC-005);
- the deprecated WebSocket setting warns and is never used, and the new source settings validate (US3).

The suite uses `fakeredis`, `httpx.MockTransport` trade servers, synthetic wallet addresses, and the
real pipeline assembled by `tests/fakes/pipeline.py`. No test contacts the provider.

## Real Redis Parity

```bash
RUN_SERVICE_TESTS=1 uv run --env-file .env pytest tests/integration/test_redis_contract.py -q
```

Expected result: the checkpoint hash, identity sorted-set trim, loss-event list, and transactional
write scenarios pass against both `fakeredis` and the loopback Redis. The `services` profile runs the
same file as its `redis-contract` gate.

## Live-Safe Smoke (explicit, bounded, aggregate-only)

```bash
uv run --env-file .env python scripts/trades_smoke.py --live --window-seconds 5 --json
```

Expected result: one anonymous request per coverage mode, exit `0`, and a JSON record with the fields in
[data-model.md](data-model.md#smoke-evidence-record) reporting reachability, schema compatibility,
newest provider timestamp, measured lag, page saturation, and cache headers. The record contains no
wallet, name, pseudonym, or raw row. No alert is sent, no Redis key is written, and the command is never
part of `scripts/verify.py` or CI. Exit `1` names the failing case; exit `2` is an invocation error.

## Running the Tracker

```bash
uv run --env-file .env python -m polymarket_insider_tracker --config-check
uv run --env-file .env python -m polymarket_insider_tracker --dry-run
```

Expected result:

- the configuration check prints the trades URL, coverage mode, poll interval, and recovery horizon, and
  reports `POLYMARKET_WS_URL` as `(not set)`; if it is set, an actionable deprecation warning is shown;
- the tracker logs `ingestion state: starting -> running` before the market metadata crawl finishes;
- within one poll interval after the first proven page, new observations reach the detection pipeline;
- status logging shows the boundary time advancing, duplicate and padding counts, and zero loss events
  under healthy conditions;
- stopping with `Ctrl+C` cancels the in-flight request, finishes the in-progress observation, writes no
  partial checkpoint, and exits `0`.

## Expected Failure Examples

- `POLYMARKET_WS_URL=https://example.invalid`: rejected with the existing scheme error; never treated as
  an HTTP source.
- `POLYMARKET_WS_URL=wss://example.invalid`: accepted with a deprecation warning naming the replacement
  settings; acquisition ignores it.
- `POLYMARKET_TRADES_COVERAGE=makers`: rejected at load with the two accepted values.
- `POLYMARKET_TRADES_POLL_INTERVAL_SECONDS=0`: rejected; the minimum keeps the request budget at or below
  5% of the published limit.
- Provider returns HTTP 429 or times out: retries with capped jittered backoff, state `degraded` after
  exhaustion, automatic recovery on the next successful cycle, no fabricated events.
- Provider returns HTTP 401/403/404 or a non-list body: state `failed`, error surfaced, poller stopped.
- Two full pages fail to reach the boundary: state `possible-data-loss`, boundary frozen, gap logged; the
  interval becomes a recorded loss event once it ages past the recovery horizon.

## Required Gates

Every existing gate remains required and must be green before convergence:

```bash
uv run python scripts/verify.py --profile static
uv run python scripts/verify.py --profile compatibility
uv run --env-file .env python scripts/verify.py --profile services
uv run --isolated --locked --all-extras --python 3.11 python scripts/verify.py --profile compatibility
uv run --isolated --locked --all-extras --python 3.12 python scripts/verify.py --profile compatibility
uv run --isolated --locked --all-extras --python 3.13 python scripts/verify.py --profile compatibility
```

The Complexipy gate keeps its fail-closed command and maximum of 5 for functions and module control
flow; the new modules are designed as small named steps so no exemption is needed.
