# Contract: Live-Safe Smoke Evidence

## Purpose

Defines the bounded, explicit, aggregate-only check that proves source reachability and schema
compatibility against the real provider (FR-013, SC-005), and the deterministic seven-case suite that
proves the check's verdicts without any network access.

## Command

```text
uv run --env-file .env python scripts/trades_smoke.py [--live] [--window-seconds N] [--coverage all|taker-only|both] [--json]
```

| Option | Default | Rule |
|---|---|---|
| `--live` | off | Without it the command exits `2` and explains that live runs are explicit |
| `--window-seconds` | `5` | 1–60; used only to compute in-window aggregates |
| `--coverage` | `both` | One request per selected coverage mode |
| `--json` | off | Emit exactly one JSON object per record on stdout |

The command reads `POLYMARKET_TRADES_URL` when set and otherwise uses the documented default. It never
reads `POLYMARKET_WS_URL`, never opens Redis or PostgreSQL, never constructs an alert channel, and never
sends a credential. It is not part of any `scripts/verify.py` profile or CI job.

## Record

One record per request with the fields in [data-model.md](../data-model.md#smoke-evidence-record).
Invariants enforced by tests:

- the serialized record contains no 40-hex-character address, no `name`, `pseudonym`, `bio`, or
  `profileImage` value, and no raw row;
- `retained_wallet_identifiers` is `false`;
- `response_sha256` is the digest of the raw body and the body is discarded after aggregation.

## Seven Named Cases

| Case | Deterministic fixture | Verdict rule |
|---|---|---|
| `valid-wallet-bearing` | HTTP 200 list with rows containing every identity-bearing field | `passed` when `valid_rows > 0` and `invalid_rows / row_count <= 0.05` |
| `valid-empty` | HTTP 200 `[]` | `passed`; reachability true, `newest_timestamp` null, no failure |
| `throttled` | HTTP 429 with `Retry-After` | `passed` when the client honoured the delay and the record reports `http_status: 429` with `transient: true`; the smoke does not spin |
| `timeout` | Transport that never answers within the timeout | `passed` when the record reports `timeout: true` and `http_status: null` |
| `malformed-row` | HTTP 200 list with one row missing `proxyWallet` among valid rows | `passed` when valid rows are counted, the invalid row is counted under `missing_required_fields`, and no row is invented |
| `incompatible-schema` | HTTP 200 object body | `failed` with `incompatible-schema`; exit `1` in live mode |
| `possible-page-saturation` | HTTP 200 list of exactly 10,000 rows whose oldest timestamp is newer than the window start | `passed` with `page_saturated: true` and a warning that saturation alone is not loss; the boundary contract owns loss detection |

Live mode classifies the real response into exactly one case per request; the exit code is `0` only
when every classified case's verdict is `passed`.

## Exit Status

| Code | Meaning |
|---|---|
| `0` | Every requested coverage mode produced a `passed` record |
| `1` | At least one record failed (`incompatible-schema`, terminal HTTP status, or every attempt transient) |
| `2` | Invocation error: `--live` missing, invalid option, or invalid `POLYMARKET_TRADES_URL` |

## Evidence Recording

Live results captured before implementation approval are in
[../evidence/feasibility.md](../evidence/feasibility.md). Live results captured during implementation
are appended to `specs/001-supported-trade-ingestion/evidence/smoke.md` with the command, timestamp,
redacted URL, the JSON record, and the operator who ran it. Raw responses are never committed.
