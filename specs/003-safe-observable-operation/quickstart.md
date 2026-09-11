# Quickstart Guide: Safe Observable Operation

**Feature**: `specs/003-safe-observable-operation`
**Date**: 2026-09-10

This guide walks through verifying the safe, observable operation features implemented in slice 003.

---

## 1. Prerequisites & Setup

Ensure the environment is synchronized and local dependencies are running:

```bash
uv sync --locked --all-extras --python 3.13
cp .env.example .env
```

Start local Redis and PostgreSQL services if not already active:
```bash
docker compose up -d postgres redis
```

---

## 2. Offline Configuration Validation

Run `--config-check` to verify offline syntax and shape checking:

```bash
uv run python -m polymarket_insider_tracker --config-check
```

**Expected Outcome**:
- Validates environment variables and types.
- Displays redacted configuration summary.
- Clarifies that offline checks passed and runtime readiness requires reachable services.
- Exits with status `0`.

---

## 3. Running with Health Server & Dry-Run Mode

Run the tracker in safe dry-run mode with a custom health port:

```bash
uv run python -m polymarket_insider_tracker --dry-run --health-port 8088
```

In another terminal, inspect the health endpoints:

### Liveness Probe
```bash
curl -s http://127.0.0.1:8088/live
# {"live": true}
```

### Readiness Probe
```bash
curl -s http://127.0.0.1:8088/ready
# {"ready": true, "components": {"database": "up", "redis": "up", "ingestion": "up"}}
```

### Detailed Health Report
```bash
curl -s http://127.0.0.1:8088/health | jq .
```

### Prometheus Metrics
```bash
curl -s http://127.0.0.1:8088/metrics
```

---

## 4. Deterministic End-to-End Verification

Execute the deterministic end-to-end suite:

```bash
uv run pytest tests/integration/test_end_to_end.py -v
```

**Expected Outcome**:
- Exercises the full flow: Trade Ingestion -> Profile -> Detect -> Score -> Persist Assessment -> Fake Dispatch.
- Confirms zero calls to Discord/Telegram and zero Redis dedup keys written in dry-run mode.
- Confirms graceful shutdown without orphaned workers.

---

## 5. Storage Migration Verification

Verify the new `003_safe_observable_operation` migration round-trip:

```bash
uv run --env-file .env python scripts/runtime_services.py --phase migrations
```

**Expected Outcome**:
- Executes `003_safe_observable_operation` upgrade on temporary database.
- Downgrades back to `002_risk_assessments`.
- Re-upgrades to head.
- Executes async query and cleans up temporary database.
