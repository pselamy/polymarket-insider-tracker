# Interface Contract: Health & Observability Endpoints

**Feature**: `specs/003-safe-observable-operation`
**Date**: 2026-09-10

The HTTP health server is exposed on `0.0.0.0:<port>`, where `<port>` defaults to `8080` and is configurable via `HEALTH_PORT` or `--health-port`.

---

## 1. `GET /live`
**Purpose**: Liveness probe for process orchestrators (e.g. Kubernetes). Indicates only that the process event loop and HTTP server are responsive.

- **Status Code**: `200 OK`
- **Response Headers**: `Content-Type: application/json`
- **Response Body**:
  ```json
  {
    "live": true
  }
  ```

---

## 2. `GET /ready`
**Purpose**: Readiness probe indicating whether the pipeline is prepared to ingest and process trades.

- **Success Status**: `200 OK` (no required component is `down`; FR-002 fails readiness only for an unavailable dependency, a terminally failed ingestion worker, or blocked progress)
- **Failure Status**: `503 Service Unavailable` (when any required component is `down`)
- **Response Headers**: `Content-Type: application/json`
- **Component status values**: `up`, `degraded`, `down`. A recoverable ingestion condition (`IngestionState.DEGRADED` or `POSSIBLE_DATA_LOSS`) is reported as `degraded`: the source is still reachable and progressing, so readiness holds, but the state is never hidden as `up`. Its error detail appears in `/health`.
- **Response Body (Success)**:
  ```json
  {
    "ready": true,
    "components": {
      "database": "up",
      "redis": "up",
      "ingestion": "up"
    }
  }
  ```
  (`"ingestion": "degraded"` may appear here while ready remains true.)
- **Response Body (Failure)**:
  ```json
  {
    "ready": false,
    "reason": "ingestion_worker_failed",
    "components": {
      "database": "up",
      "redis": "up",
      "ingestion": "down"
    }
  }
  ```
  `reason` is `ingestion_worker_failed` for a down ingestion worker and `<component>_unreachable` for a down dependency.

---

## 3. `GET /health`
**Purpose**: Detailed diagnostic report for operators. Distinguishes polling acquisition freshness from trade event arrival.

- **Status Code**: `200 OK` if `status` is "healthy" or "degraded"; `503 Service Unavailable` if "unhealthy".
- **Response Headers**: `Content-Type: application/json`
- **Response Body**:
  ```json
  {
    "status": "healthy",
    "uptime_seconds": 3600.5,
    "last_acquisition_time": 1788983721.0,
    "last_trade_time": 1788983715.0,
    "acquisition_freshness_seconds": 4.5,
    "trade_freshness_seconds": 10.5,
    "total_events_received": 1420,
    "total_events_per_second": 12.4,
    "components": {
      "database": {
        "status": "up",
        "latency_ms": 1.2,
        "last_error": null
      },
      "redis": {
        "status": "up",
        "latency_ms": 0.5,
        "last_error": null
      },
      "ingestion": {
        "status": "up",
        "latency_ms": null,
        "last_error": null
      }
    },
    "streams": {
      "trades": {
        "status": "active",
        "events_received": 1420,
        "events_per_second": 12.4,
        "last_event_time": 1788983715.0,
        "last_error": null
      }
    },
    "last_error": null,
    "timestamp": 1788983725.5
  }
  ```

The top-level `last_error` reports the pipeline's most recent worker or per-trade processing
error (null when none has occurred). Per-trade processing failures are counted and surfaced
here without failing `/ready` on their own; see the pipeline-lifecycle contract for their
acknowledgment semantics.

Component and overall degradation semantics:

- A `degraded` component (recoverable acquisition failure or possible-data-loss) carries its
  `last_error` (or `ingestion state: <state>` when the state itself is the condition) and makes
  the overall `status` at least `"degraded"` while the response stays `200`.
- A `down` component makes the overall `status` `"unhealthy"` (`503`).
- Stream staleness is based on the freshest evidence of source progress — a received trade
  **or** a successful acquisition — so a quiet market with current acquisitions stays
  `active`/healthy (US1 scenario 2). A stream goes `stale` only when both trade arrival and
  acquisition age past the staleness threshold.

---

## 4. `GET /metrics`
**Purpose**: Prometheus metrics exposition endpoint.

- **Status Code**: `200 OK`
- **Response Headers**: `Content-Type: text/plain; version=0.0.4; charset=utf-8`
- **Response Body**: Standard Prometheus format containing:
  - `polymarket_events_total`
  - `polymarket_events_per_second`
  - `polymarket_stream_status`
  - `polymarket_last_event_timestamp`
  - `polymarket_health_status`
