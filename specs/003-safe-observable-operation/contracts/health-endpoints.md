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

- **Success Status**: `200 OK` (when PostgreSQL, Redis, and trade poller are healthy)
- **Failure Status**: `503 Service Unavailable` (when any required component is unhealthy, degraded, or terminated)
- **Response Headers**: `Content-Type: application/json`
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
    "events_per_second": 12.4,
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
    "last_error": null,
    "timestamp": 1788983725.5
  }
  ```

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
