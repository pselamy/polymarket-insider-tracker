# Interface Contract: Delivery Deduplication & Channel Semantics

**Feature**: `specs/003-safe-observable-operation`
**Date**: 2026-09-10

This contract governs the lifecycle of alert delivery, deduplication state, and channel outcomes.

---

## 1. Deduplication Separation Principle
- `RiskScorer` evaluates risk signals and determines `should_alert` (True if `weighted_score >= alert_threshold`). It MUST NOT check or set Redis deduplication keys.
- `AlertDispatcher` / `AlertHistory` evaluates deduplication state per channel and records delivery outcomes.

---

## 2. Delivery Identity & Key Format
- **Identity**: `channel` + normalized lowercase `wallet` + `market`
- **Key Pattern**: `alert:dedup:{channel}:{wallet}:{market}`
- **TTL**: `dedup_window_seconds` (default 3600 seconds)
- **Scope**:
  - Discord and Telegram have separate keys:
    - `alert:dedup:discord:0x123...:0xabc...`
    - `alert:dedup:telegram:0x123...:0xabc...`
  - Score changes, side changes, or repeated trade IDs for the same wallet/market do NOT generate new delivery identities.

---

## 3. Channel Outcomes & Retry Matrix

| Outcome | Channel Result | Redis Dedup Key | Retry Status | Assessment Disposition |
|---|---|---|---|---|
| **Dry Run** | Skipped | Not written | N/A | `dry_run` |
| **Confirmed Success** | HTTP 2xx | Written with TTL | Suppressed | `delivered` (or `partial_failure` if another channel failed) |
| **Confirmed Failure** | HTTP 4xx, 5xx, ConnRefused, or connect/pool timeout | Not written | Eligible immediately | `failed` (or `partial_failure`) |
| **Ambiguous Timeout** | Read/response timeout after the payload was sent | `alert:ambiguous:...` written (60s TTL) | Suppressed for 60s; then eligible (a duplicate remains possible) | `ambiguous` / `failed` |
| **Duplicate** | Key exists | Untouched | Suppressed | `duplicate` |
| **No Channels Configured** | No attempt possible | Not written | N/A | `no_channels` |

Connect and pool-acquisition timeouts occur before the payload leaves the process, so they
are confirmed failures and safe to retry. Read/response timeouts occur after the payload was
sent; the channel MUST NOT re-post internally and MUST surface the ambiguity (raise
`TimeoutError`) so the dispatcher applies the 60-second ambiguity window. Deduplication-state
reads or writes that fail (for example a Redis outage) degrade toward eventual delivery with
an explicit possible-duplicate warning; they never block an authorized delivery attempt.

---

## 4. Dry-Run Guarantees
When `dry_run=True`:
1. Network requests to Discord and Telegram endpoints MUST NOT occur.
2. Redis keys under `alert:dedup:*` and `alert:ambiguous:*` MUST NOT be created or modified.
3. Every qualifying assessment MUST still be created and persisted with `delivery_disposition="dry_run"` and `dry_run=True`.
