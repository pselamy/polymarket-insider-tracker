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
| **Confirmed Success** | HTTP 2xx | Written with TTL | Suppressed | `delivered` (or `partial_failure` if another channel failed or is unknown) |
| **Confirmed Failure** | HTTP 4xx, 5xx, ConnRefused, or connect/pool timeout | Not written; claim released | Eligible immediately | `failed` (or `partial_failure`) |
| **Ambiguous Timeout** | Read/response timeout after the payload was sent | `alert:ambiguous:...` retained (60s TTL) | Suppressed for 60s; then eligible (a duplicate remains possible) | `ambiguous` / `partial_failure` |
| **Suppressed Unknown** | Ambiguity window or concurrent in-flight claim active | Untouched | Suppressed until the 60s key expires | `ambiguous` (or `partial_failure` when another channel delivered); never counted as success |
| **Duplicate** | Key exists | Untouched | Suppressed | `duplicate` |
| **No Channels Configured** | No attempt possible | Not written | N/A | `no_channels` |

Connect and pool-acquisition timeouts occur before the payload leaves the process, so they
are confirmed failures and safe to retry. Read/response timeouts occur after the payload was
sent; the channel MUST NOT re-post internally and MUST surface the ambiguity (raise
`TimeoutError`) so the dispatcher applies the 60-second ambiguity window. Deduplication-state
reads or writes that fail (for example a Redis outage) degrade toward eventual delivery with
an explicit possible-duplicate warning; they never block an authorized delivery attempt.

### Atomic in-flight claim with ownership and a bounded send

The `alert:ambiguous:{channel}:{wallet}:{market}` key doubles as the per-identity in-flight
send claim. Before a send attempt the dispatcher acquires it atomically (`SET NX EX
claim_ttl`, default 60 s) with a **unique ownership token** as the value, re-checks the dedup
key under the claim, and only then contacts the channel:

- claim not acquired → the attempt is suppressed as an unknown outcome (`ambiguous_timeout`);
- confirmed success → the dedup key is written first, then the claim is released;
- confirmed failure (or open circuit breaker) → the claim is released so retry is immediate;
- ambiguous outcome → the key is refreshed to the full claim window (ownerless) and retained.

Two guarantees make the claim safe across lease expiry:

1. **Proven send-time bound.** Every channel send runs under `send_deadline_seconds`
   (default 45 s), which the dispatcher requires to be strictly smaller than
   `claim_ttl_seconds`. A send cut off at the deadline is recorded as ambiguous (the payload
   may already have been accepted) and the ambiguity window is retained, so no send attempt
   can ever outlive its claim and two concurrent dispatches of one identity can never both
   be sending.
2. **Compare-and-delete release.** Release happens only while the stored value still equals
   this dispatch's ownership token (`WATCH`/`MULTI` transaction). A claim that expired and
   was re-acquired by another dispatch is never deleted by the stale owner. A stale owner's
   ambiguous refresh may overwrite a newer claim; that direction only extends suppression
   and can never cause a duplicate delivery.

Claim errors degrade toward delivery (a duplicate is possible) exactly like other
dedup-state failures. Aggregate classification never counts a suppressed-unknown channel as
delivered: `all_succeeded` requires every counted channel to be a confirmed success, and any
unknown outcome downgrades a delivered aggregate to `partial_failure`.

---

## 4. Dry-Run Guarantees
When `dry_run=True`:
1. Network requests to Discord and Telegram endpoints MUST NOT occur.
2. Redis keys under `alert:dedup:*` and `alert:ambiguous:*` MUST NOT be created or modified.
3. Every qualifying assessment MUST still be created and persisted with `delivery_disposition="dry_run"` and `dry_run=True`.
