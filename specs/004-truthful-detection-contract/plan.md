# Implementation Plan: Truthful Detection Contract

**Branch**: `dispatch/polymarket-004-full-plan-20261017-3ae1b398` | **Date**: 2026-10-17 | **Spec**: [spec.md](spec.md)

**Status**: Phase0 complete; Phase1 design proposed. Implementation is not authorized.
**Baseline**: upstream main `f2e21672c9707c927befea8d82b1069171ae38f9`, re-checked against `112a733` (Protocol typing only), `0ddee0b` (callback rename only) `f3871cb` (Protocol stub bodies only) and `3d5ab59` (one unhashed `AlertHistory` property; see source-refresh) (refreshed 2026-10-19; the
original 2026-10-17 baseline was `46e41921ca06b701fece7a17953a666f6873b7e1`; scoring semantics are
unchanged between them, see [evidence/source-refresh-f2e2167.md](evidence/source-refresh-f2e2167.md)).
The existing A4 direction was approved; no new human answer or implementation approval is claimed.

## Summary

Make the existing two-group research monitor truthful about wallet age, usable daily volume, score
inputs, threshold equality and capability status. Preserve the003 lifecycle, assessment schema,
channel deduplication and reusable pipeline harness. No trading, new detector, backtest, real alert,
production deployment or new database migration belongs to this slice.

The reviewed114-case synthetic replay is preparation evidence, not calibration or an implementation.
Its two proposed operator overrides remain proposals. R01 unit qualification now has a recorded
disposition (see [research.md](research.md)): Gamma market `volume24hr` is interpreted as provider
USD-denominated monetary market volume over the provider's 24-hour window, grounded in the
reviewed first-party integration plus bounded current field/schema qualification. Claim limits
(archived example, no USDC-settlement/freshness/aggregation warranty, no total-volume fallback)
are preserved in research.

## Technical Context

**Language/Version**: CPython3.11–3.13, syntax baseline3.11.
**Dependencies**: existing Decimal, dataclasses, httpx, redis, Pydantic settings, Psycopg3/SQLAlchemy;
pytest, fakeredis and working boundary fakes. No new runtime dependency is proposed.
**Storage**: existing003 assessment table and Redis metadata cache. Explicit versioned extension of
`scoring_config` TEXT is proposed for immutable per-evaluation evidence; no evidence column is assumed.
**Testing**: regression-first detector/value contracts; existing003 real-pipeline harness; real
PostgreSQL/Redis parity and migrations; safe public-provider qualification.
**Platforms**: blocking Linux and supported Apple Silicon; no Python3.14 or Windows scope expansion.
**Performance**: preserve001 post-response and003 lifecycle bounds; metadata cannot hold ingestion
hostage; additive evidence collection must not duplicate provider fetches.
**Constraints**: research only; fixed defaultthreshold0.80; exactly two signal groups; no book/funding/
sniper score; no fabricated zero/age/freshness facts; all applicable quality and delivery gates.
**Scale/Scope**: existing monitoring path plus replay preparation and explicit compatibility notes.
**Source qualification**: R01 disposition recorded — Gamma `volume24hr` interpreted as provider
USD-denominated monetary market volume (reviewed first-party integration + bounded current
field/schema check). Local acquisition-age policy (stamp HTTP-page time, 0–600s rule) remains a
local recency rule, not provider measurement-time proof.

## Constitution Check

| Principle | Planning disposition | Implementation entry |
| --- | --- | --- |
| Research/monitoring only | Preserved in proposed design. | No trading, recommendations or real messages. |
| Source-grounded truth | Gamma market field exists with R01 monetary interpretation; archived-example limits recorded. | Bounded current field/schema check at implementation entry; no fallback or warranty claims. |
| End-to-end evidence | Existing003 harness retained; replay is only one layer. | Regression, persistence, rendering and live-safe evidence required. |
| Safe durable effects | Use immutable evidence and existing storage; no delivery changes. | Versioned003 storage-contract amendment and dry-run proof required. |
| Compatibility/reproducibility | Additive evaluation API and explicit semantic migration proposed. | Review public constructor, nullable impact and JSON-envelope changes before code. |
| Spec Kit/human ownership | Clarification scan precedes this research; domain checklist generated, reviewer-owned and unchecked. | Full tasks/analyze recorded below; reserved human validation remains mandatory. |

No constitution exception is requested. Phase1 design is proposed below.

## Phase1 Design

See [research.md](research.md) R02–R07 for full decisions. Concrete pins:

- **Acquisition provenance (revised 2026-10-19)**: stamp each successful Gamma HTTP page at body
  acquisition (`daily_volume_acquired_at`, aware UTC, source id) and carry it through the page tuples to
  `GammaMarketStats`; store it in a **separate per-market volume record** written by a Gamma loop that is
  decoupled from the CLOB crawl (the coupled loop's worst-case entry age is `2D + w + S − g`, 688 s with the
  reported D = 204 s; research R02a); usable only when the single precedence table of data-model §1.2
  yields `available` (`0 <= evaluated_at - acquired_at <= 600s`). Eight mutually exclusive states; never
  synthesize the stamp from cache reads, TTL, CLOB refresh, or generic `last_updated`.
- **Entry gate (revised 2026-10-19, codex round 2)**: task T027 (proxy-wallet nonce qualification) gates the
  wallet group. Before its receipt exists the wallet group is non-scoring (FR-007c): the scorer records
  `wallet.group_status = unqualified_pre_T027`, null wallet confidence, contribution 0, the group is not
  counted and the 1.2 bonus never applies, so the ceiling is 0.450. This is the contract's single default
  posture (D1b option G1, formerly O5); implementation tasks that let the wallet group score do not merge before the
  receipt. Opening the gate is a reviewed code change citing the receipt, not an environment knob.
- **Operator surface**: keep `DETECTOR_ALERT_THRESHOLD` default 0.80 (inclusive `>=`,
  0.001 quantum); add `DETECTOR_FRESH_TRANSACTION_BOUNDARY` default 5, positive integer and
  exclusive nonce bound — every classification rule uses the captured effective boundary
  `B`, with 5 only its default (boundary 3 excludes nonce 3, boundary 10 includes nonce 9;
  nonce exactly `B` never qualifies; see [data-model.md](data-model.md) §2) —
  and `DETECTOR_SIZE_VOLUME_THRESHOLD` default 0.02 (strict `>`). Existing wallet minimum
  notional 1000, age ceiling 48h, niche volume 50000 / niche-only minimum 500, confidence
  constants, and weight rules stay fixed.
- **Alert-path report (revised 2026-10-20, Claude round 3 B2)**: FR-022a is evaluated over the classes the
  configured live sources can produce, from a typed capability value whose `wallet_group_status` and
  `wallet_age_source` are code constants (`unqualified_pre_T027`, `none` on main), so it fires on the
  shipped configuration (ceiling 0.450 < 0.80). One pure function computes the ceiling through the real
  scorer combination; T028 regenerates `evidence/reachability.md` with it, so the runtime never reads a
  test table. Surfaces: a startup log record beside `pipeline.py:407-412`, an `Alert Path:` line in
  `print_config_summary` (`__main__.py:173-189`, also under `--config-check`), and an additive,
  informational `alert_path` object in `/health` (`ingestor/health.py:658-679`) that does not change any
  health verdict, status code or probe (T029).
- **Wallet identity and age (revised 2026-10-19)**: the profiled address is the V2 `proxy_wallet`
  (FR-007a, nonce meaning unverified); live age is unknown until DECISION D1a selects a qualified
  source (FR-007b, research R09; recommended default O1); unknown age is exactly 0.50 everywhere.
- **Wallet/size semantics**: known-age requires notional ≥ 1000, `nonce < B` (effective
  boundary), age ≤ 48h; unknown-age low-activity requires notional ≥ 1000, `nonce < B`,
  confidence exactly 0.50, no age/new-wallet/large-trade additions; missing retrieval
  supplies no freshness fact. The canonical freshness rule in
  [data-model.md](data-model.md) §2a governs signal JSON, assessment evidence, cached
  profiles, and persisted wallet rows coherently, with the legacy
  `profiler/analyzer.py:223–242` unknown-age-`True` path named as an explicit compatibility
  exception. Usable positive current volume
  enables notional/volume ratio; unavailable/nonpositive/stale/invalid volume yields no
  observed ratio; unavailable volume may use category fallback with basis recorded. Book
  depth stays unavailable in the live core.
- **Evidence**: additive evaluation entry point sharing one acquisition/calculation with
  `analyze`; immutable wallet/market evaluation evidence (including known-nonqualifying
  inputs) flows through bundle → assessment → persistence → formatting. Effective threshold
  captured at assessment time.
- **Storage**: explicit versioned amendment of the shared 003 contract — preserve root keys,
  add `detector_config`, versioned `evaluation_evidence`, evidence-schema identity
  (proposed algorithm/evidence identity `004.1`); keep `RiskScorer.scoring_config` as pure
  effective configuration; retain `multi_signal_bonus_3` as documented-neutral compatibility
  key with the unreachable branch removed. No competing migration or harness.
- **Compatibility**: new exclusive `fresh_transaction_boundary` keyword coexists with the
  deprecated inclusive `max_nonce` alias (omitted → 5; alias-only → `B = N + 1` with
  warning; conflicts rejected) per [contracts/detector-api.md](contracts/detector-api.md);
  unavailable impacts never serialize as measured zero via additive nullable
  `volume_impact_measured`/`book_impact_measured` companions (required-float legacy fields
  and wire positions unchanged);
  same quantized score/threshold on every output; niche as size modifier; research wording kept.

See [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md),
[tasks.md](tasks.md), and [evidence/implementation-validation.md](evidence/implementation-validation.md).
Reviewer-owned domain checklist
[checklists/domain-requirements.md](checklists/domain-requirements.md) is generated unchecked;
human validation and implementation remain reserved.

## Proposed Structure

Keep the existing single-package layout. Code ownership: detector evaluation/value modules, immutable evidence through `SignalBundle`/`RiskAssessment`, per-page Gamma acquisition
provenance, configuration wiring, persistence projection and existing formatters. The full files,
contracts, data model, validation guide and task decomposition belong to Phase1 after research closes.

## Complexity Tracking

No broad refactor is authorized by this plan. The separately tracked Relentless goal requires full
source enrollment and bounded remediation. Planning prototypes are not automatically production
modules or qualified controls. No suppressions, weaker types, baselines or exclusions may substitute
for that work.
