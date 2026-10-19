# Slice 004 Human Decision Packet: revised proposal (not approved)

**Date**: 2026-10-17, rewritten 2026-10-19 after the codex and agy second opinions (both DO NOT
APPROVE; triage in [evidence/revision-2026-10-19.md](evidence/revision-2026-10-19.md): 27 findings,
22 confirmed, 5 partly confirmed, 0 rejected whole).

**Status, stated plainly**: this contract is **not approved**. Nothing here records an approval,
waiver or acceptance. The reviewer-owned [domain checklist](checklists/domain-requirements.md) is
entirely unchecked (23 items) and stays that way until Patrick or a designated reviewer decides.
No product code, test, script, migration or CI file changed; no network call was made for this
revision. Implementation is not authorized. Scope of any eventual approval: the reviewed implementation
contract only, not a merge, deployment, real notification, trading or calibration claim.

## Open decisions at a glance

| ID | Question | Recommended default | If you pick otherwise |
|---|---|---|---|
| **D1** | Wallet-age source (current main has none) | **O1**: age branch non-operational, score only on low-activity evidence; O2 next after a qualification receipt | O2/O3 add a source task and a qualification receipt; wording rules already in spec |
| **D2** | What 0.80 means when age is unknown (ceiling 0.780) | **R0**: keep 0.80 and exact 0.50, publish ceilings | R1 changes every threshold surface and the journeys; R3 needs a new spec |
| **D3** | Category-only niche signal when volume is unusable | **(c)** only for a category from real metadata | (a) keeps invented evidence; (b) silences the size group on any outage |

**Consequence to accept knowingly**: with D1 = O1 and D2 = R0 the default configuration records
assessments but **no assessment can qualify as an alert** (unknown-age ceiling 0.780 < 0.80). An
operator can still lower `DETECTOR_ALERT_THRESHOLD`; the value is recorded per assessment.

## DECISIONS FOR PATRICK (open; none is settled by this revision)

### D1 — Wallet-age source (DECISION FOR PATRICK)

Current main cannot produce a known wallet age (`profiler/chain.py:485-532` returns no first
transaction for any wallet), and the profiled address is the V2 `proxy_wallet`, whose nonce meaning is
**unverified**. Full analysis: [research.md](research.md) R09.

| Option | Live behaviour | Main trade-off |
|---|---|---|
| **O1** age branch non-operational; score only on low-activity evidence | every wallet candidate is "low-activity wallet, age unknown", confidence exactly 0.50 | Truthful, no new dependency; maximum score is 0.78 (see D2) |
| **O2** Polymarket-activity age from the earliest public trade for the proxy address | "first Polymarket trade seen < 48 h ago" when full history is reachable, else unknown | Measures newness to Polymarket without the nonce question; needs a qualified per-user query, offset-capped so often only a lower bound |
| **O3** external indexer for chain first-transaction age | "first on-chain transaction < 48 h ago" | Vendor, credential/quota and cost; still subject to the proxy-address question |

**Recommended default: O1 for slice 004**, O2 as the first follow-up after the T027 qualification
receipt. If you pick O2 or O3 the spec already has the wording rules and the unknown-age fallback; the
implementation then gains a source task. Until you choose, the contract is written for O1 and says so.

### D2 — What the 0.80 threshold means when age is unknown (DECISION FOR PATRICK)

Recomputed by `evidence/reachability.py` ([research.md](research.md) R10): with age unknown the
highest attainable score is **0.780 for every trade size**; markets with ≥ $50k volume top out at
0.45 (unknown age) or 0.642 (best-case known age); main today (003.1 rules, volume never passed) tops
out at 0.528. The checked-in 114-case replay's "0 → 15 qualifications" is entirely known-age cases.
Unknown-age live-realistic replay: 0 of 40 qualify at 0.80, 7 at 0.78.

| Option | Consequence | Main trade-off |
|---|---|---|
| **R0** keep 0.80 and exact-0.50; publish the ceilings | with D1 = O1 the monitor records assessments but nothing qualifies by default; `DETECTOR_ALERT_THRESHOLD` remains the explicit, per-assessment-recorded override | no unsupported calibration claim; no deliverable alerts out of the box |
| **R1** lower the default to ≤ 0.78 | saturated niche unknown-age cases alert | changes what an alert means with no outcome data; the scorer comment's "cost-adjusted backtest" justification has no reproducible evidence in the repo (G-028) |
| **R2** unknown-age base ≥ 0.541 | keeps 0.80 reachable | unknown age would outscore a known 24 h wallet; breaks FR-008 |
| **R3** two tiers (alert 0.80 + lower "research watch") | surfaces the 0.78 class without calling it an alert | new product surface; needs its own spec |
| **R4** re-weight / remove niche double counting | changes every ceiling | model change needing calibration data |

**Recommended default: R0.** It is the only option that adds no unvalidated claim, but you should
decide knowingly that with D1 = O1 the default configuration produces no deliverable alert.

### D3 — Category fallback when volume is not usable (DECISION FOR PATRICK)

When the volume state is anything but `available`, today's code can still emit a niche-only size signal
(confidence 0.2) from the market category alone (`detector/size_anomaly.py:282,293-300`). On a metadata
lookup failure the code even invents category `"other"`, which counts as niche-prone
(`:29,205-221`). Reviewers (codex, agy) both flagged this as a false-positive source.

| Option | Effect on an unknown-age trade with no usable volume | Trade-off |
|---|---|---|
| (a) keep as today | niche-prone category or failed lookup ⇒ size 0.2 ⇒ score 0.384 | preserves 003.1 behaviour; a lookup failure is scored as niche evidence |
| (b) no category fallback; size signal needs measured volume | no size signal ⇒ score 0.200 | most conservative; every volume outage silences the size group |
| **(c) fallback only for a category from real metadata (recommended)** | real `science/tech/finance/other` ⇒ 0.384; failed lookup or other categories ⇒ 0.200 | removes the invented evidence, keeps the documented heuristic |

## What changed since the 2026-10-17 packet

| Area | 2026-10-17 | 2026-10-19 revision | Finding(s) |
|---|---|---|---|
| Base | main `46e4192` | main `1bdccbc`; hashes/lines refreshed, check script | F25 |
| V2 ingestion | "separate pending request, untouched" | live contract; wallet subject is `proxy_wallet`; journey suites in scope (T021–T026) | F02, F24 |
| Wallet age | assumed available | none on main; FR-007a/b, basis recorded, D1 | F01, F02 |
| Unknown-age confidence | "capped at" and "exactly" 0.50 | exactly 0.50 everywhere (single table) | F26 |
| Threshold reachability | not examined | table + script + 40 live-realistic replay cases; ceiling 0.780; D2 | F03, F04, F05 |
| Volume freshness | per-page stamp carried through Redis by the coupled loop | arithmetic shows 688 s worst case; decoupled Gamma loop, own volume record, miss path cannot poison | F07, F08, F09, F10 |
| Volume states | seven, overlapping, 3 fields | eight, one precedence table P1–P8, zero ⊂ `nonpositive`, one replay case per state/reason | F11, F13, F15 |
| Raw value | "byte-preserved" | decoded JSON value preserved semantically, 256-byte bound | F12 |
| 004.1 envelope | prose list | JSON Schema + example + failure-row + checker; reader dispatch; stated as a semantic amendment of the 003 field | F16, F17, F23 |
| Detector API | pins only | evaluation clock, alias table, explicit-volume rule, `Decimal \| None` companions, formatter line | F18–F21 |
| Task graph | T012 depended on T011 | T012a–d independent; T021–T028 added | F14, F24 |

## Spec choices made in this revision (visible, not decisions you must make)

- Gamma volume is refreshed by its own loop (default `S_g = 180 s`) and stored in its own record
  (research R02a option A). Option B (keep the coupling, widen the recency window to ~900 s) is
  documented; widening the window would be a product-semantics change and is yours to choose.
- A negative `max_nonce` raises `ValueError` instead of building a detector that can never fire.
- The threshold lives only in the root `alert_threshold` of the envelope; `multi_signal_bonus_3` is the
  inert string `"1"` in 004.1 documents (`"1.3"` stays in 003.1 rows).
- `SizeAnomalySignal` keeps its required floats; authoritative values are the new `Decimal | None`
  companions.

## Final behavior (if authorized, under the recommended defaults)

Two-group research core (low-activity wallet + size anomaly; niche as size modifier). Live wallet
evidence is "low-activity wallet, age unknown" on the **proxy address**, confidence exactly 0.50, no
bonus of any kind. Threshold 0.80 inclusive; effective exclusive boundary `B` (default `< 5`; nonce
exactly `B` never qualifies); strict volume ratio `> 0.02`; eight mutually exclusive volume states,
only `available` yields a ratio, never a measured zero; Gamma stamps at response-body receipt with a
0–600 s local recency rule that the decoupled loop can meet; one evaluation instant per trade;
immutable evaluation evidence in the versioned `004.1` document; additive evaluation API with the
deprecated inclusive alias; no new migration.

## Evidence and limitations

- 114-case / 228-score / 63-delta behavioural replay is unchanged and still valid as an
  old-versus-proposed comparison; its 0 → 15 qualifications are **all known-age** cases the live
  runtime cannot produce. The 40-case live-realistic addendum shows 0 qualifying at 0.80 and 7 at 0.78.
  Neither is calibration.
- Source hashes: 4 of 7 recorded hashes unchanged, 3 changed by refactors only
  (`evidence/source-refresh-1bdccbc.md`).
- R01 monetary-volume interpretation rests on a reviewed first-party integration (archived
  2025-12-22 example, not a current warranty; no USDC-settlement/freshness/aggregation claims; no
  total-volume fallback; bounded current schema check still required at implementation entry).
- Reproduce: `python3 evidence/reachability.py --check evidence/reachability.md`,
  `python3 evidence/replay_live.py --check evidence/replay-live/results.csv`,
  `python3 evidence/check_envelope.py`, `python3 evidence/check_source_hashes.py`.

## Remaining known limits

- **UNVERIFIED**: proxy-address nonce semantics; the per-user trades query for age option O2; the
  204 s CLOB crawl duration on current data (reported value, not re-measured); Gamma crawl duration `g`.
  No live call was made; task T027 is the qualification receipt and needs authorization.
- No outcome-based calibration exists. The scorer's "cost-adjusted backtest" justification of 0.80
  (`detector/scorer.py:25-31`) cannot be reproduced in this repository (G-028).
- Niche is double counted (effective weight 0.60 on size confidence in niche markets) and low-volume
  markets mechanically inflate the ratio; unchanged by this slice (D2/R4).
- The reviewer-owned checklist text still says "seven" states (CHK003) and "cap 0.50" (CHK005); it is
  left untouched and is read against the eight-state table and the exactly-0.50 rule.
- A 003 consumer that rejects unknown keys in `scoring_config` breaks on 004.1 rows (named risk).

## Exact scope to authorize

Approve or reject the contract in spec/plan/research/data-model/contracts/tasks **after** deciding
D1–D3. Do not decide whether a factual provider claim is true — evidence category and limits are
stated above.
