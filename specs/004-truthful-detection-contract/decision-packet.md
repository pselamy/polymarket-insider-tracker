# Slice 004 Human Decision Packet: revised proposal (not approved)

**Date**: 2026-10-17, rewritten 2026-10-19 after the codex and agy second opinions (both DO NOT
APPROVE; triage in [evidence/revision-2026-10-19.md](evidence/revision-2026-10-19.md): 27 findings,
22 confirmed, 5 partly confirmed, 0 rejected whole).

**Status, stated plainly**: this contract is **not approved**. Nothing here records an approval,
waiver or acceptance. The reviewer-owned [domain checklist](checklists/domain-requirements.md) is
entirely unchecked (23 items) and stays that way until Patrick or a designated reviewer decides.
No product code (`src/`), test (`tests/`), migration (`alembic/`), quality-gate (`quality/`) or CI file
changed. The one change outside `specs/` is the evidence tooling this packet relies on,
`scripts/spec004_evidence/*` (stdlib only, no `src/` import, inside every Python gate; see "Remaining
known limits"). No network call was made for this revision other than git/GitHub for this repository. Implementation is not authorized. Scope of any eventual approval: the reviewed implementation
contract only, not a merge, deployment, real notification, trading or calibration claim.

## Open decisions at a glance

| ID | Question | Recommended default | If you pick otherwise |
|---|---|---|---|
| **D1a** | Wallet-age source (current main has none) | **O1** (none) for this slice; O2 or O6 as the first follow-up | O2/O3/O6 add a source task and a qualification receipt; O4 is an unverified lead |
| **D1b** | May nonce or activity evidence score before qualification? | **G1** (former O5): wallet group non-scoring until T027 (FR-007c) | G2 scores an unvalidated proxy-nonce heuristic; G3 needs D1a = O6 |
| **D2** | What 0.80 means while nothing live can reach it (pre-T027 ceiling 0.450; post-T027 unknown-age ceiling 0.780) | **R0**: keep 0.80; the default configuration cannot alert, an operator threshold override still can, and FR-022a/T029 report `default alert path unreachable` | **R5** makes "no alert" a guarantee: delivery structurally suppressed at every threshold (FR-022b, T030); R1 to 0.78 still yields zero alerts pre-T027; R3 needs a new spec |
| **D3** | Category-only niche signal when volume is unusable | **(c)** only for a category from real metadata (written default; both round-4 reviewers chose **(d)**, see D3) | (a) keeps invented evidence; (b) silences the size group on any outage; (d) also drops the fallback when Gamma did report a value |
| **D4** | Which positive volume values are usable at all (added round 4) | **M1**: `10^-6 <= v < 10^15` and ≤ 32 plain-notation characters, else `invalid`/`out_of_range` | M0 has a reachable crash/memory path (`"1e999999999"`); M2 caps the stored ratio instead |

**Consequence to accept knowingly**: in the default pre-T027 posture only the size group scores, so the
highest attainable score of **any** trade is **0.450** (`0.75 × (0.35 + 0.25)`, no two-group bonus).
The default configuration records assessments but **no assessment can qualify as an alert**, and
lowering the threshold to 0.78 changes nothing; only a threshold ≤ 0.45 would let a size-only signal
alert. After T027 qualifies the wallet group, the unknown-age ceiling is 0.780, still below 0.80. Under
R0 an operator can still lower `DETECTOR_ALERT_THRESHOLD`, and a qualifying assessment is then delivered
normally (the value is recorded per assessment); under R5 nothing is delivered at any threshold.

**Round-4 reviewer choices (input, not decisions)**:

| | D1a | D1b | D2 | D3 | D4 |
|---|---|---|---|---|---|
| codex round 4 | O6 after qualification (O1 behaviour until then) | G1 | R0 for this slice | (d) | not yet reviewed (added after round 4) |
| Claude round 4 | O1 now, O6 as the named follow-up | G1 | R5 (would fold R0 into it) | (d) | not yet reviewed (raised the N3 crash it answers) |
| this packet | O1; O6 (or O2) as follow-up | G1 | R0 | (c) written; (d) favoured by both reviewers | M1 |

## DECISIONS FOR PATRICK (open; none is settled by this revision)

### D1a — Wallet-age source (DECISION FOR PATRICK)

Current main cannot produce a known wallet age (`profiler/chain.py:485-532` returns no first
transaction for any wallet), and the profiled address is the V2 `proxy_wallet`, whose nonce meaning is
**unverified**. Full analysis: [research.md](research.md) R09. D1a only matters once the wallet group may
score (D1b); it does not change the pre-T027 ceiling 0.450. **Under the recommended D1b (G1), the D1a
choice has no effect on what slice 004 does**: it decides only what the first follow-up builds.

| Option | Live behaviour | Main trade-off |
|---|---|---|
| **O1** no age source; age branch non-operational | every wallet candidate is "low-activity wallet, age unknown", confidence exactly 0.50 | Truthful, no new dependency; maximum score 0.780 once the wallet group scores (see D2) |
| **O2** Polymarket-activity age from the earliest public trade for the proxy address (per-user Data API query) | "first Polymarket trade seen < 48 h ago" when full history is reachable, else unknown | Measures newness to Polymarket without the nonce question; needs a qualified per-user query, newest-first with a 10,000-row offset cap, so often only a lower bound (then `unknown`) |
| **O3** external indexer for chain first-transaction age | "first on-chain transaction < 48 h ago" | Vendor, credential/quota and cost; still subject to the proxy-address question |
| **O4** Gamma public-profile style account-creation timestamp | "account created < 48 h ago" as Polymarket records it | **Unverified lead only** (research R09 O4): no endpoint or terms evidenced in the repo; not recommendable until qualified |
| **O6** O2's per-user query used for **both** age and a **Polymarket trade count** as the low-activity measure (added after the round-3 Claude review) | "first Polymarket trade seen < 48 h ago" and "fewer than `B` public Polymarket trades seen", each from the same response; nonce recorded as an observation only | Replaces the unqualified nonce on the activity side too, so the wallet group no longer depends on proxy-nonce semantics; low counts are exactly the case the 10,000-row cap does not truncate. Costs: the same query qualification as O2, plus whether `takerOnly` or maker visibility undercounts trades (an undercount would *create* false "low activity"); changes what FR-007/FR-009's activity count means and needs its own reviewed spec text. **UNVERIFIED**: no live call was made |

**Recommended default: O1 for slice 004** (the only option whose every claim is supported by code on
main today), with **O2 or O6 as the first follow-up** once a bounded, authorized qualification
establishes the per-user query semantics (T027's optional per-user scope). If you pick O2, O3 or O6 the
spec already has the wording rules and the unknown-age fallback; the implementation then gains a source
task, and O6 also gains an activity-measure task.

### D1b — May nonce or activity evidence score before qualification? (DECISION FOR PATRICK)

This is a scoring-safety posture, not an age source; it composes with any D1a option. The earlier
packet listed it as D1 option O5 beside the age sources; both round-3 reviewers objected that this
blurred two independent choices.

| Option | Pre-qualification behaviour | Main trade-off |
|---|---|---|
| **G1 (= former O5; the spec's default posture)** wallet group non-scoring until T027 qualifies what the activity measure counts (FR-007c) | size group alone scores; wallet classification recorded with `group_status = unqualified_pre_T027`, never as scoring evidence | Strongest truthfulness; every ceiling drops to the size-group-only maximum 0.450 (`evidence/reachability.md`, pre-T027 table; replay columns `*_pre_T027`) |
| **G2** let the proxy-address nonce score now at exactly 0.50 (the earlier "O1 ungated") | wallet group scores as "low transaction count on the proxy address"; ceilings are the post-T027 rows (0.780 unknown age) | Accepts an unvalidated heuristic: if relayers submit transactions the nonce may stay low for active traders and every large trader looks "low activity" |
| **G3** score only on an activity measure that does not need the nonce (requires D1a = O6) | wallet group non-scoring until the per-user query is qualified, then scores on the Polymarket trade count; the nonce is never scored | Removes the proxy-nonce question entirely; T027 becomes a query qualification instead of a nonce one; needs O6's spec text and source task before any wallet scoring |

**Recommended default: G1 (O5 until T027).** The earlier wording "O1 gated by T027" was G1 in effect and
is withdrawn: before the T027 receipt the wallet group is non-scoring and the spec, schema,
reachability table and replay are written for that posture. Choosing G2 is an explicit choice to accept
an unvalidated heuristic; choosing G3 moves the wallet group's activity measure to D1a O6.

**What opens the gate (added after codex round 4, blocker 2)**: T027 now has a fixed, objective
acceptance rule, `research.md` R09a. Part A samples 200 proxy addresses from the ≥ $1,000-trade frame,
uses the supported all-participant ingestion as ground truth over ≥ 20 h of proven coverage, and passes
only if the nonce moved for ≥ 95 % of addresses that traded and stayed ≤ 5 for ≤ 2 % of addresses with
more than five trades that day, overall and in each contract/EOA stratum. Part B (only for O2/O6/G3)
qualifies the per-user query (recall ≥ 0.99, ordering, truncation, stability). **FAIL or INCONCLUSIVE
keeps the gate closed**, the nonce is never scored, and the recommended path becomes G3 with D1a O6; if
Part B also fails, D1a/D1b come back to you. The thresholds (N = 200, 0.95, 0.02) are spec choices you
may tighten or loosen; they are not presented as settled facts about provider behaviour.

**Reviewer positions on D1a/D1b (input, not decisions)**:

- *codex round 2*: scoring a proxy-wallet nonce as "low activity" before T027 says more than it
  measures; hence T027 is an entry gate (FR-007c).
- *codex round 3*: D1a **O2 after qualification**, with age `unknown` whenever history is truncated or
  maker visibility is incomplete; D1b **G1 (O5) until then**. Says "O1 plus a gate" is not one
  recommendation: before qualification it is O5, afterwards O1.
- *Claude round 3*: D1b **G1 (O5) now**, then D1a **O6** (O2's query for age plus a Polymarket trade
  count) once T027 qualifies the query; would not pick plain O1, because scoring an unqualified nonce at
  0.50 is what the gate exists to prevent.
- *codex round 4*: D1a **O6 after qualification**, with O1 behaviour until then; D1b **G1**.
- *Claude round 4*: D1a **O1 for 004**, naming **O6** (not "O2 or O6") as the follow-up, because O6 also
  replaces the unqualified nonce on the activity side and is the only path to a wallet group that does not
  depend on what T027 finds; D1b **G1**, and calls G2's relayer risk accurately stated. Both round-4
  reviewers therefore converge on G1 now and O6 next; they differ only in whether to call the interim
  "O1" or "O6 pending qualification", which under G1 has no behavioural difference.

### D2 — What the 0.80 threshold means while nothing live can reach it (DECISION FOR PATRICK)

Recomputed by `scripts/spec004_evidence/reachability.py` ([research.md](research.md) R10,
[evidence/reachability.md](evidence/reachability.md)) for both wallet postures:

| Posture | Highest attainable score | Where |
|---|---|---|
| **Default, pre-T027** (wallet non-scoring, FR-007c) | **0.450** for every trade size (`0.75 × (0.35 + 0.25)`, no bonus) | niche market with usable volume, ratio ≥ 0.06; non-niche 0.175; no usable volume 0.120 (niche-prone category) or 0.000 |
| Post-T027, unknown age | 0.780 for every trade size | same niche case with the 1.2 two-group bonus; non-niche 0.450 |
| Post-T027, known age (needs a D1a age source; none on main) | 0.972 | best case, niche, > $10k; non-niche tops out at 0.642 |
| Main today (003.1 rules, volume never passed) | 0.528 | |

The checked-in 114-case replay's "0 → 15 qualifications" is entirely known-age cases. Live-realistic
replay (64 cases): pre-T027, 0 qualify at 0.80 and 0 at 0.78 (maximum 0.450); post-T027, 0 at 0.80 and
10 at 0.78.

| Option | Pre-T027 (default posture) | Post-T027 | Main trade-off |
|---|---|---|---|
| **R0** keep 0.80 and exact-0.50; publish the ceilings; delivery path unchanged | at the default nothing qualifies (0.450 < 0.80); an operator override ≤ 0.450 qualifies size-only signals and **delivers them** (subject to `--dry-run`); FR-022a reports `unreachable` at the default and `reachable` at such an override | same, with ceiling 0.780 while age is unknown | no unsupported calibration claim and no new behavior; the default alert path is inert, so it is honest only with FR-022a visibility; the delivery stage is proven end to end only at non-default thresholds (T021–T026 journeys at 0.4) |
| **R1** lower the default to ≤ 0.78 | **still zero alerts** (0.450 < 0.78); only ≤ 0.45 would let size-only signals alert, which makes an "alert" mean "large trade in a niche market" | saturated niche unknown-age cases alert | changes what an alert means with no outcome data; the scorer comment's "cost-adjusted backtest" justification has no reproducible evidence in the repo (G-028); every threshold surface and journey changes |
| **R2** unknown-age base ≥ 0.541 | no effect (the wallet group does not score) | keeps 0.80 reachable | unknown age would outscore a known 24 h wallet; breaks FR-008 |
| **R3** two tiers (alert 0.80 + lower "research watch") | a watch tier needs a floor ≤ 0.45 to show anything | surfaces the 0.78 class without calling it an alert | new product surface; needs its own spec |
| **R4** re-weight / remove niche double counting | changes every ceiling | changes every ceiling | model change needing calibration data |
| **R5** ship 004 as **assessment-only**: delivery structurally suppressed regardless of threshold or overrides (FR-022b, SC-010, T030) | assessments are recorded with their computed `should_alert`; the pipeline never formats or dispatches (zero sends, zero dedup keys) at any `DETECTOR_ALERT_THRESHOLD`, dry-run or real; qualifying rows carry disposition `assessment_only`; FR-022a reports `assessment_only` | same until a later reviewed spec sets `release_posture = alert_capable` | "no alert" is a tested guarantee, not a property of the default; costs a code constant, a new disposition value (no migration, `VARCHAR(32)`), T030, and removes the operator's ability to alert by lowering the threshold; the delivery stage is not exercised by the pipeline at all in this release (dispatcher unit tests remain) |

**Recommended default: R0** (threshold 0.80 retained and recorded; delivery path unchanged), together
with FR-022a/T029 so the startup log, the `Alert Path:` configuration-summary line and the `/health`
`alert_path` field report `default alert path unreachable` (evaluated over the classes the configured
live sources can produce, so it fires on the shipped configuration). It adds no unvalidated claim, no new
behavior and keeps the existing threshold override. You should decide knowingly that the default
configuration produces no deliverable alert in this slice, and that an operator who lowers the threshold
to 0.45 or below gets size-only alerts. Pick **R5** instead if "assessment-only" must hold as a guarantee
under any configuration; then FR-022b, SC-010 and T030 become obligations.

**Correction (round 4)**: until round 4 this packet recommended "R5, configuration as in R0". That was
internally inconsistent: R0's configuration keeps the override and T021 proves a deliverable-eligible
assessment at threshold 0.4, so it is not assessment-only. R0 and R5 are now distinct options with stated
delivery behavior; the recommended substance (R0's configuration with FR-022a) is unchanged, its label
is corrected.

**Reviewer positions (input, not decisions)**: codex round 2 objected that R0 leaves the default alert
path inert and misleading unless visible; codex round 3 chooses **R3** if an operational surface is
required, otherwise assessment-only (R5), and says R0 should not be presented as alert-capable. The
round-3 Claude reviewer chooses R0 framed as an **assessment-only** release (R5 in substance) and would
not pull R3 into this slice because a research-watch tier needs its own spec. Round 4: codex chooses
**R0 for this slice**, described as "default alert path unreachable", not "assessment-only", and says
true assessment-only behavior needs hard delivery suppression with tests (now FR-022b/T030); it would
defer R3 to a separate spec. The round-4 Claude reviewer chooses **R5** and would fold R0 into R5 as the
same configuration framed differently; that fold is not adopted here because, with overrides kept, the
two differ in delivery behavior. Claude adds the trade-off now recorded in the R0 row: with defaults
unreachable, the delivery stage is proven only at non-default thresholds.

### D3 — Category fallback when volume is not usable (DECISION FOR PATRICK)

When the volume state is anything but `available`, today's code can still emit a niche-only size signal
(confidence 0.2) from the market category alone (`detector/size_anomaly.py:288,299-306`). On a metadata
lookup failure the code even invents category `"other"`, which counts as niche-prone
(`:29,211-227`). Reviewers (codex, agy) both flagged this as a false-positive source.

| Option | Effect on an unknown-age trade with no usable volume | Trade-off |
|---|---|---|
| (a) keep as today | niche-prone category or failed lookup ⇒ size 0.2 ⇒ score 0.120 pre-T027 (0.384 post-T027) | preserves 003.1 behaviour; a lookup failure is scored as niche evidence |
| (b) no category fallback; size signal needs measured volume | no size signal ⇒ score 0.000 pre-T027 (0.200 post-T027) | most conservative; every volume outage silences the size group |
| **(c) fallback only for a category from real metadata (recommended)** | real `science/tech/finance/other` ⇒ 0.120 pre-T027 (0.384 post-T027); failed lookup or other categories ⇒ 0.000 (0.200) | removes the invented evidence, keeps the documented heuristic |
| (d) (c) **and** only when no volume value was observed: P1 `unavailable` or P3 `missing` (added after the round-3 Claude review) | as (c) for P1/P3; for P2 `unqualified`, P4 `invalid`, P5 `nonpositive`, P6 `invalid_time`, P7 `stale` ⇒ no size signal ⇒ 0.000 pre-T027 (0.200 post-T027) | closes the path where Gamma did report a value (for example a $5M `science` market whose stamp is 601 s old) yet the market is treated as niche by category; an outage that still returns stale or unqualified values silences the size group for those markets |

**Written default: (c).** (d) is the stricter variant. The earlier reason for not making it the default
("a new rule no reviewer had evaluated") **no longer holds**: two review rounds have now evaluated it and
both round-4 reviewers choose (d). The contract artifacts and replay literals stay on (c) only so they do
not move before you decide; on the reviewers' reasoning alone (d) is the stronger candidate. Choosing (d)
changes FR-005a, data-model §1.2/§3 (`category_source` use), tasks T009/T012 test literals, and the
replay rows for P2/P4–P7 in a niche-prone category (0.120 → 0.000 pre-T027, 0.384 → 0.200 post-T027).
The score effect is small either way (at most 0.120 pre-T027, which never alerts at 0.80).

**Reviewer positions (input, not decisions)**: codex round 3 chooses **(c)** and calls the option set
complete; the round-3 Claude reviewer chooses **(d)**: (c) plus the no-observed-value rule. agy and codex
round 1 flagged the fallback as a false-positive source. Round 4: **both** codex and Claude choose
**(d)**; Claude adds that under G1 the size group is the only scoring group, so a stale or unqualified
$5M `science` market recording niche evidence under (c) matters more.

### D4 — Usable-magnitude bound for a positive volume (DECISION FOR PATRICK; added round 4)

The round-4 Claude review (N3) found that `"1e-30"` (string or number) classifies as `available` and the
ratio `notional / daily_volume` then raises `InvalidOperation` under Python's default 28-digit decimal
context. The same contract also let the grammar-valid string `"1e999999999"` be `available`, whose
plain-notation `daily_volume` would be a billion characters. Gamma controls these values, so both are
reachable paths. The ratio itself is now defined as an exact rational rounded once (data-model §3), which
fixes the crash for every row; D4 decides which positive values may be interpreted at all.

| Option | Behaviour | Trade-off |
|---|---|---|
| **M0** no bound; exact arithmetic only | every finite positive value is `available` when fresh | Truthful to the raw value, but stored decimals are unbounded (`"1e999999999"` exhausts memory when formatted) and a ratio can be dozens of digits; needs a separate storage bound anyway |
| **M1 (recommended; written default)** `10^-6 <= v < 10^15` and ≤ 32 plain-notation characters, else `invalid`/`out_of_range` (data-model §1.1c) | values no real 24 h USD volume can take are malformed observations, scored like any other `invalid` (category fallback per D3) | Adds one reason value and one rule; a genuine sub-micro-dollar or quadrillion-dollar volume would be refused (none is plausible); changes one committed replay row (the 300-digit integer, `available` → `invalid`) |
| **M2** keep every value `available` but cap the stored `volume_impact` (for example at 10^6) and the stored `daily_volume` length | the size signal still saturates as before | The stored ratio is then no longer `notional / daily_volume` (SC-002 fails for capped rows) and needs its own marker; more schema |

**Recommended default: M1.** It is the only option that keeps SC-002 exact for every `available` row and
every stored decimal short without a new marker. The bounds 10^-6, 10^15 and 32 characters are spec
choices you may change; they are not facts about Gamma.

## What changed since the 2026-10-17 packet

| Area | 2026-10-17 | 2026-10-19 revision | Finding(s) |
|---|---|---|---|
| Base | main `46e4192` | main `f2e2167`; hashes/lines refreshed, check script | F25 |
| V2 ingestion | "separate pending request, untouched" | live contract; wallet subject is `proxy_wallet`; journey suites in scope (T021–T026) | F02, F24 |
| Wallet age | assumed available | none on main; FR-007a/b, basis recorded, D1a/D1b | F01, F02 |
| Unknown-age confidence | "capped at" and "exactly" 0.50 | exactly 0.50 everywhere (single table) | F26 |
| Threshold reachability | not examined | table + script + 64 live-realistic replay cases; ceiling 0.450 pre-T027 (default posture), 0.780 post-T027 unknown age; D2 | F03, F04, F05, R3 B1 |
| Wallet gate | none | FR-007c default posture: wallet group non-scoring before T027, serialized as `wallet.group_status` | R2-4a, R3 B1 |
| Volume freshness | per-page stamp carried through Redis by the coupled loop | arithmetic shows 688 s worst case; decoupled Gamma loop, own volume record, miss path cannot poison | F07, F08, F09, F10 |
| Volume states | seven, overlapping, 3 fields | eight, one precedence table P1–P8, zero ⊂ `nonpositive`, one replay case per state/reason | F11, F13, F15 |
| Raw value | "byte-preserved" | decoded JSON value preserved semantically; one rule (data-model §1.1a): verbatim ≤ 256 canonical bytes, else a typed `$truncated` marker or `$nonfinite` tag (explicit exceptions) | F12 |
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
- A volume string counts as numeric only if it is one RFC 8259 number token (data-model §1.1b), so
  `"1_000"`, `" 30000"`, `"+5"` are `invalid`; current main's `Decimal(str(value))` accepts them. If you
  prefer to keep main's leniency, say so; no replay score changes either way for the committed cases.
- A stored volume stamp always carries the exact signed age, so a future stamp shows a negative age in
  whichever state wins (not only `invalid_time`/`future`); nulling it instead would discard evidence.
  Ages are exact to the microsecond (no float), so 600 s is `available` and 600.000001 s is `stale`.
- Opening the T027 gate is a scoring-rule change, so it gets its own version: 004.1 is exactly the
  pre-T027 posture and the qualified posture is 004.2 (contracts/scoring-config.md §2a). The alternative,
  declaring `group_status` part of the 004.1 identity, was not taken because both this contract and the
  shared 003 contract already require a bump for any combination-rule change.
- `volume_impact` is the exact rational `notional / daily_volume` rounded once to 0.0001, not a
  fixed-precision decimal division (data-model §3).

## Final behavior (if authorized, under the recommended defaults)

Assessment-only research core with two defined groups (low-activity wallet + size anomaly; niche as
size modifier). Before T027 only the size group scores (ceiling 0.450); live wallet evidence is recorded
as "low-activity wallet, age unknown" on the **proxy address** with `group_status =
unqualified_pre_T027` and no score contribution. After T027 it scores at confidence exactly 0.50, no
bonus of any kind. Threshold 0.80 inclusive; effective exclusive boundary `B` (default `< 5`; nonce
exactly `B` never qualifies); strict volume ratio `> 0.02`; eight mutually exclusive volume states,
only `available` yields a ratio, never a measured zero; Gamma stamps at response-body receipt with a
0–600 s local recency rule that the decoupled loop can meet; one evaluation instant per trade;
immutable evaluation evidence in the versioned `004.1` document (`004.2` once the gate opens); additive evaluation API with the
deprecated inclusive alias; no new migration.

## Evidence and limitations

- 114-case / 228-score / 63-delta behavioural replay is unchanged and still valid as an
  old-versus-proposed comparison; its 0 → 15 qualifications are **all known-age** cases the live
  runtime cannot produce. The 64-case live-realistic addendum shows, pre-T027, 0 qualifying at 0.80 or
  0.78 (maximum 0.450); post-T027, 0 at 0.80 and 10 at 0.78.
  Neither is calibration.
- Source hashes: 4 of 7 recorded hashes unchanged, 3 changed by refactors only
  (`evidence/source-refresh-f2e2167.md`).
- R01 monetary-volume interpretation rests on a reviewed first-party integration (archived
  2025-12-22 example, not a current warranty; no USDC-settlement/freshness/aggregation claims; no
  total-volume fallback; bounded current schema check still required at implementation entry).
- Reproduce from the repository root (each exits 0; `--check` exits 1 on a stale or missing file and 2 on
  any other arguments):
  `uv run python -m scripts.spec004_evidence.reachability --check specs/004-truthful-detection-contract/evidence/reachability.md`,
  `uv run python -m scripts.spec004_evidence.replay_live --check specs/004-truthful-detection-contract/evidence/replay-live/results.csv`,
  `uv run python -m scripts.spec004_evidence.check_envelope`, `uv run python -m scripts.spec004_evidence.check_source_hashes`.

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
  CHK003's wording is stale (seven versus eight states, P1-P8); whether it is met is for the
  reviewer to judge, and nothing in this revision ticks it (see revision log §5). CHK005 also predates
  the FR-007c posture (pre-T027 an unknown-age wallet has no confidence at all), and CHK023's "v2
  migration" wording is stale (revision log §6).
- **Evidence tooling lives under `scripts/spec004_evidence/`** (stdlib only, no `src/` import) so it is
  inside every Python gate (ruff, black, strict mypy, vulture, complexipy max 5, cyclomatic 10) with no
  suppression; this revision therefore also adds that package outside `specs/`. Outputs are committed and
  `--check` proves they are current.
- A 003 consumer that rejects unknown keys in `scoring_config` breaks on 004.1 rows (named risk).

## Exact scope to authorize

Approve or reject the contract in spec/plan/research/data-model/contracts/tasks **after** deciding
D1a, D1b, D2, D3 and D4. Do not decide whether a factual provider claim is true — evidence category and limits are
stated above.
