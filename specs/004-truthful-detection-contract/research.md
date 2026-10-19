# Research: Truthful Detection Contract

**Date**: 2026-10-16, R01 disposition appended 2026-10-17. **Status**: Phase0 complete;
Phase1 design proposed. Implementation is not authorized.
Current source was main `46e41921ca06b701fece7a17953a666f6873b7e1` when written; **refreshed
2026-10-19 to main `1bdccbca1db93dea7045d6d9c0068b9d04d30455`** (see
[evidence/revision-2026-10-19.md](evidence/revision-2026-10-19.md) and
[evidence/source-refresh-1bdccbc.md](evidence/source-refresh-1bdccbc.md)). No product code changed.
Prior Phase0 artifacts (corpus, replay, clarification) hydrated byte-identically from
`polymarket-004-preparation-c64a73b`; scoring runtime verified unchanged between the replay
base (`523b0a3`) and this base (no `src/` diff). Re-verified 2026-10-19 against `1bdccbc`: `scorer.py` and `pipeline.py` byte-identical,
three detector files refactor-only (`evidence/source-refresh-1bdccbc.md`).

## R01 — Gamma volume units and freshness qualification

**Prior finding (2026-10-16, preserved as history)**: the [official market reference](https://docs.polymarket.com/api-reference/markets/list-markets)
documents`volume24hr` as a nullable number and`updatedAt` as a general timestamp. It does not provide a
field-specific monetary-unit definition or provider freshness SLA. A bounded anonymous one-market
probe returned HTTP200 and the field; that proves presence, not units or measurement freshness.
The [official market UI](https://polymarket.com/event/will-the-tesla-roadsters-0-60-time-be-less-than-1-second)
displayed dollar-denominated total volume matching that market's Gamma`volumeNum` at displayed
rounding. This supports total-volume monetary interpretation, but is not yet a daily-field contract.

**Decision**: keep daily monetary-unit qualification open before enabling notional/daily-volume
scoring. The bounded final primary-source/UI check did not qualify that field. Do not substitute Data API v2
bare-volume share semantics or a third-party claim. If primary evidence remains insufficient, record the actual
source question and obtain its legitimate disposition before implementation entry.

**Alternatives**: assuming units from field names is unsupported; replacing the provider or deriving
24-hour history expands acquisition work and needs its own source/rate/recovery qualification.

**Bounded qualification result**: two official pages and three static UI bundles were inspected.
The official bundle `https://polymarket.com/_next/static/immutable/chunks/3a5yk3a1f9q08.js`
(SHA256 `5ad6be99c9fbb7b6e921511412b90e54d3a35a6c85b98e293c62014d6b01120b`)
destructures event-level `volume`/`volume24hr` and calls imported module161469 `formatVolume` for
both. The formatter definition was not located in those bounded bundles; an event-level display
path also does not establish the Gamma market-level daily-field contract. Four further targeted
search queries produced no qualifying primary evidence. This remains an unresolved source question,
not a completed contract. A new lookup needs a specific primary-source lead; do not repeat broad
searches or promote partial UI inference into a provider guarantee.

**R01 disposition (2026-10-17, independent primary-source review)**:
`outputs/polymarket-gamma-unit-primary-review.md` (SHA256
`0b9555816e6d7df667f664483f63bf87ac9c5e3a2041e5604326bd862ceec279`) records a bounded
SHIP verdict for a **proposed monetary-volume contract**: official
`Polymarket/safe-wallet-integration` commit `49ec9991b7f3e95197a4d53910f6086bf3ff2294`
(dated 2025-12-22; repository archived May 2026) preserves Gamma `event.markets` objects,
reads `market.volume24hr` into `volumeUSD`, and renders dollar-denominated 24h Volume
(`app/api/polymarket/markets/route.ts:17-58`, `hooks/useMarkets.ts:16-17,66-72,131`,
`components/Trading/Markets/MarketCard.tsx:27-29,63-65`, `utils/formatting.ts:12-17`).
The same commit's `constants/api.ts:5` pins the Gamma base URL
`https://gamma-api.polymarket.com` (snapshot receipts under packet
`work/gamma-unit-primary-review-20261017` and
`work/gamma-unit-endpoint-primary-review-20261017`).

**Proposed contract**: interpret Gamma market `volume24hr` as provider USD-denominated
monetary market trading volume over the provider's 24-hour window, grounded in that
first-party integration path plus a bounded current field/schema qualification at
implementation entry. This is field-specific first-party semantic evidence, not the
earlier event-total inference. It resolves the missing primary-unit evidence for managed
planning; the rejected unavailable-everywhere workaround is not needed.

**Preserved limits**: archived example is the correct evidence category, not a current
normative unit warranty or provider SLA. Do not claim literal USDC settlement, conversion
guarantee, exact aggregation/fee/AMM-CLOB boundaries, measurement freshness, or provider
permanence. Never adopt the example's `volume24hr || volume || "0"` fallback: no all-time
substitution. Numeric zero, missing, malformed, nonpositive, stale, and invalid-time values
remain distinct availability states. Current schema qualification stays an integration
check; generic metadata `last_updated` is not acquisition time.

## R02 — Explicit local acquisition provenance

**Proposed decision**: stamp each successful Gamma HTTP page at body acquisition, carry
`daily_volume_acquired_at` and source identifier through stats, enrichment and Redis unchanged.
A finite positive volume is locally recent only when an aware UTC timestamp is valid and
`0 <= evaluated_at - acquired_at <= 600 seconds`. The fixed600-second policy matches the existing
cacheTTL; it is not a new operator knob or provider measurement-time guarantee.

Missing, malformed, naive, nonpositive-epoch and future timestamps are unusable. Old cache records
remain metadata-readable but provide no usable volume provenance. Never synthesize this timestamp
from cache reads, TTL, CLOB refresh or generic`last_updated`. The latter currently falls back to now
when deserialization cannot read a timestamp (`ingestor/models.py::MarketMetadata.from_dict`).

**Rationale**: per-page timestamps prevent a long crawl making early pages appear newly acquired.
The one aware acquisition timestamp is stamped at successful HTTP body acquisition
(`ingestor/gamma_client.py` page fetch) and preserved unchanged on Redis cache write/read
(`ingestor/metadata_sync.py::_enrich_and_cache_market`); a cache read never restamps it, so
local age 0–600s is a local recency rule distinct from provider measurement freshness.
**Alternatives**: whole-crawl timestamps overstate recency; retaining old values after failures adds
carry-forward behavior. Keep the current conservative omission policy: unavailable Gamma results
leave volume unavailable while CLOB metadata remains usable. Partial results or omitted markets are
never measured zero. Value states distinguish available, missing, nonpositive, invalid, stale, invalid
time and unqualified units. A failed fetch cannot refresh an older observation's time.

**Sources**: `ingestor/gamma_client.py::GammaMarketStats`, `MarketMetadataSync._fetch_gamma_stats`,
`_enrich_and_cache_market`, `MarketMetadata.to_dict/from_dict`; sync300s/cacheTTL600s.

## R03 — Minimal coherent operator surface

**Proposed decision**: retain existing finite`DETECTOR_ALERT_THRESHOLD` default0.80 in[0,1],
quantized to0.001 with inclusive score comparison. Add only the two replay-covered settings:
`DETECTOR_FRESH_TRANSACTION_BOUNDARY` default5, positive integer and exclusive nonce bound;
`DETECTOR_SIZE_VOLUME_THRESHOLD` default0.02, finite positive ratio and strict greater-than rule.
Do not impose an invented maximum of1 on a volume ratio.

Keep existing wallet minimum notional1000, known-age ceiling48h, niche volume50000, niche-only
minimum500, confidence constants and weight rules fixed, discoverable and recorded. Exactly48h is
allowed; the very-young bonus is strictly below2h and large-trade bonus strictly above10000.

**Rationale**: only these two new controls have dedicated replay override cases.
**Alternatives**: exposing every constructor constant introduces unreviewed controls; an age override
above48h conflicts with the newly-created-wallet claim. No new setting is already wired on main.

**Sources**: `config.py::DetectorSettings`; `pipeline.py::_initialize_components`; existing detector
constructors; slice004 userstory3; fixed114-case corpus.

## R04 — Core wallet and size semantics

**Proposed decision**: known-age wallet candidates require notional>=1000, nonce<effective
boundary B (captured `DETECTOR_FRESH_TRANSACTION_BOUNDARY`, default5) and age<=48h. Keep
the current known-age confidence formula. Unknown-age low-activity candidates require
notional>=1000 and nonce<B, have confidence exactly0.50 and only a low-activity factor; no
new-wallet, age or large-trade addition survives. Missing wallet retrieval never supplies a
freshness fact. Equality: nonce exactly B never qualifies. Counterexamples fixed by this
rule: `transaction-override-3-3` (nonce3, B=3) emits no wallet signal; `transaction-override-10-9`
(nonce9, B=10) emits one — the replay already decides both correctly, and the data model now
uses B everywhere instead of a hardcoded5.

Usable daily volume enables notional/volume ratio. Unusable volume has no observed ratio. Usable
volume<50000 gives numeric niche context; unavailable volume may use existing category fallback
`science/tech/finance/other`, with the basis recorded. Retain existing strict volume threshold,3x
confidence saturation,1.5 niche multiplier and0.2 niche-only confidence subject to notional>=500.
Book depth stays unavailable in the live core; metadata liquidity is not observed book depth.

**Rationale**: this implements the approved A4 direction with minimal score changes.
**Alternatives**: inferring unknown age from nonce or treating failed inputs as zero violates explicit
specification requirements. Dormant book/sniper/funding algorithms remain outside the core.

## R05 — Evidence independent of optional signals

**Proposed decision**: use immutable wallet/market evaluation evidence alongside optional signals,
including known nonqualifying inputs. An additive evaluation-returning entry point shares one
acquisition/calculation with the existing signal-returning`analyze` wrapper. Avoid a second fetch.
Carry evidence through bundle, assessment, persistence and formatting. Distinguish not-requested
below minimum, retrieval failed, known nonqualifying, unknown age and emitted signal.

Populate nullable scalar impacts and availability from the evaluated snapshot, not signal presence
or liquidity. Capture the effective threshold with the assessment rather than rereading mutable
settings when persistence runs. Normal no-signal trades need no new row;003's signal-bearing and
detector-failure storage scope remains.

**Sources**: `scorer.py::_extract_size_diagnostics/_extract_fresh_diagnostics`,
`pipeline.py::_persist_assessment/_persist_detector_failure_skip`;003 assessment-storage contract.

## R06 — Versioned evidence without a second migration

**Finding**: the003 table has scalar fields and`scoring_config` TEXT, not a generic evidence JSON
column. Existing signals/factors in memory are not durable evidence by themselves.

**Proposed decision**: explicitly amend the shared003 storage contract. Preserve current root keys
and types, add`detector_config`, versioned`evaluation_evidence` and evidence schema identity.
Static configuration stays canonical; the whole per-assessment envelope varies with observations.
Keep`RiskScorer.scoring_config` as pure effective configuration and serialize the extended envelope
for each assessment. Proposed algorithm/evidence identity is004.1.

Retain root`multi_signal_bonus_3` as a documented neutral structural compatibility key and mark it
disabled; remove the unreachable operational branch. The two-group1.2 bonus, weights0.40/0.35/0.25
and0.001 ROUND_HALF_EVEN precision remain. Old003.1 and legacy-unversioned rows remain untouched and
use their version-specific reader semantics. Preserve the deprecated custom-weight API/window.

**Rationale**: exact source values, acquisition/evaluation times, availability reasons, factors,
contributions and threshold must survive without a competing migration.
**Alternatives**: new DDL conflicts with FR020; delivery_channels is not an evidence field; logs-only
evidence cannot reconstruct durable assessments. This is a deliberate semantic contract amendment,
not a claim that the old field already stored source evidence.

## R07 — Compatibility and truthful output are review decisions

**Decided Phase1 choice** (this packet; implementation still reserved): preserve public
call shapes and signal-returning wrappers. The new exclusive `fresh_transaction_boundary`
keyword coexists with an explicitly supplied, deprecated inclusive `max_nonce` alias, with
conflicting controls rejected; supported runtime uses only the exclusive control; see
[contracts/detector-api.md](contracts/detector-api.md) for the omitted/default/explicit/
conflicting argument table. No research sentence is left open.

Unavailable impacts must never be serialized, persisted or rendered as measured zero. The
decided migration is additive/versioned nullable companions
(`volume_impact_measured`/`book_impact_measured`), documented in
[contracts/detector-api.md](contracts/detector-api.md) with exact current-interface pins
(required floats at `detector/models.py:152-153`, direct emission at `:172-173`); retaining a numeric sentinel without an explicit non-measurement contract is not
a solution. Render the same quantized score and actual threshold on every output, identify unknown
age and unavailable volume/book, and show niche as a size modifier. Keep research/non-accusatory
wording; funding is enrichment, sniper is experimental and backtesting is deferred.

**Sources**: public constructors, detector signal serialization, all formatter variants,004 FR005/008/
017/018 and the constitution's compatibility requirement. These choices are not implementation approval.

## R08 — Replay and verification limits

The fixed114-case corpus and old/proposed results have an independent preliminary SHIP review.
All228 scores were independently reconstructed;63 changed cases have requirement attribution.
Default-runtime subset99 has qualification0->15. Nine proposed detector-override cases are separate.
The prototype initially passed proposed overrides into old construction; that was corrected to the
real pipeline defaults without changing the corpus. No calibration or production correctness claim
follows. See[evidence/replay.md](evidence/replay.md).

Canonical reproduction tooling still needs complete source enrollment, types/complexity controls and
regression proof before adoption. Full pipeline, real storage, output, failure, restart and integration
evidence remain mandatory. Reviewer checklists remain human-owned; implementation remains reserved.

## R09 — Wallet identity and age source (added 2026-10-19; resolves review findings F01, F02, F26)

**What is profiled (verified in code)**: the trade's wallet is the V2 `proxy_wallet`:
`ingestor/trade_row_v2.py:27` maps it to `proxyWallet`; `ingestor/trade_rows.py:169` takes it as the
identity wallet and `:251` sets `TradeEvent.wallet_address`; `profiler/analyzer.py:168` →
`profiler/chain.py:308-327` call `eth_getTransactionCount` on exactly that address. This predates V2
(slice 001 already mapped `proxyWallet`→`wallet_address`).

**What that address can and cannot carry**

| Can carry (code-verified) | Cannot carry / unverified |
|---|---|
| A per-address Polygon transaction count at profiling time (`analyzed_at`) | Whether that count tracks Polymarket trading for a proxy address. If the address is a contract account its nonce counts contract creations, not trades; if relayers submit the transactions the address's own nonce may not move at all. **Neither is verified for Polymarket** (no first-party code, test or checked-in evidence); the reviewers' "stays 0" claim is likewise unverified. The spec is written to hold under either outcome. |
| The address as a stable trade-row identity (lowercased, slice 001 contract) | The participant's account age, total history or identity; a proxy address is not shown to be 1:1 with a person |
| Nothing about age on current main | `first_seen` is `None` for every wallet: `profiler/chain.py:519-530`, `profiler/analyzer.py:180-196` |

Consequences adopted in the spec: FR-007a (subject kind recorded; wording limited to "low transaction
count on the proxy address"), FR-007b (age needs a recorded basis; live age unknown until a source
is selected), FR-008 (unknown age is exactly 0.50), and a required qualification receipt (T027) before
the wallet group may be described more strongly.

**Age-source options** (a product/data-semantics choice → DECISION D1 in
[decision-packet.md](decision-packet.md); nothing here is settled)

| Option | What "age" means | Trade-offs | Evidence status |
|---|---|---|---|
| **O1 – age branch non-operational; score only on low-activity evidence** (recommended default for this slice) | No age. Live wallet group = `low_activity_unknown_age`, confidence exactly 0.50 | No new dependency, credential, request or rate cost; fully truthful; the known-age branch stays tested via injected profiles but is documented non-operational. Cost: max score 0.78 (see R10), so 0.80 alerts need D2 | Fully supported by current code |
| **O2 – Polymarket-activity age**: earliest public trade seen for the proxy address in the Data API | "Time since this proxy address's first public Polymarket trade" (`age_basis = polymarket_first_trade`). Not chain age and must never be worded as such | Directly measures "new to Polymarket", independent of the nonce question; one extra anonymous request per candidate (only trades ≥ $1,000 pass the wallet gate, so volume is small against the 200 req/10 s Data API limit recorded in slice 001 feasibility). Costs: needs a per-user query qualified for this use, results are newest-first with a 10,000-row offset cap, so "earliest" is only exact for addresses whose whole history fits; otherwise age is a lower bound and must be `unknown`. A maker-side trade may be hidden if `takerOnly` defaults apply | The official endpoint is documented as "Get trades for a user or markets" (`specs/001-supported-trade-ingestion/evidence/feasibility.md:13`) but the user filter, ordering and `takerOnly` interaction are **UNVERIFIED** here; no live call was made in this revision |
| **O3 – external indexer** (chain first-transaction time of the address) | `age_basis = chain_first_transaction` | Matches the original README meaning; adds a vendor dependency, a credential or quota, new failure/cost surface and a trust decision; still subject to the proxy-address semantics question (a contract's creation time is not the participant's first use) | No indexer in the repo (`get_first_transaction` note at `profiler/chain.py:524-527`); vendor behaviour unverified |
| **O4 – Gamma public-profile style creation timestamp** | Account creation as Polymarket records it | Possibly the closest to "new account" | **UNVERIFIED lead only**: nothing in the repo or checked-in evidence shows such an endpoint or its terms; listed so a future qualification can evaluate it, not as a candidate this revision can recommend |

**Recommended default**: O1 for this slice, with O2 as the first follow-up candidate once a bounded,
authorized qualification (T027) establishes the per-user query semantics. Rationale: it is the only option
whose every claim is supported by code on main today, and it keeps FR-007/FR-008 truthful without
adding a network surface to the detection path.

**FR-007/FR-008 under each option**

| | O1 | O2 | O3 |
|---|---|---|---|
| Live `age_state` | always `unknown` | `known` when full history is reachable, else `unknown` | `known` when indexer answers, else `unknown` |
| Live classification | `low_activity_unknown_age` only | `fresh` or `low_activity_unknown_age` | same as O2 |
| Wording of a `fresh` hit | n/a (never emitted live) | "first Polymarket trade seen < 48 h ago" | "first on-chain transaction < 48 h ago" |
| Unknown-age rule (exactly 0.50) | applies to every candidate | applies when history truncated or lookup failed | applies on indexer miss/failure |
| Known-age confidence formula | unchanged, library-only | unchanged | unchanged |

The unknown-age rule is identical under every option: exactly 0.50, factors `{base}`, no bonuses.

**Qualification receipt T027 (not performed; needs explicit authorization)**: for a bounded sample of
proxy addresses, compare `eth_getTransactionCount` against the address's public trade count and record
whether the nonce moves with trading. Until then every statement about proxy-address nonce meaning is
labelled UNVERIFIED.
