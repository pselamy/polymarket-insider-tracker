# Research: Truthful Detection Contract

**Date**: 2026-10-16, R01 disposition appended 2026-10-17. **Status**: Phase0 complete;
Phase1 design proposed. Implementation is not authorized.
Current source is main `46e41921ca06b701fece7a17953a666f6873b7e1`. No product code changed.
Prior Phase0 artifacts (corpus, replay, clarification) hydrated byte-identically from
`polymarket-004-preparation-c64a73b`; scoring runtime verified unchanged between the replay
base (`523b0a3`) and this base (no `src/` diff).

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

**Proposed decision**: known-age wallet candidates require notional>=1000, nonce<configured boundary
and age<=48h. Keep the current known-age confidence formula. Unknown-age low-activity candidates
have confidence exactly0.50 and only a low-activity factor; no new-wallet, age or large-trade addition
survives. Missing wallet retrieval never supplies a freshness fact.

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

**Proposed direction**: preserve public call shapes and signal-returning wrappers. A new exclusive
transaction-boundary keyword can coexist with an explicitly supplied, deprecated inclusive
`max_nonce` alias, with conflicting controls rejected; supported runtime uses only the exclusive
control. Validate the exact migration in Phase1.

Unavailable impacts must never be serialized, persisted or rendered as measured zero. Phase1 must
choose and document nullable authoritative impact fields and compatibility behavior, including direct
library consumers; retaining a numeric sentinel without an explicit non-measurement contract is not
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
