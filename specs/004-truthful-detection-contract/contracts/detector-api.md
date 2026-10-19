# Contract: Detector Evaluation API Compatibility

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17, rewritten 2026-10-19 | **Status**: proposed.
Pins exact current interfaces (refreshed to main `1bdccbc`; originally `46e4192`); implementation is not authorized.

## Current pins

- `FreshWalletDetector.__init__(wallet_analyzer, *, min_trade_size=1000, max_nonce=5, max_age_hours=48)`
  (`detector/fresh_wallet.py:56`); `analyze(trade) -> FreshWalletSignal | None`. The bound is stored
  privately as `_max_nonce` (`:74`) and compared with `profile.nonce > self._max_nonce` (`:153`); there
  is **no public `max_nonce` attribute** to preserve.
- `SizeAnomalyDetector.__init__(metadata_sync, *, volume_threshold=0.02, book_threshold=0.05,
  niche_volume_threshold=50000, niche_min_trade_size=500)` (`detector/size_anomaly.py:61`);
  `analyze(trade, *, daily_volume=None, book_depth=None)` (`:87-93`); `analyze_batch(trades, *,
  volume_data=None, book_data=None)` (`:363`). The live pipeline calls `analyze(trade)` with no volume
  (`pipeline.py:735`; gap G-022).
- `DetectorSettings` (`config.py:335`); `RiskScorer.scoring_config` canonical JSON
  (threshold, both bonuses, quantum, weights); `RiskScorer.assess(bundle)`;
  `MarketMetadataSync` sync 300 s / cache TTL 600 s; `MarketMetadata.from_dict` (generic
  `last_updated` falls back to now — must never source acquisition time).
- `RiskAssessment.timestamp` (`detector/models.py:260`) is the assessment creation time. There is **no**
  `evaluated_at` anywhere in `src/` today.

## 1. Fresh-transaction boundary and the deprecated `max_nonce` alias (complete table)

Keyword-only parameters on `FreshWalletDetector`: `fresh_transaction_boundary: int | None = None` (new,
exclusive: a wallet qualifies when `nonce < B`) and the existing `max_nonce: int | None = None`
(deprecated, inclusive: legacy `nonce <= N`). The default of `max_nonce` changes from the literal `5` to
`None` so "omitted" is distinguishable from "explicitly 5" (same call shape, same effective behavior for
omitted-both).

| `max_nonce` (N) | `fresh_transaction_boundary` (B) | Result |
|---|---|---|
| omitted | omitted | `B = 5` (`DETECTOR_FRESH_TRANSACTION_BOUNDARY` default) |
| omitted | given | `B`; `bool`, non-int or `B < 1` or `B > 2**53 - 1` → `ValueError` |
| given | omitted | `B = N + 1` (legacy inclusive semantics: `max_nonce=5` ⇒ `nonce <= 5` ⇒ `B = 6`, which differs from the omitted-both default 5 by design); emits the deprecation warning; `bool`, non-int, `N < 0` or `N + 1 > 2**53 - 1` → `ValueError` |
| given | given | `ValueError` unless `B == N + 1`; when equal, warn and use `B` |

- **Negative N**: `ValueError("max_nonce must be a non-negative integer")`. Legacy accepted it and
  silently built a detector that can never fire (`nonce > -1` is always true); an untruthful
  configuration is rejected rather than reproduced. This is the one deliberate behaviour break in the
  alias path and is listed in the decision packet's known limits.
- **Overflow**: Python integers do not overflow; the `2**53 - 1` ceiling exists only so the integer
  stored in `detector_config.fresh_transaction_boundary` is exactly representable by any JSON consumer.
- **Warning**: `DeprecationWarning`, `stacklevel` chosen so the warning is attributed to the caller of
  `FreshWalletDetector(...)` (the repository precedent is `_warn_deprecated_weights`,
  `detector/scorer.py:56-63`, `stacklevel=3` through a helper). Text: names `fresh_transaction_boundary`
  and the equivalent value `N + 1`.
- **Attributes**: a read-only `fresh_transaction_boundary` property exposes the effective `B`;
  `_max_nonce` is removed (private). No public attribute is lost.
- **Removal**: this slice only emits the warning. Removal needs a separate, later, approved change; no
  date is promised here.
- `profiler/analyzer.py:24,225` (`DEFAULT_FRESH_THRESHOLD = 5`, `nonce >= threshold` rejects) is already
  exclusive; the canonical freshness rule of data-model §2a governs which meaning surfaces use.

## 2. Evaluation entry points and the evaluation clock

New additive methods (existing signal-returning wrappers are preserved and call them):

```
FreshWalletDetector.evaluate(trade, *, evaluated_at: datetime) -> WalletEvaluation
SizeAnomalyDetector.evaluate(trade, *, evaluated_at: datetime,
                             daily_volume: Decimal | None = None,
                             book_depth: Decimal | None = None) -> SizeEvaluation
```

- `evaluated_at` MUST be timezone-aware (naive → `ValueError`) and is normalised to UTC. It is the
  **single** instant against which volume age (`evaluated_at − daily_volume_acquired_at`, 0–600 s) and
  wallet `observation_time` are judged, and it is stored as `evaluation_evidence.evaluated_at`.
- Both detectors gain a keyword-only constructor argument `clock: Callable[[], datetime]` (default
  `lambda: datetime.now(UTC)`). The wrappers `analyze`/`analyze_batch` read the clock **once per trade**
  and pass that value to `evaluate`; `analyze` therefore keeps its signature.
- The pipeline reads one instant per trade and passes the same value to both detectors, so wallet and size
  evidence of one assessment agree on "now". `RiskAssessment.timestamp` keeps its meaning (creation
  time) and is not reused.
- `WalletEvaluation` / `SizeEvaluation` are frozen dataclasses carrying the evidence of
  `scoring-config.md` §2 (including known-nonqualifying inputs), an optional signal, and share one
  acquisition and calculation with the wrapper (no second fetch).

## 3. Explicit `daily_volume` / `volume_data`

An explicit `daily_volume` (or an entry of `volume_data`, `detector/size_anomaly.py:310-322`) is a
caller assertion without provenance. Per data-model §1.3: positive `Decimal` ⇒ state `available`,
`daily_volume_source = "caller"`, reason `caller`, `acquired_at = evaluated_at`, age 0; zero or negative
⇒ `nonpositive` (reason `zero`/`negative`); `None` ⇒ no argument, fall through to the stored
observation. An explicit value overrides stored data and is recorded as `source = "caller"` so it
cannot be mistaken for Gamma data. `book_depth` stays accepted but cannot score (FR-006): the evidence
records `book_state = "unavailable"` and `book_impact = null` regardless of the argument.

## 4. Impact migration (nullable measured companions)

Current `SizeAnomalySignal.volume_impact`/`book_impact` are required `float`s
(`detector/models.py:152-153`), emitted directly (`:172-173`), documented "0.0 if unknown"
(`:142-143`); they do not "stay nullable". Decision:

- Keep both required-float attributes and wire positions unchanged for compatibility; `0.0` there is a
  legacy non-measurement sentinel.
- Add authoritative companions `volume_impact_measured: Decimal | None` and
  `book_impact_measured: Decimal | None`. `volume_impact_measured` is `notional / daily_volume`
  quantised to `0.0001` (`ROUND_HALF_EVEN`) **only in state `available`**, otherwise `None` (never
  `Decimal("0")`); `book_impact_measured` is always `None` in this slice. The type is `Decimal`, not
  `float`, to match SC-002, the storage DTO (`storage/repos.py:566`, already `Decimal | None`) and the
  stored evidence (4-decimal string). (An earlier draft of this contract said `float | None`; superseded.)
- Persistence: `pipeline.py:904-906` currently writes `round(float, 4)` whenever a signal exists
  (so an unmeasured volume would persist as `0.0`). It must write `volume_impact_measured` /
  `book_impact_measured` (None ⇒ SQL NULL). Rows written before 004 keep their numbers with legacy
  semantics (a `0.0` is a non-measurement there).
- Rendering: `alerter/formatter.py` renders no impact value today (`grep -rn impact alerter` is empty), so
  there is no existing `None` rendering to break. The detailed format gains exactly one line:
  `Size vs 24h volume: 10.83% of $30,000` when measured, or `Size vs 24h volume: unavailable
  (<volume_state>)` when `None`; compact format is unchanged; neither prints `0%` for a non-measurement.
  Discord/Telegram channels reuse the formatter output.
- Serializers (`to_dict`), `scorer.py::_extract_size_diagnostics`, the `RiskAssessment` persistence
  projection, health/capability surfaces and storage readers consume the companions.

## 5. Wallet evidence fields

Wallet evidence returned by `evaluate` carries `subject_kind = "proxy_wallet"`, `age_basis` (`none` on
current main) and `observation_time = profile.analyzed_at` ([data-model.md](../data-model.md) §2). The
unknown-age rule (exactly 0.50, factors `{base}` only, no brand-new/very-young/large-trade addition) is
defined once in data-model §2; nothing in this contract permits "capped at".

## 6. Output consistency

Same quantised score and actual threshold on every output surface; unknown age and unavailable
volume/book explicitly labelled; niche shown as a size modifier; the threshold's reachable evidence
classes disclosed (FR-022).
