# Contract: Detector Evaluation API Compatibility

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17 | **Status**: proposed.
Pins exact current interfaces (refreshed 2026-10-19 to main `1bdccbc`; originally `46e4192`); implementation is not authorized.

## Current pins

- `FreshWalletDetector.__init__(wallet_analyzer, *, min_trade_size=1000, max_nonce=5, max_age_hours=48)`
  (`detector/fresh_wallet.py:56`); `analyze(trade) -> FreshWalletSignal | None`.
- `SizeAnomalyDetector.__init__(metadata_sync, *, volume_threshold=0.02, book_threshold=0.05,
  niche_volume_threshold=50000, niche_min_trade_size=500)` (`detector/size_anomaly.py:61`);
  `analyze(trade, *, daily_volume=None, book_depth=None)`.
- `DetectorSettings` (`config.py:335`); `RiskScorer.scoring_config` canonical JSON
  (threshold, both bonuses, quantum, weights); `RiskScorer.assess(bundle)`;
  `MarketMetadataSync` sync 300s / cache TTL 600s; `MarketMetadata.from_dict` (generic
  `last_updated` falls back to now — must never source acquisition time).

## Proposed compatible path

1. New exclusive keyword `fresh_transaction_boundary` (default 5) coexists with an
   explicitly-supplied, deprecated inclusive `max_nonce` alias; conflicting controls rejected.
   Omitted both → effective boundary `B = 5`. Explicit `max_nonce=N` alone → `B = N + 1`
   with a deprecation warning (legacy inclusive semantics preserved: old `max_nonce=5` means
   `nonce <= 5`, i.e. `B = 6`). Explicit `fresh_transaction_boundary=B` alone → `B`.
   Both supplied → reject unless `B == N + 1`, then warn and use `B`. Supported runtime
   evaluates only `nonce < B`. Current code pins: `detector/fresh_wallet.py:56-63`
   (`max_nonce: int = DEFAULT_MAX_NONCE = 5`, `profile.nonce > self._max_nonce` reject at `:153`) and
   `profiler/analyzer.py:24,225` (`DEFAULT_FRESH_THRESHOLD = 5`, `nonce >= threshold`
   reject). No external non-use is asserted: additive keyword, alias retained.
2. New additive evaluation entry point returns immutable wallet/market evaluation evidence
   alongside optional signals, sharing one acquisition/calculation with `analyze` (no second
   fetch). Existing signal-returning wrappers preserved.
3. Impact migration (explicit type/semantic change, additive/versioned — Phase 1 choice
   now closed): current `SizeAnomalySignal.volume_impact`/`book_impact` are required
   `float`s (`detector/models.py:152-153`) emitted directly (`detector/models.py:172-173`)
   with `0.0`-on-unknown docstrings (`:142-143`) — they do NOT “stay nullable” today.
   Proposed: keep the two required-float attributes and wire positions unchanged, and add
   authoritative nullable companions `volume_impact_measured: float | None` /
   `book_impact_measured: float | None`, where `None` + availability reason =
   non-measurement. Serializers (`to_dict` `:138-154`),
   `scorer.py::_extract_size_diagnostics`, `RiskAssessment` persistence projection,
   formatters (`alerter/formatter.py`, `alerter/channels/*`), health/capability surfaces,
   and storage readers consume the nullable companions; operational unavailable
   volume/book/ratio MUST NOT look like a measured zero anywhere. Legacy numeric
   sentinels (including `0.0`) keep version-specific reader semantics and are documented
   as non-measurements. Compatibility window covers the deprecated custom-weight
   API/window per 003.
4. Same quantized score and actual threshold on every output surface; unknown age and
   unavailable volume/book explicitly labeled; niche shown as size modifier.
