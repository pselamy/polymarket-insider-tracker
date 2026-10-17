# Contract: Detector Evaluation API Compatibility

**Feature**: `specs/004-truthful-detection-contract` | **Date**: 2026-10-17 | **Status**: proposed.
Pins exact current interfaces (base `46e4192`); implementation is not authorized.

## Current pins

- `FreshWalletDetector.__init__(wallet_analyzer, *, min_trade_size=1000, max_nonce=5, max_age_hours=48)`
  (`detector/fresh_wallet.py:56`); `analyze(trade) -> FreshWalletSignal | None`.
- `SizeAnomalyDetector.__init__(metadata_sync, *, volume_threshold=0.02, book_threshold=0.05,
  niche_volume_threshold=50000, niche_min_trade_size=500)` (`detector/size_anomaly.py:60`);
  `analyze(trade, *, daily_volume=None, book_depth=None)`.
- `DetectorSettings` (`config.py:334`); `RiskScorer.scoring_config` canonical JSON
  (threshold, both bonuses, quantum, weights); `RiskScorer.assess(bundle)`;
  `MarketMetadataSync` sync 300s / cache TTL 600s; `MarketMetadata.from_dict` (generic
  `last_updated` falls back to now — must never source acquisition time).

## Proposed compatible path

1. New exclusive keyword (e.g. `fresh_transaction_boundary`, default 5) coexists with an
   explicitly-supplied, deprecated inclusive `max_nonce` alias; conflicting controls rejected.
   Supported runtime uses only the exclusive control; default contract is nonce `< 5`.
2. New additive evaluation entry point returns immutable wallet/market evaluation evidence
   alongside optional signals, sharing one acquisition/calculation with `analyze` (no second
   fetch). Existing signal-returning wrappers preserved.
3. Unavailable `volume_impact`/`book_impact` stay nullable authoritative fields; they must
   never serialize, persist, or render as measured zero. Direct library consumers get an
   explicit semantic note: null + availability reason = non-measurement, not zero impact.
4. Same quantized score and actual threshold on every output surface; unknown age and
   unavailable volume/book explicitly labeled; niche shown as size modifier.
