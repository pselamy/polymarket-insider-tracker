# Interface Contract: Persisted Risk Assessment Schema

**Feature**: `specs/003-safe-observable-operation`
**Date**: 2026-09-10

This contract governs the shared storage schema for slice 003 and slice 004, implemented in Alembic revision `003_safe_observable_operation`.

---

## 1. Migration Specification
- **Revision ID**: `003_safe_observable_operation`
- **Down Revision**: `002_risk_assessments`
- **Table**: `risk_assessments`

### Columns Added:
1. `delivery_disposition`: `VARCHAR(32)`, `nullable=False`, `server_default='unrecorded'`
   (rows existing before the migration have no recorded delivery outcome; backfilling them
   as `dry_run` while `dry_run` defaults to false would assert contradictory facts)
2. `delivery_channels`: `TEXT`, `nullable=True`
3. `dry_run`: `BOOLEAN`, `nullable=False`, `server_default='false'`
4. `volume_available`: `BOOLEAN`, `nullable=True`
5. `market_daily_volume`: `NUMERIC(20, 6)`, `nullable=True`
6. `book_depth_available`: `BOOLEAN`, `nullable=True`
7. `wallet_tx_count`: `INTEGER`, `nullable=True`
8. `wallet_age_known`: `BOOLEAN`, `nullable=True`
9. `scoring_algorithm_version`: `VARCHAR(32)`, `nullable=False`, **no server default**.
   Added nullable, backfilled for all pre-existing rows as exactly `legacy-unversioned`
   (their exact configuration is unknowable), then altered to NOT NULL. The absence of an
   insert default forces every new row to state its actual producing algorithm version.
10. `scoring_config`: `TEXT`, `nullable=True`. Canonical deterministic JSON of the exact
    active scoring configuration for each new row; stays `NULL` for legacy rows only.

Columns 9–10 were added by Patrick's 2026-09-11 schema decision (recorded in the spec's
Session 2026-09-11 clarification), expanding the original 8-column clarification after the
round-4 adversarial review showed stored rows could not identify a reproducible algorithm.

### Indexes Added:
- `idx_risk_assessments_disposition` on (`delivery_disposition`)

---

## 2. Downgrade Contract
The `downgrade()` function in `003_safe_observable_operation`:
- Drops `idx_risk_assessments_disposition`
- Drops the 10 added columns
- Returns the schema to exact `002_risk_assessments` state

---

## 3. Domain Model Synchronization
The SQLAlchemy `RiskAssessmentModel` in `src/polymarket_insider_tracker/storage/models.py` and the domain dataclass `RiskAssessment` in `src/polymarket_insider_tracker/detector/models.py` MUST expose matching fields.

---

## 4. Scoring Algorithm Identity & Compatibility
Every stored row replays its decision (Constitution IV, FR-012) because it carries its own
identity:

- `scoring_algorithm_version` records the producing algorithm version
  (`SCORING_ALGORITHM_VERSION`, currently `003.1`, defined in `detector/models.py`); the
  migration backfills pre-existing rows as exactly `legacy-unversioned` and nothing else may
  produce that label (the column has no insert default).
- `scoring_config` records the canonical deterministic JSON of the exact active
  configuration used for the assessment: alert threshold, multi-signal bonuses, score
  quantum, and the active weights — all as decimal strings, keys sorted, separators fixed,
  so equal configurations serialize identically. Legacy rows keep `NULL` because their
  configuration is unknowable; `detector_failure` skip rows record the active configuration
  under which they were written even though no score was computed.
- confidences, the threshold, and the score are quantized to the persisted `NUMERIC(4,3)`
  precision *before* the decision, so replaying from the row plus its `scoring_config`
  reproduces the score and decision exactly.

Weight compatibility window (Patrick's 2026-09-11 decision): `DEFAULT_WEIGHTS` is immutable
(`MappingProxyType`) and `get_weights()` returns a defensive copy, so nothing can change
scoring behind the persisted records' back. The pre-slice-003 `weights=` constructor
argument and `set_weights()` API remain functional for one compatibility/deprecation window
and emit `DeprecationWarning`; rows produced under custom weights stay replayable because
the weights are recorded per row in `scoring_config`. Breaking removal of the deprecated
API is outside this slice. Any change to the default weights, bonuses, or combination rules
MUST still bump `SCORING_ALGORITHM_VERSION`.
