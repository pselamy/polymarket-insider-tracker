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

### Indexes Added:
- `idx_risk_assessments_disposition` on (`delivery_disposition`)

---

## 2. Downgrade Contract
The `downgrade()` function in `003_safe_observable_operation`:
- Drops `idx_risk_assessments_disposition`
- Drops the 8 added columns
- Returns the schema to exact `002_risk_assessments` state

---

## 3. Domain Model Synchronization
The SQLAlchemy `RiskAssessmentModel` in `src/polymarket_insider_tracker/storage/models.py` and the domain dataclass `RiskAssessment` in `src/polymarket_insider_tracker/detector/models.py` MUST expose matching fields.

---

## 4. Scoring Algorithm Versioning
The schema stores no signal weights or algorithm-version column (the human-approved
clarification enumerates exactly the 8 columns above). A stored row replays its decision
(Constitution IV, FR-012) only because:

- signal weights, multi-signal bonuses, quantization, and combination rules are constants of
  `SCORING_ALGORITHM_VERSION` (`003.1`) in `detector/scorer.py`;
- no runtime weight configuration exists — `RiskScorer` accepts no weights argument and has
  no mutation API — so every persisted row maps to exactly one algorithm;
- confidences, the threshold, and the score are quantized to the persisted `NUMERIC(4,3)`
  precision *before* the decision.

Any future change to weights, bonuses, or combination rules MUST bump
`SCORING_ALGORITHM_VERSION` and, before rows from two algorithms can coexist, obtain a schema
decision (Patrick) on persisting the version per row.
