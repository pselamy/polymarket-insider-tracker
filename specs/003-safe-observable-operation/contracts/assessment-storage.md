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
1. `delivery_disposition`: `VARCHAR(32)`, `nullable=False`, `server_default='dry_run'`
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
