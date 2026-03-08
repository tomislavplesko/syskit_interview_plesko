# SQL Scripts — Analytical Layer Documentation

These SQL scripts document the analytical layer logic and can be run directly against `saas_dataset.sqlite` for ad-hoc analysis or validation.

## Prerequisites

1. Build the SQLite database from CSVs (if not already present):
   ```bash
   python pipeline/00_build_sqlite_from_csv.py
   ```

2. Run from project root with SQLite:
   ```bash
   sqlite3 saas_dataset.sqlite < sql/01_tenant_health_summary.sql
   ```

## Scripts

| Script | Purpose | Grain |
|--------|---------|-------|
| `01_tenant_health_summary.sql` | Core tenant metrics (users, events, CS touch) | One row per tenant |
| `02_channel_performance.sql` | Acquisition channel quality (churn rate, ARR) | One row per channel |
| `03_weekly_usage_trends.sql` | Weekly event volume per tenant (12 weeks) | One row per tenant × week |
| `04_at_risk_tenants.sql` | Simplified at-risk cohort identification | One row per at-risk tenant |

## Note

The **full health score** (0–100 with 5 components) and **expansion candidates** are computed in the Python pipeline (`pipeline/02_build_analytical_layer.py`) because they require multi-window aggregations and custom threshold logic. These SQL scripts provide the foundational queries and serve as documentation of the data model.
