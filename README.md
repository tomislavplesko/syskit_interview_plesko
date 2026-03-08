# Syskit Customer Intelligence — Data Scientist Case Study

A complete analytical solution combining Product Telemetry, Subscription/Billing, and CRM data into a clean analytical layer, with health scoring, churn prediction, and an interactive Streamlit dashboard.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Build SQLite from CSV (if you have CSV files; skip if saas_dataset.sqlite exists)
python pipeline/00_build_sqlite_from_csv.py

# 3. Run the pipeline
python pipeline/01_ingest_and_clean.py
python pipeline/02_build_analytical_layer.py
python ml/churn_model.py          # optional — for Churn Prediction dashboard page

# 4. Launch the dashboard
streamlit run dashboard/app.py
```

**Note:** Step 1 auto-runs Step 0 if `saas_dataset.sqlite` is missing (builds from CSV files).

---

## Project Structure

| Path | Description |
|------|--------------|
| `pipeline/00_build_sqlite_from_csv.py` | Builds `saas_dataset.sqlite` from CSV files |
| `pipeline/01_ingest_and_clean.py` | Ingest, clean, data quality checks → Parquet |
| `pipeline/02_build_analytical_layer.py` | Builds marts, health score, channel performance |
| `ml/churn_model.py` | XGBoost churn prediction model |
| `dashboard/app.py` | Streamlit dashboard (7 pages) |
| `sql/` | SQL scripts for ad-hoc analysis against `saas_dataset.sqlite` |
| `architecture.md` | ERD, data model, health score design, reproducibility |
| `executive_summary.md` | Jargon-free brief for VP Customer Success & VP Sales |

---

## Data Sources

- **Product Telemetry:** `events.csv`, `users.csv`
- **Subscription/Billing:** `subscriptions.csv`
- **CRM:** `crm_companies.csv`, `crm_activities.csv`
- **Master:** `tenants.csv`

See `README_dataset.md` for full schema and hints.

---

## Dashboard

**Local:** `streamlit run dashboard/app.py`

**Deployed URL:** *(Add your Streamlit Community Cloud or deployment URL here after publishing)*

---

## SQL Scripts

Run against `saas_dataset.sqlite` for ad-hoc analysis:

```bash
sqlite3 saas_dataset.sqlite < sql/01_tenant_health_summary.sql
sqlite3 saas_dataset.sqlite < sql/02_channel_performance.sql
sqlite3 saas_dataset.sqlite < sql/03_weekly_usage_trends.sql
sqlite3 saas_dataset.sqlite < sql/04_at_risk_tenants.sql
```

See `sql/README.md` for details.

---

## Requirements Checklist (Client)

| Area | Delivered |
|------|-----------|
| **Data Engineering** | Ingest & unify, ERD, data quality documented, reproducible pipeline |
| **Analytical Deliverables** | Health score, at-risk cohort, usage trends (12w), channel performance |
| **ML & Prediction** | Churn model, Precision/Recall/ROC-AUC, documented limitations |
| **Communication** | Live dashboard, Executive Summary |
