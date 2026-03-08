# Architecture Decisions & Pipeline Documentation

## Overview

This solution ingests three operational data sources, unifies them into a clean analytical layer, computes customer health metrics, trains a churn prediction model, and publishes everything through an interactive Streamlit dashboard.

---

## Technology Stack

| Component | Tool | Rationale |
|---|---|---|
| Data processing | Python + Pandas | Industry standard; fully reproducible scripts; no infrastructure required |
| Storage | Parquet files | Columnar format; fast reads; preserves types; portable |
| Source database | SQLite | Provided format; single-file; no server needed |
| ML model | XGBoost (with GradientBoosting fallback) | Strong tabular performance; built-in feature importance; interpretable |
| Dashboard | Streamlit | Deploy to Streamlit Community Cloud in one click; self-contained Python |
| Visualisations | Plotly Express | Interactive; web-native; works inside Streamlit |

---

## Data Model (ERD)

```
tenants (tenant_id PK)
    │
    ├──< subscriptions (tenant_id FK)   [1:1 in this dataset]
    │       plan, arr, contract_start_date, renewal_date, churned, churn_date
    │
    ├──< users (tenant_id FK)           [1:many]
    │       user_id, role, registered_at, last_seen_at, is_active
    │
    ├──< events (tenant_id FK, user_id FK)  [many:many via daily aggregates]
    │       event_name, event_time, event_count, is_high_value
    │
    ├──  crm_companies (tenant_id FK)   [1:1]
    │       lifecycle_stage, acquisition_source, pov_started
    │
    └──< crm_activities (tenant_id FK)  [1:many]
            activity_type, activity_date, outcome, days_to_renewal
```

**Key relationships:**
- `tenants` is the master entity. Everything joins to it via `tenant_id`.
- `subscriptions` is effectively 1:1 (the dataset has one subscription per tenant).
- `events` grain: one row = one user × one event type × one day. Absence of a row means zero activity that day (not missing data).
- `crm_activities` grain: one row = one CS/sales touch. Can be multiple per tenant per day.

---

## Analytical Layer — Table Descriptions

### `mart_tenant_health` (one row per tenant)

The master analytical table. Joins all six source tables into a single wide view.

| Column group | Columns | Source |
|---|---|---|
| Identity | tenant_id, company_name, plan, region, industry, employee_size, csm_assigned | tenants |
| Financials | arr, contract_start_date, renewal_date, churned, churn_date | subscriptions |
| CRM | lifecycle_stage, acquisition_source, pov_started | crm_companies |
| User adoption | total_users, active_users, admin_count, active_user_pct | users |
| Usage (30/60/90d) | total_events_Xd, hv_events_Xd, active_days_Xd, active_users_Xd | events |
| Usage trend | total_events_recent_4w, total_events_prior_4w, usage_trend_ratio | events |
| HV share | hv_share_30d | events |
| CS coverage | last_cs_touch_date, days_since_cs_touch, cs_touches_90d | crm_activities |
| Health | score_active_users, score_event_volume, score_hv_share, score_trend, score_cs_touch, health_score, health_tier | computed |
| Flags | expansion_candidate, cs_blind_spot | computed |

### `mart_weekly_activity` (one row per tenant × week)

Weekly event aggregates for trend charts. Covers the last 12 weeks.

### `mart_channel_performance` (one row per acquisition source)

Channel-level summary: churn rate, avg ARR, % healthy, expansion candidates. Answers the marketing question directly.

### `mart_renewal_pipeline` (one row per tenant renewing in next 90 days)

Sorted by days to renewal. Used by the renewals page.

### `mart_trial_funnel` (5-row stage summary for trial-sourced tenants)

Because the dataset does not include explicit lead->trial->paid transition timestamps, a transparent proxy funnel is used:

1. Trial-sourced accounts (`acquisition_source = trial`)
2. POV started
3. Converted to paying (`arr > 0`)
4. Retained paying (`churned = False`)
5. Healthy retained (`health_tier in {Healthy, Neutral}`)

This directly addresses the "where in trial-to-paid are we losing accounts" question while documenting assumptions.

---

## Health Score — Design Rationale

Health score = 0–100, composed of five independent components.

| Component | Max pts | Signal | Threshold logic |
|---|---|---|---|
| Active user ratio | 25 | Are seats being used? | Linear: 0% → 0 pts, 100% → 25 pts |
| Normalised event volume | 20 | Is the product being used recently? | Ratio of 30d daily avg vs 90d baseline; capped at 2x |
| High-value event share | 20 | Are users getting real value? | 40%+ HV share = full score; linear below |
| Usage trend | 20 | Is engagement growing or shrinking? | ≥1.2x = 20 pts; 0.8–1.2x = 10 pts; <0.8x = 0 pts |
| CS coverage | 15 | Is the account being managed? | ≤30d = 15 pts; 31–60d = 10 pts; 61–90d = 5 pts; 90d+ = 0 pts |

**High-value events** (from README hints + product logic):
- `policy_created` — active governance behaviour
- `risky_workspace_resolved` — resolved a detected risk (not passive)
- `sensitivity_label_applied` — applied label (not just recommended)
- `license_recommendation_applied` — acted on optimisation recommendation

**Tiers:**
- 80–100: Healthy
- 60–79: Neutral
- 40–59: At Risk
- 0–39: Red Alert
- Churned: always Churned regardless of score

**Why these weights?** Adoption breadth (active user ratio, 25 pts) is the strongest leading indicator of retention in SaaS research. Event volume recency and depth (HV share) measure whether users are realising value. Trend direction is the earliest warning signal. CS coverage is included because it is an actionable lever — unlike usage signals, the business can change it immediately.

---

## Data Quality Decisions

Every issue is logged to `outputs/data_quality_report.csv`. Nothing is silently dropped.

| Table | Issue found | Decision |
|---|---|---|
| subscriptions | churned=True but churn_date NULL | Set churn_date = renewal_date as proxy; flagged in log |
| subscriptions | churned=False but churn_date filled | Set churned=True to match churn_date |
| users | last_seen_at < registered_at | Retained; flagged as `date_order_suspect` |
| events | event_count < 1 | Set to 1 (minimum meaningful count) |
| events | unparseable timestamp | Dropped (unusable without timestamp) |
| crm_activities | days_to_renewal < 0 | Retained — valid per spec (activity after renewal) |

**What we could not validate with this data:**
- ARR outliers (e.g., a starter plan with enterprise-level ARR) — flagged as `arr_suspect` where ARR ≤ 0, but pricing sanity checks need a plan–ARR mapping that isn't provided.
- User deduplication across tenants — `user_id` format (`usr_ten_XXX_YY`) implies uniqueness; no duplicates found.
- Event timestamps vs subscription dates — some events may pre-date contract start for tenants with long histories; not treated as errors.

---

## Reproducibility

Run these commands in order:

```bash
pip install -r requirements.txt

# Step 0: Build SQLite from CSV (skip if saas_dataset.sqlite already provided)
python pipeline/00_build_sqlite_from_csv.py

python pipeline/01_ingest_and_clean.py
python pipeline/02_build_analytical_layer.py
python ml/churn_model.py          # optional — required for Churn Prediction page
streamlit run dashboard/app.py
```

**Note:** If you have `saas_dataset.sqlite` (all six tables), skip Step 0. If you have CSV files only, run Step 0 first.

All random seeds are fixed (`random_state=42`). The pipeline is deterministic given the same source SQLite file.

---

## What I Would Improve with More Time

1. **Time-based train/test split for the ML model** — Use months 1–5 for training, month 6 for evaluation. This is the only correct way to evaluate a churn model without temporal leakage.

2. **dbt for the analytical layer** — Replace the Python mart scripts with dbt models. This gives version-controlled SQL transformations, automatic lineage documentation, and incremental materialisation.

3. **More granular health signals**:
   - Time-between-sessions (engagement consistency vs burst usage)
   - Support ticket volume and sentiment as a leading risk indicator
   - Feature adoption depth (how many of the 11 event types does each tenant use?)

4. **Expansion model** — A separate propensity-to-expand model using pp_sync growth as the label would complement the churn model. Not built here because the churn model creates more urgent business value.

5. **Automated alerting** — Pipe the health score delta (week-over-week change) into a Slack/email alert so CSMs don't need to check the dashboard.

6. **Real-time scoring** — Integrate with the live event stream so health scores update daily, not as a batch job.

---

## Missing Signals (Documented)

The following signals would substantially improve accuracy if they were available:

| Missing signal | Why it matters |
|---|---|
| NPS / CSAT scores | Direct customer sentiment — often the earliest churn signal |
| Support ticket volume & resolution time | Frustrated customers generate tickets before churning |
| Login frequency vs event frequency | A user who logs in but doesn't act is a different risk profile |
| Payment / invoice data | Failed payments are a near-perfect churn predictor |
| Product version / feature flag data | Tenants on older versions or without new features disengage faster |
| Champion contact turnover | If the primary admin leaves, churn risk spikes sharply |

---

## Delivery Notes

- Dashboard code is fully complete and runnable from `dashboard/app.py`.
- Final live URL is deployment-dependent (Streamlit Community Cloud or equivalent).  
  Add the deployed link to your submission once published.

