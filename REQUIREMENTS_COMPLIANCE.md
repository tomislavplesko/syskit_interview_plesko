

## 1. Business Questions → Answers We Got

### Customer Success

| Question | Answer | Where to Find It |
|----------|--------|------------------|
| **Which customers are healthy, which are at risk, and which have gone quiet before anyone noticed?** | Health score (0–100) with tiers: Healthy (80+), Neutral (60–79), At Risk (40–59), Red Alert (0–39). "Gone quiet" = CS blind spots (churned with zero CRM contact in final 60 days). | Dashboard: Customer Health, At-Risk & Renewals, Executive Overview |
| **Which accounts are coming up for renewal, and what does their recent activity look like?** | Renewal pipeline (next 90 days) with health score, ARR, days since CS touch, event volume. Sorted by urgency. | Dashboard: At-Risk & Renewals page |

### Sales and Expansion

| Question | Answer | Where to Find It |
|----------|--------|------------------|
| **Which existing customers show signals of being ready for an upsell or seat expansion?** | Expansion candidates = Healthy/Neutral tenants on starter/business plans with growing Power Platform usage or multi-user engagement. | Dashboard: Executive Overview (count), Customer Health (filterable), At-Risk page |
| **Where in the trial-to-paid funnel are we losing the most potential customers?** | Proxy funnel: Trial → POV started → Paying → Retained → Healthy. Largest drop at **POV Started** (30.9% drop). Implication: conversion quality and onboarding, not top-of-funnel volume. | Dashboard: Trial-to-Paid Funnel page |

### Marketing

| Question | Answer | Where to Find It |
|----------|--------|------------------|
| **Which acquisition channels bring in customers who actually stay and grow?** | **Referral** and **Partner** channels: lowest churn, highest % healthy, higher avg ARR. Some paid channels bring volume but higher churn. Recommendation: shift budget toward referral incentives and partner development. | Dashboard: Marketing & Channels page; Executive Summary |

### Leadership

| Question | Answer | Where to Find It |
|----------|--------|------------------|
| **If we could predict one thing that would most change how CS spends its time next quarter, what would it be?** | **Churn risk** — which account will go quiet in the next 30 days without telling us. Model built (XGBoost), in dashboard. CS can prioritise by probability instead of renewal date alone. | Dashboard: Churn Prediction page; Executive Summary |

---


### 1. Data Engineering & Modeling

| Requirement | Status | Evidence |
|-------------|--------|----------|
| **Ingest and Unify Data** | ✅ | Pipeline combines Product Telemetry (events, users), Subscription/Billing (subscriptions), CRM (crm_companies, crm_activities) into `mart_tenant_health` and related marts. |
| **Document the Architecture** | ✅ | `architecture.md`: ERD, table grain, relationships, mart descriptions. |
| **Handle Quality Explicitly** | ✅ | `outputs/data_quality_report.csv`; every issue logged. No silent drops except unparseable event timestamps (documented). |
| **Ensure Reproducibility** | ✅ | Scripted pipeline: `00_build_sqlite_from_csv.py` → `01_ingest_and_clean.py` → `02_build_analytical_layer.py`. Fixed `random_state=42`. |

### 2. Analytical Deliverables

| Requirement | Status | Evidence |
|-------------|--------|----------|
| **Define a Health Score** | ✅ | 0–100, 5 components (active users 25, event volume 20, HV share 20, trend 20, CS touch 15). Tiers and thresholds in `architecture.md`. |
| **Segment the Base** | ✅ | At-risk cohort: health_tier At Risk/Red Alert. Usage trends: `mart_weekly_activity` (12 weeks). |
| **Answer Marketing Questions** | ✅ | `mart_channel_performance`; Marketing & Channels dashboard page with visual analysis (churn rate, % healthy, avg ARR by channel). |

### 3. Machine Learning & Prediction

| Requirement | Status | Evidence |
|-------------|--------|----------|
| **Build a Predictive Model** | ✅ | Churn risk model (XGBoost). Rationale in `ml/churn_model.py`: direct ARR impact, ground-truth label exists. |
| **Evaluate Beyond Accuracy** | ✅ | ROC-AUC, Average Precision, Precision, Recall, confusion matrix. Decision threshold (35%) with business rationale. |
| **Discuss ≥2 Real-World Limitations** | ✅ | Documented: lifecycle leakage risk, small churned sample, synthetic data, temporal leakage. |

### 4. Communication & Tools

| Requirement | Status | Evidence |
|-------------|--------|----------|
| **Publish a Live Dashboard** | ✅ | Streamlit dashboard, 7 pages. Deploy to Streamlit Community Cloud for URL. |
| **Write an Executive Summary** | ✅ | `executive_summary.md`: jargon-free, VP Customer Success & VP Sales, current state, findings, next steps. |

### Tech Stack (SQL, Python, Streamlit)

| Requirement | Status | Evidence |
|-------------|--------|----------|
| **Python** | ✅ | Pipeline, ML, dashboard in Python. |
| **Streamlit** | ✅ | `dashboard/app.py`. |
| **SQL** | ✅ | `sql/` folder: 4 scripts for ad-hoc analysis against `saas_dataset.sqlite`. |

---

## 3. Task Brief Sections (PDF) → Deliverables

| Section | Requirement | Delivered |
|---------|-------------|-----------|
| **1. Data Pipeline and Model** | Ingest, clean analytical layer, document model, handle DQ, reproducible | ✅ `architecture.md`, `pipeline/`, `outputs/data_quality_report.csv` |
| **2. Customer Analytics** | Health score, at-risk cohort, usage trend (8–12 weeks) | ✅ `mart_tenant_health`, `mart_weekly_activity` (12w), dashboard pages |
| **3. Predictive Model** | At least one model, explain metric choice, ≥2 limitations | ✅ Churn model, Precision/Recall/ROC-AUC, 4 limitations documented |
| **4. Marketing and Growth** | Visual analysis, budget recommendation | ✅ Marketing & Channels page, channel quality matrix, recommendations |
| **5. Dashboard** | Live, self-explanatory, actionable | ✅ 7 pages, filters, KPIs, next steps in Executive Summary |
| **6. Executive Summary** | VP CS & Sales, current state, 2–3 findings, next week/month/quarter | ✅ `executive_summary.md` |

---

## 4. Submission Checklist (from PDF)

| Item | Status |
|------|--------|
| Architecture decisions, pipeline description, tools | ✅ `architecture.md` |
| Pipeline / ETL code | ✅ `pipeline/00_*.py`, `01_*.py`, `02_*.py` |
| ML model code, training script, evaluation output | ✅ `ml/churn_model.py`, `outputs/churn_model_metrics.csv`, `feature_importance.csv` |
| Link to published dashboard | ⏳ Add URL after deployment |
| Short summary for leadership | ✅ `executive_summary.md` |
| Data (if generated) | ✅ `pipeline/00_build_sqlite_from_csv.py` builds SQLite from CSV |

---

## 5. Evaluation Criteria Alignment

| Criterion | How Addressed |
|-----------|---------------|
| **Judgment** | Decisions explained (health score design, churn vs expansion, lifecycle exclusion, trial funnel proxy). |
| **Technical depth** | Real pipeline, valid model, correct metrics (Precision, Recall, ROC-AUC). |
| **Business thinking** | Outputs answer the business questions; recommendations tied to findings. |
| **Communication** | Executive summary jargon-free; dashboard filterable and actionable. |

---

## 6. What to Add Before Submission

1. **Dashboard URL** — After deploying to Streamlit Community Cloud, add the link to `README.md` and your submission.
2. **GitHub repo** — Structure as requested (architecture, pipeline, ML, dashboard link, summary).

---

