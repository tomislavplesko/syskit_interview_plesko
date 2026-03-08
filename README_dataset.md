# SaaS Assessment Dataset — README

This dataset was created for the Data Analyst / Scientist home task. It represents a simplified but realistic view of a B2B SaaS product — the kind of data you'd encounter working with product telemetry, customer subscriptions, and CRM activity.

---

## Files

| File | Format | Rows | Description |
|---|---|---|---|
| `tenants.csv` | CSV | ~500 | One row per company/tenant |
| `subscriptions.csv` | CSV | ~500 | Subscription, ARR, and renewal info per tenant |
| `users.csv` | CSV | ~2,800 | One row per user within a tenant |
| `events.csv` | CSV | ~410,000 | Daily product usage telemetry |
| `crm_companies.csv` | CSV | ~500 | CRM account-level metadata |
| `crm_activities.csv` | CSV | ~3,500 | CRM activity log (calls, emails, meetings) |
| `saas_dataset.sqlite` | SQLite | — | All six tables in one queryable file |

---

## Table Schemas

### `tenants`

| Column | Type | Description |
|---|---|---|
| `tenant_id` | STRING | Unique tenant identifier (e.g. `ten_001`) |
| `company_name` | STRING | Company name |
| `plan` | STRING | `starter`, `business`, `enterprise` |
| `region` | STRING | `EMEA`, `NAM`, `APAC`, `LATAM` |
| `industry` | STRING | Finance, Healthcare, Retail, Technology, Government, Manufacturing, Education, Legal |
| `employee_size` | STRING | `1-50`, `51-200`, `201-1000`, `1000+` |
| `csm_assigned` | STRING | Name of the assigned Customer Success Manager |

---

### `subscriptions`

| Column | Type | Description |
|---|---|---|
| `subscription_id` | STRING | Unique subscription ID |
| `tenant_id` | STRING | FK → `tenants.tenant_id` |
| `plan` | STRING | `starter`, `business`, `enterprise` |
| `arr` | FLOAT | Annual Recurring Revenue (USD) |
| `contract_start_date` | DATE | When the subscription started (YYYY-MM-DD) |
| `renewal_date` | DATE | Upcoming renewal date |
| `churned` | BOOLEAN | `True` if the tenant has churned |
| `churn_date` | DATE | Date of churn — NULL if tenant is active |

---

### `users`

| Column | Type | Description |
|---|---|---|
| `user_id` | STRING | Unique user ID (e.g. `usr_ten_001_01`) |
| `tenant_id` | STRING | FK → `tenants.tenant_id` |
| `role` | STRING | `admin`, `member`, `read-only` |
| `registered_at` | DATE | Date the user was first activated |
| `last_seen_at` | DATE | Most recent login date |
| `is_active` | BOOLEAN | True if active within the last 30 days |

---

### `events`

| Column | Type | Description |
|---|---|---|
| `event_id` | STRING | Unique event row ID |
| `tenant_id` | STRING | FK → `tenants.tenant_id` |
| `user_id` | STRING | FK → `users.user_id` |
| `event_name` | STRING | Name of the product event (see list below) |
| `event_time` | TIMESTAMP | UTC timestamp of the event (YYYY-MM-DDTHH:MM:SS) |
| `event_count` | INT | Number of times this event was triggered that day by that user |
| `properties` | JSON | Optional event-specific metadata (nullable) |

**Available `event_name` values:**

| Event | Description |
|---|---|
| `report_generated` | User generated a report |
| `policy_created` | A governance policy was created |
| `policy_updated` | An existing policy was updated |
| `risky_workspace_detected` | System detected a risky workspace |
| `risky_workspace_resolved` | User resolved a risky workspace |
| `sensitivity_label_applied` | Sensitivity label applied to content |
| `sensitivity_label_recommended` | Label was recommended (not yet applied) |
| `pp_sync_started` | Power Platform sync initiated |
| `pp_sync_completed` | Power Platform sync completed |
| `license_optimization_viewed` | User viewed license optimization recommendations |
| `license_recommendation_applied` | User applied a license recommendation |

**Date range:** 180 days of history ending 2024-06-30.
**Note:** `event_count` reflects aggregated daily activity per user per event type. Rows only exist for days with activity — absence of a row means no activity.

---

### `crm_companies`

Account-level CRM metadata — one row per tenant.

| Column | Type | Description |
|---|---|---|
| `company_id` | STRING | Unique CRM company ID |
| `tenant_id` | STRING | FK → `tenants.tenant_id` |
| `lifecycle_stage` | STRING | `onboarding`, `active`, `at-risk`, `churned` |
| `acquisition_source` | STRING | `inbound`, `outbound`, `partner`, `trial`, `referral` |
| `pov_started` | BOOLEAN | Whether a Proof of Value engagement was started |
| `created_at` | DATE | CRM record creation date |
| `region` | STRING | Matches `tenants.region` |
| `industry` | STRING | Matches `tenants.industry` |

---

### `crm_activities`

Activity log — individual CRM touches per tenant.

| Column | Type | Description |
|---|---|---|
| `activity_id` | STRING | Unique activity ID |
| `tenant_id` | STRING | FK → `tenants.tenant_id` |
| `activity_type` | STRING | `email_sent`, `call_completed`, `meeting_held`, `support_ticket`, `qbr_completed` |
| `activity_date` | DATE | Date of the activity |
| `outcome` | STRING | `positive`, `neutral`, `negative`, `no_response` |
| `days_to_renewal` | INT | Days between this activity and the tenant's renewal date (can be negative) |

---

## Relationships

```
tenants       (1) ──< subscriptions  (1:1 in this dataset)
tenants       (1) ──< users          (many)
tenants       (1) ──< events         (many)
tenants       (1) ──  crm_companies  (1:1)
tenants       (1) ──< crm_activities (many)
users         (1) ──< events         (many)
```

---

## Data Quality Notes

- `churn_date` is NULL for active tenants — handle accordingly (`IS NOT NULL`, not `!= ''`)
- `event_count` is a daily aggregate per user per event — not a raw event log
- `events` only contains rows for active days — no row = no activity (not missing data)
- `days_to_renewal` in `crm_activities` can be negative (activity after renewal date)
- `properties` in `events` is nullable — some event types have no additional metadata
- A small number of tenants have no CRM activity — intentional (see hints)

---

## Hints (read only if you're stuck)

<details>
<summary>Hint 1 — Churn signal</summary>
Look at how event activity changes in the weeks before <code>churn_date</code>. Compare average daily <code>event_count</code> in two windows: 45–90 days before churn vs. 0–45 days before churn. Which event types drop the most?
</details>

<details>
<summary>Hint 2 — Engagement / adoption score</summary>
Not all events indicate the same level of value. Which events suggest a user is actively governing their environment vs. just browsing? Consider weighting <code>policy_created</code>, <code>risky_workspace_resolved</code>, and <code>sensitivity_label_applied</code> more heavily.
</details>

<details>
<summary>Hint 3 — Expansion candidates</summary>
Look at tenants where <code>pp_sync_completed</code> and <code>pp_sync_started</code> events are growing over time AND where multiple users are actively engaged. Cross-reference with plan type.
</details>

<details>
<summary>Hint 4 — CS blind spot</summary>
Some tenants that churned had no CRM activity in their final 60 days before churn. Can you identify them via <code>crm_activities</code>? What do their usage patterns look like vs. tenants that did get outreach?
</details>

<details>
<summary>Hint 5 — WAU calculation</summary>
Use <code>date(event_time)</code> to extract the date from the timestamp. For weekly aggregation, truncate to the week start using <code>strftime('%Y-%W', event_time)</code> in SQLite or <code>date_trunc('week', event_time)</code> in PostgreSQL-style dialects.
</details>

---

## How to Use the SQLite File

**Python:**
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect("saas_dataset.sqlite")
df = pd.read_sql("SELECT * FROM events LIMIT 10", conn)
```

**DBeaver / TablePlus / DB Browser for SQLite:**
Open `saas_dataset.sqlite` directly — no credentials needed.

**Command line:**
```bash
sqlite3 saas_dataset.sqlite
.tables
SELECT COUNT(*) FROM events;
SELECT event_name, COUNT(*) FROM events GROUP BY event_name;
```

---

## Questions?

If anything is unclear about the data, feel free to reach out — we'd rather you ask than guess.

