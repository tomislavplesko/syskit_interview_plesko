-- =============================================================================
-- SQL: Tenant Health Summary (Analytical Layer)
-- =============================================================================
-- This query documents the grain and logic of the mart_tenant_health table.
-- The Python pipeline implements this; this SQL serves as documentation and
-- can be run against saas_dataset.sqlite for ad-hoc analysis.
--
-- Grain: One row per tenant
-- =============================================================================

-- User adoption metrics per tenant
WITH user_metrics AS (
    SELECT
        tenant_id,
        COUNT(*) AS total_users,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_users,
        SUM(CASE WHEN LOWER(role) = 'admin' THEN 1 ELSE 0 END) AS admin_count
    FROM users
    GROUP BY tenant_id
),

-- Event volume in last 30 days (snapshot = 2024-06-30)
event_30d AS (
    SELECT
        tenant_id,
        SUM(event_count) AS total_events_30d,
        SUM(CASE WHEN event_name IN (
            'policy_created', 'risky_workspace_resolved',
            'sensitivity_label_applied', 'license_recommendation_applied'
        ) THEN event_count ELSE 0 END) AS hv_events_30d
    FROM events
    WHERE date(event_time) >= date('2024-06-30', '-30 days')
      AND date(event_time) <= '2024-06-30'
    GROUP BY tenant_id
),

-- Last CS touch per tenant
last_cs_touch AS (
    SELECT
        tenant_id,
        MAX(activity_date) AS last_cs_touch_date,
        julianday('2024-06-30') - julianday(MAX(activity_date)) AS days_since_cs_touch
    FROM crm_activities
    GROUP BY tenant_id
)

SELECT
    t.tenant_id,
    t.company_name,
    t.plan,
    t.region,
    s.arr,
    s.churned,
    s.renewal_date,
    COALESCE(um.total_users, 0) AS total_users,
    COALESCE(um.active_users, 0) AS active_users,
    CASE WHEN COALESCE(um.total_users, 0) > 0
         THEN 1.0 * um.active_users / um.total_users
         ELSE 0 END AS active_user_pct,
    COALESCE(e30.total_events_30d, 0) AS total_events_30d,
    COALESCE(e30.hv_events_30d, 0) AS hv_events_30d,
    lct.last_cs_touch_date,
    COALESCE(lct.days_since_cs_touch, 999) AS days_since_cs_touch,
    cc.acquisition_source,
    cc.lifecycle_stage
FROM tenants t
LEFT JOIN subscriptions s ON t.tenant_id = s.tenant_id
LEFT JOIN crm_companies cc ON t.tenant_id = cc.tenant_id
LEFT JOIN user_metrics um ON t.tenant_id = um.tenant_id
LEFT JOIN event_30d e30 ON t.tenant_id = e30.tenant_id
LEFT JOIN last_cs_touch lct ON t.tenant_id = lct.tenant_id
ORDER BY COALESCE(s.arr, 0) DESC;
