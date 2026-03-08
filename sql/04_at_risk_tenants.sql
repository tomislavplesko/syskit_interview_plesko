-- =============================================================================
-- SQL: At-Risk Tenants (Segment the Base)
-- =============================================================================
-- Identifies tenants that need Customer Success attention.
-- Full health_score is computed in Python; this query provides a simplified
-- at-risk signal based on: low activity, no recent CS touch, or churned.
--
-- For the complete at-risk cohort (health_tier At Risk / Red Alert),
-- use mart_tenant_health.parquet from the Python pipeline.
-- =============================================================================

WITH event_30d AS (
    SELECT tenant_id, SUM(event_count) AS total_events_30d
    FROM events
    WHERE date(event_time) >= date('2024-06-30', '-30 days')
    GROUP BY tenant_id
),
last_cs AS (
    SELECT tenant_id, MAX(activity_date) AS last_cs_date
    FROM crm_activities
    GROUP BY tenant_id
)
SELECT
    t.tenant_id,
    t.company_name,
    t.plan,
    t.region,
    t.csm_assigned,
    s.arr,
    s.renewal_date,
    s.churned,
    COALESCE(e.total_events_30d, 0) AS total_events_30d,
    julianday('2024-06-30') - julianday(lc.last_cs_date) AS days_since_cs_touch,
    CASE
        WHEN s.churned = 1 THEN 'Churned'
        WHEN COALESCE(e.total_events_30d, 0) < 10 AND COALESCE(julianday('2024-06-30') - julianday(lc.last_cs_date), 999) > 60 THEN 'At Risk (low usage + no CS touch)'
        WHEN COALESCE(e.total_events_30d, 0) < 10 THEN 'At Risk (low usage)'
        WHEN lc.last_cs_date IS NULL OR (julianday('2024-06-30') - julianday(lc.last_cs_date)) > 90 THEN 'At Risk (no CS touch 90+ days)'
        ELSE 'Monitor'
    END AS risk_signal
FROM tenants t
JOIN subscriptions s ON t.tenant_id = s.tenant_id
LEFT JOIN event_30d e ON t.tenant_id = e.tenant_id
LEFT JOIN last_cs lc ON t.tenant_id = lc.tenant_id
WHERE s.churned = 1
   OR COALESCE(e.total_events_30d, 0) < 10
   OR lc.last_cs_date IS NULL
   OR (julianday('2024-06-30') - julianday(lc.last_cs_date)) > 60
ORDER BY s.churned DESC, s.arr DESC;
