-- =============================================================================
-- SQL: Channel Performance (Marketing Question)
-- =============================================================================
-- Answers: Which acquisition channels bring in "good" customers who stay and grow?
-- Grain: One row per acquisition_source
--
-- Run against saas_dataset.sqlite. For full health_score logic, use the Python
-- pipeline output (mart_channel_performance.parquet).
-- =============================================================================

WITH tenant_base AS (
    SELECT
        t.tenant_id,
        t.plan,
        t.region,
        s.arr,
        s.churned,
        cc.acquisition_source,
        cc.lifecycle_stage
    FROM tenants t
    LEFT JOIN subscriptions s ON t.tenant_id = s.tenant_id
    LEFT JOIN crm_companies cc ON t.tenant_id = cc.tenant_id
    WHERE cc.acquisition_source IS NOT NULL
)

SELECT
    acquisition_source,
    COUNT(*) AS total_tenants,
    SUM(CASE WHEN churned = 1 THEN 1 ELSE 0 END) AS churned_tenants,
    ROUND(1.0 * SUM(CASE WHEN churned = 1 THEN 1 ELSE 0 END) / COUNT(*), 4) AS churn_rate,
    ROUND(AVG(arr), 2) AS avg_arr,
    ROUND(SUM(arr), 2) AS total_arr,
    SUM(CASE WHEN lifecycle_stage = 'active' AND churned = 0 THEN 1 ELSE 0 END) AS active_healthy_count
FROM tenant_base
GROUP BY acquisition_source
ORDER BY churn_rate ASC, avg_arr DESC;
