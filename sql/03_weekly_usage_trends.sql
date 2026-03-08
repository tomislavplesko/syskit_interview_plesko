-- =============================================================================
-- SQL: Weekly Usage Trends (Last 12 Weeks)
-- =============================================================================
-- Grain: One row per tenant × week
-- Used for usage trends over 8-12 weeks (requirement: "view of usage trends")
--
-- SQLite week: strftime('%Y-%W', event_time) gives ISO week
-- =============================================================================

SELECT
    tenant_id,
    strftime('%Y-%W', event_time) AS week_key,
    SUM(event_count) AS event_count,
    COUNT(DISTINCT date(event_time)) AS active_days,
    COUNT(DISTINCT user_id) AS active_users
FROM events
WHERE date(event_time) >= date('2024-06-30', '-84 days')  -- 12 weeks
  AND date(event_time) <= '2024-06-30'
GROUP BY tenant_id, strftime('%Y-%W', event_time)
ORDER BY tenant_id, week_key;
