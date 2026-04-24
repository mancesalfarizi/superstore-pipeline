-- ================================================
-- MONITORING QUERIES — Pipeline Health Dashboard
-- Buat di Metabase sebagai Tab 2: Pipeline Monitor
-- ================================================

-- QUERY 1: Pipeline Run History
-- Save as: Pipeline Run History | Visualization: Table
SELECT
    id,
    run_type,
    loaded_until,
    row_count,
    ROUND(duration_sec, 1)  AS duration_sec,
    status,
    started_at,
    finished_at
FROM pipeline_watermark
WHERE dag_id = 'superstore_pipeline'
ORDER BY started_at DESC
LIMIT 20;


-- QUERY 2: Task Duration per Run
-- Save as: Task Duration | Visualization: Bar Chart
-- X-axis: task_id, Y-axis: avg_duration_sec,max_duration_sec 
SELECT
    task_id,
    layer,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    ROUND(MAX(duration_sec), 2) AS max_duration_sec,
    COUNT(*)                    AS total_runs
FROM pipeline_task_log
WHERE dag_id = 'superstore_pipeline'
AND status = 'success'
GROUP BY task_id, layer
ORDER BY avg_duration_sec DESC;


-- QUERY 3: Row Count per Layer over Time
-- Save as: Row Count Trend | Visualization: Line Chart
SELECT
    logged_at::date     AS run_date,
    layer               AS table_name,
    row_count
FROM pipeline_task_log
WHERE dag_id = 'superstore_pipeline'
AND task_id = 'validate_row_counts'
AND status = 'success'
ORDER BY logged_at DESC
LIMIT 50;


-- QUERY 4: Pipeline Alerts
-- Save as: Pipeline Alerts | Visualization: Table
SELECT
    alert_type,
    task_id,
    alert_message,
    logged_at
FROM pipeline_alerts
WHERE dag_id = 'superstore_pipeline'
ORDER BY logged_at DESC
LIMIT 20;


-- QUERY 5: Success Rate
-- Save as: Pipeline Success Rate | Visualization: Number
SELECT
    ROUND(
        COUNT(*) FILTER (WHERE status = 'success') * 100.0 / COUNT(*),
        1
    ) AS success_rate_pct
FROM pipeline_watermark
WHERE dag_id = 'superstore_pipeline';
