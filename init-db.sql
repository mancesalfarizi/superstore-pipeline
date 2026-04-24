-- ─────────────────────────────────────────────────────────────
-- Databases
-- ─────────────────────────────────────────────────────────────
CREATE DATABASE analytics;
CREATE DATABASE metabase;

CREATE USER dbt_user WITH PASSWORD 'dbt_pass';
GRANT ALL PRIVILEGES ON DATABASE analytics TO dbt_user;
GRANT ALL PRIVILEGES ON DATABASE analytics TO airflow;

-- ─────────────────────────────────────────────────────────────
-- Pipeline monitoring tables (in analytics DB)
-- ─────────────────────────────────────────────────────────────
\connect analytics

GRANT ALL ON SCHEMA public TO airflow;
GRANT ALL ON SCHEMA public TO dbt_user;

-- Tracks every DAG run (watermark + status)
CREATE TABLE IF NOT EXISTS pipeline_watermark (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100) NOT NULL,
    run_id          VARCHAR(200),
    run_type        VARCHAR(20)  NOT NULL,   -- 'initial' | 'incremental' | 'skip'
    loaded_until    DATE,                    -- max order_date loaded
    row_count       INTEGER,
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    duration_sec    NUMERIC(10,2),
    status          VARCHAR(20) DEFAULT 'running'  -- 'running' | 'success' | 'failed' | 'skipped'
);

-- Tracks every task execution with row counts + duration
CREATE TABLE IF NOT EXISTS pipeline_task_log (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100),
    run_id          VARCHAR(200),
    task_id         VARCHAR(100),
    layer           VARCHAR(50),             -- 'bronze' | 'silver' | 'gold' | 'marts'
    row_count       INTEGER,
    duration_sec    NUMERIC(10,2),
    status          VARCHAR(20),             -- 'success' | 'failed'
    error_message   TEXT,
    logged_at       TIMESTAMP DEFAULT NOW()
);

-- Tracks all alerts (errors, warnings, anomalies)
CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100),
    run_id          VARCHAR(200),
    task_id         VARCHAR(100),
    alert_type      VARCHAR(50),             -- 'error' | 'warning' | 'anomaly'
    alert_message   TEXT,
    details         JSONB,
    resolved        BOOLEAN DEFAULT FALSE,
    logged_at       TIMESTAMP DEFAULT NOW()
);
