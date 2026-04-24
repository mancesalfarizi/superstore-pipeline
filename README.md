# Superstore Analytics Pipeline

End-to-end data pipeline built with medallion architecture and 8 production-grade features.

**Stack:** PostgreSQL · dbt · Apache Airflow · Metabase  
**Dataset:** Superstore (Kaggle) — 9,994 rows

---

## Production Features

| # | Feature | Implementation |
|---|---|---|
| 1 | Monitoring | Task duration + DAG run logging to PostgreSQL |
| 2 | Row count validation | Per-layer empty check + anomaly detection (>20% drop) |
| 3 | Pipeline metadata | All events recorded in `pipeline_task_log` |
| 4 | Incremental load | Watermark-based — only loads data after last successful run |
| 5 | Alert logging | Errors, warnings, and anomalies written to `pipeline_alerts` |
| 6 | Retry + backoff | 3 retries with exponential delay: 1 min → 2 min → 4 min |
| 7 | FileSensor | Waits for CSV presence in target folder before pipeline starts |
| 8 | No-data branching | Automatically skips pipeline if no new data is detected |

---

## Pipeline Flow

```
wait_for_csv (FileSensor)
        ↓
check_load_type (Branch)
    ↓ initial               ↓ incremental
prepare_initial        prepare_incremental
    └──────────┬────────────┘
           join_prepare
               ↓
     check_has_new_data (Branch)
      ↓ has data              ↓ no data
   run_pipeline           skip_pipeline
         ↓
      dbt_seed
         ↓
    dbt_run_bronze
         ↓
    dbt_run_silver   ← incremental
         ↓
     dbt_run_gold    ← incremental
         ↓
    dbt_run_marts
         ↓
      dbt_test
         ↓
 validate_row_counts
         ↓
  update_watermark
```

---

## Medallion Architecture

```
CSV (Kaggle)
    ↓
Bronze:  raw_superstore           (9,994 rows · table)
    ↓
Silver:  stg_orders               (incremental)
         stg_customers            (view)
         stg_products             (view · ROW_NUMBER dedup)
         stg_locations            (view)
    ↓
Gold:    fct_sales                (incremental · 9,994 rows · table)
         dim_customer             (793 rows · table)
         dim_product              (1,862 rows · table)
    ↓
Marts:   mart_sales_by_region     (49 rows)
         mart_sales_by_category   (17 rows)
         mart_sales_by_segment    (3 rows)
```

---

## Getting Started

### Step 1 — Download the Dataset

Download from Kaggle:
```
https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
```

Place `Sample - Superstore.csv` in the `data/` folder.

---

### Step 2 — Start Docker

```bash
docker-compose up -d
```

Wait 3–5 minutes, then verify all containers are running:

```bash
docker ps
```

Expected containers:

| Container | Status |
|---|---|
| `superstore_postgres` | healthy |
| `superstore_airflow_webserver` | port 8080 |
| `superstore_airflow_scheduler` | port 8081 |
| `superstore_metabase` | port 3000 |

---

### Step 3 — Initialize dbt

Enter the scheduler container:

```bash
docker exec -it superstore_airflow_scheduler bash
```

Inside the container:

```bash
cd /opt/airflow/dbt
dbt init superstore
```

Select `1` (postgres), then copy the profiles file:

```bash
mkdir -p /home/airflow/.dbt
cp /opt/airflow/dbt/superstore/profiles.yml /home/airflow/.dbt/profiles.yml
```

Test the connection:

```bash
cd /opt/airflow/dbt/superstore
dbt debug
```

Expected output: `Connection test: [OK connection ok]`

---

### Step 4 — Configure FileSensor Connection

Before triggering the DAG, create the FileSensor connection in the Airflow UI:

1. Open http://localhost:8080
2. Go to **Admin** → **Connections**
3. Click **+** to add a new connection
4. Fill in the following:

| Field | Value |
|---|---|
| Connection Id | `fs_default` |
| Connection Type | `File (path)` |
| Path | `/opt/airflow/data` |

5. Click **Save**

> ⚠️ Without this step, the `wait_for_csv` task will fail with: `The conn_id 'fs_default' isn't defined`

---

### Step 5 — Trigger the DAG

1. Open http://localhost:8080
2. Log in with `admin` / `admin`
3. Find the `superstore_pipeline` DAG
4. Toggle it **ON**, then click ▶ **Trigger DAG**

| Run | Type | Expected Behavior |
|---|---|---|
| Run 1 | Initial Load | Loads 2014–2016 data (7,292 rows) |
| Run 2 | Incremental Load | Appends 2017 data (9,994 rows total) |
| Run 3 | Skip | No new data — pipeline skips cleanly |

Trigger each run manually and wait for the previous run to complete before triggering the next.

#### Verifying Data After Each Run

Run the following query in DBeaver to confirm data loaded correctly:

```sql
SELECT
    EXTRACT(YEAR FROM order_date) AS year,
    COUNT(*)                      AS row_count,
    ROUND(SUM(sales), 2)          AS total_sales
FROM fct_sales
GROUP BY year
ORDER BY year;
```

**After Run 1 (Initial Load):**

| year | row_count | total_sales |
|------|-----------|-------------|
| 2014 | 1,994 | 484,247 |
| 2015 | 1,880 | 470,532 |
| 2016 | 3,418 | 609,205 |
| **Total** | **7,292** | |

**After Run 2 (Incremental Load):**

| year | row_count | total_sales |
|------|-----------|-------------|
| 2014 | 1,994 | 484,247 |
| 2015 | 1,880 | 470,532 |
| 2016 | 3,418 | 609,205 |
| 2017 | 2,702 | 733,215 |
| **Total** | **9,994** | |

**After Run 3 (Skip):** No change — still 9,994 rows.

Check the watermark table:

```sql
SELECT run_type, row_count, loaded_until, status, started_at
FROM pipeline_watermark
WHERE dag_id = 'superstore_pipeline'
ORDER BY started_at DESC;
```

---

### Step 6 — Connect Metabase

1. Open http://localhost:3000
2. Complete the Metabase initial setup
3. Under **Add your data**, select **PostgreSQL** and enter:

| Field | Value |
|---|---|
| Host | `postgres` |
| Port | `5432` |
| Database | `analytics` |
| Username | `airflow` |
| Password | `airflow` |

> ⚠️ Use `postgres` as the host (Docker service name), not `localhost`

4. Build questions from the queries in `metabase-queries/`

---

### Step 7 — Build Metabase Dashboards

#### Dashboard 1: Superstore Analytics

Go to **+ New** → **SQL query** → select the **Superstore Analytics** database.

Create the following questions and save them to the **Our Analytics** collection:

**KPI Cards (01_kpi_cards.sql) — 5 separate questions**

| Query | Question Name | Visualization |
|---|---|---|
| Total Revenue | `Total Revenue` | Number |
| Total Profit | `Total Profit` | Number |
| Total Orders | `Total Orders` | Number |
| Total Customers | `Total Customers` | Number |
| Avg Days to Ship | `Avg Days to Ship` | Number |

**Revenue by Category (02_revenue_by_category.sql)**

| Setting | Value |
|---|---|
| Name | `Revenue by Category` |
| Visualization | Bar Chart |
| X-axis | `sub_category` |
| Y-axis | `total_revenue` only — remove `total_orders`, `total_profit`, `avg_margin` |

**Revenue by Region (03_revenue_by_region.sql)**

| Setting | Value |
|---|---|
| Name | `Revenue by Region` |
| Visualization | Bar Chart |
| X-axis | `region` |
| Y-axis | `total_revenue` |

**Revenue by Segment (04_revenue_by_segment.sql)**

| Setting | Value |
|---|---|
| Name | `Revenue by Segment` |
| Visualization | Bar Chart |
| X-axis | `segment` |
| Y-axis | `total_revenue` only — remove other metrics |

**Top 10 Products (05_top_10_products.sql)**

| Setting | Value |
|---|---|
| Name | `Top 10 Products` |
| Visualization | Table |
| Columns | product_name, category, total_orders, total_revenue, total_profit |

**Dashboard 1 layout:**

```
Row 1: Total Revenue | Total Profit | Total Orders | Total Customers | Avg Days to Ship
Row 2: Revenue by Segment              | Revenue by Region
Row 3: Revenue by Category             (full width)
Row 4: Top 10 Products                 (full width)
```

---

#### Dashboard 2: Pipeline Monitor

Create a new dashboard named **Pipeline Monitor**.

Build questions from `06_pipeline_monitoring.sql`:

**Pipeline Run History**

| Setting | Value |
|---|---|
| Name | `Pipeline Run History` |
| Visualization | Table |
| Columns | id, run_type, loaded_until, row_count, duration_sec, status, started_at |

**Task Duration**

| Setting | Value |
|---|---|
| Name | `Task Duration per Run` |
| Visualization | Bar Chart |
| X-axis | `task_id` |
| Y-axis | `avg_duration_sec` |

**Row Count Trend**

| Setting | Value |
|---|---|
| Name | `Row Count per Layer over Time` |
| Visualization | Line Chart |
| X-axis | `run_date` |
| Y-axis | `row_count` |
| Series | `table_name` |

**Pipeline Alerts**

| Setting | Value |
|---|---|
| Name | `Pipeline Alerts` |
| Visualization | Table |
| Columns | alert_type, task_id, alert_message, logged_at |

**Pipeline Success Rate**

| Setting | Value |
|---|---|
| Name | `Pipeline Success Rate` |
| Visualization | Number |

**Dashboard 2 layout:**

```
Row 1: Pipeline Success Rate   | (empty)
Row 2: Pipeline Run History    (full width)
Row 3: Task Duration           | Row Count Trend
Row 4: Pipeline Alerts         (full width)
```

---

### Step 8 — dbt Docs (Optional)

```bash
docker exec -it superstore_airflow_scheduler bash -c "
  cd /opt/airflow/dbt/superstore &&
  dbt docs generate &&
  dbt docs serve --port 8081"
```

Open: http://localhost:8081

---

## Services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | Set during first-time setup |
| dbt Docs | http://localhost:8081 | — |
| PostgreSQL | localhost:5432 | airflow / airflow |

---

## Monitoring Tables

| Table | Description |
|---|---|
| `pipeline_watermark` | DAG run history — status, row count, duration, loaded_until |
| `pipeline_task_log` | Per-task detail — layer, row count, duration |
| `pipeline_alerts` | Detected errors, warnings, and anomalies |

---

## Troubleshooting

| Error | Solution |
|---|---|
| `fs_default isn't defined` | Create the FileSensor connection as described in Step 4 |
| FileSensor timeout | Ensure `Sample - Superstore.csv` exists in the `data/` folder |
| `dbt: command not found` | Run: `cp /opt/airflow/dbt/superstore/profiles.yml /home/airflow/.dbt/profiles.yml` |
| `fct_sales` row count > 9,994 | Ensure `stg_products.sql` uses `ROW_NUMBER()` for deduplication |
| Table not found in Metabase | Select database `Superstore Analytics`, not `Sample Database` |
| Cannot connect to `postgres` from DBeaver | Use `localhost` as the host in DBeaver (not in Metabase) |
| Watermark not updating | Check `pipeline_alerts` table for error details |
| Port 8081 refused | Verify `ports: 8081:8081` is set for `airflow-scheduler` in `docker-compose.yml` |
