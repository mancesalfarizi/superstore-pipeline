"""
Superstore Analytics Pipeline DAG
===================================
Features:
  1. Monitoring          — task duration + DAG run logging
  2. Row count validation — cek setiap layer tidak 0 + anomaly detection
  3. Pipeline metadata    — log semua events ke PostgreSQL
  4. Incremental load     — watermark-based, hanya load data baru
  5. Alert logging        — semua error/warning masuk ke pipeline_alerts
  6. Retry + backoff      — retry 3x dengan delay makin lama
  7. FileSensor           — tunggu CSV ada sebelum pipeline jalan
  8. No-data branching    — skip pipeline kalau tidak ada data baru
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import psycopg2
import psycopg2.extras
import json
import time
import os

# ── Config ────────────────────────────────────────────────────
DBT_DIR      = '/opt/airflow/dbt/superstore'
DATA_DIR     = '/opt/airflow/data'
DBT_BIN      = '/home/airflow/.local/bin/dbt'
DBT_PROFILES = '--profiles-dir /home/airflow/.dbt'
CSV_FILE     = f'{DATA_DIR}/Sample - Superstore.csv'

DB_CONN = {
    'host':     'postgres',
    'port':     5432,
    'dbname':   'analytics',
    'user':     'airflow',
    'password': 'airflow'
}

# ── Feature 6: Retry with exponential backoff ─────────────────
default_args = {
    'owner': 'salman',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,   # 1min → 2min → 4min
    'max_retry_delay': timedelta(minutes=10),
}


# ── Helper: DB connection ─────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONN)


# ── Helper: log alert to DB ───────────────────────────────────
def log_alert(run_id, task_id, alert_type, message, details=None):
    """Feature 5: Log alert to pipeline_alerts table."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_alerts
                (dag_id, run_id, task_id, alert_type, alert_message, details)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            'superstore_pipeline',
            run_id,
            task_id,
            alert_type,
            message,
            json.dumps(details) if details else None
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Could not log alert: {e}")


# ── Helper: log task to DB ────────────────────────────────────
def log_task(run_id, task_id, layer, row_count, duration_sec, status, error=None):
    """Feature 1+3: Log task execution metadata."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_task_log
                (dag_id, run_id, task_id, layer, row_count, duration_sec, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            'superstore_pipeline',
            run_id, task_id, layer,
            row_count, duration_sec, status, error
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Could not log task: {e}")


# ── Feature 1: on_failure_callback ───────────────────────────
def on_failure_callback(context):
    """Log failure to DB + update watermark status."""
    task_id  = context['task_instance'].task_id
    run_id   = context['run_id']
    error    = str(context.get('exception', 'Unknown error'))

    print(f"""
    ❌ TASK FAILED
    DAG:    superstore_pipeline
    Task:   {task_id}
    Run ID: {run_id}
    Error:  {error}
    """)

    # Log alert
    log_alert(run_id, task_id, 'error', f'Task {task_id} failed: {error}',
              {'exception': error})

    # Update watermark
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE pipeline_watermark
            SET status = 'failed', finished_at = NOW()
            WHERE dag_id = 'superstore_pipeline'
            AND run_id = %s
            AND status = 'running'
        """, (run_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Could not update watermark on failure: {e}")


# ── Feature 4: check_load_type ────────────────────────────────
def check_load_type(**context):
    """
    Branch: initial load or incremental load?
    Initial  → first ever successful run
    Incremental → subsequent runs
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM pipeline_watermark
        WHERE dag_id = 'superstore_pipeline'
        AND status = 'success'
    """)
    count = cur.fetchone()[0]
    conn.close()

    if count == 0:
        print("No previous successful run → INITIAL LOAD")
        return 'prepare_initial'
    else:
        print(f"{count} previous successful runs found → INCREMENTAL LOAD")
        return 'prepare_incremental'


# ── Feature 4: prepare_initial ───────────────────────────────
def prepare_initial(**context):
    """
    Initial load: load semua data 2014-2016.
    Simulate: data sudah tersedia dari awal, 2017 akan datang nanti.
    """
    start = time.time()
    run_id = context['run_id']

    df = pd.read_csv(CSV_FILE, encoding='latin1')
    df['Order Date'] = pd.to_datetime(df['Order Date'])

    initial = df[df['Order Date'].dt.year <= 2016].copy()
    initial['Order Date'] = initial['Order Date'].dt.strftime('%m/%d/%Y')
    initial['Ship Date']  = pd.to_datetime(initial['Ship Date']).dt.strftime('%m/%d/%Y')

    output = f'{DBT_DIR}/seeds/superstore.csv'
    initial.to_csv(output, index=False, encoding='utf-8')

    duration = round(time.time() - start, 2)
    row_count = len(initial)

    print(f"✅ Initial load: {row_count} rows (2014-2016) → {output} [{duration}s]")

    # Log to watermark
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_watermark
            (dag_id, run_id, run_type, row_count, status)
        VALUES (%s, %s, 'initial', %s, 'running')
        RETURNING id
    """, ('superstore_pipeline', run_id, row_count))
    wm_id = cur.fetchone()[0]
    conn.commit()
    conn.close()

    log_task(run_id, 'prepare_initial', 'ingestion', row_count, duration, 'success')

    context['ti'].xcom_push(key='watermark_id', value=wm_id)
    context['ti'].xcom_push(key='row_count', value=row_count)
    context['ti'].xcom_push(key='has_new_data', value=True)


# ── Feature 4+8: prepare_incremental ─────────────────────────
def prepare_incremental(**context):
    """
    Incremental load: load hanya data setelah watermark terakhir.
    Feature 8: kalau tidak ada data baru, set has_new_data=False → skip.
    """
    start  = time.time()
    run_id = context['run_id']

    # Get last loaded date
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT loaded_until FROM pipeline_watermark
        WHERE dag_id = 'superstore_pipeline' AND status = 'success'
        ORDER BY finished_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    last_loaded = row[0] if row else None
    conn.close()

    print(f"Last loaded until: {last_loaded}")

    df = pd.read_csv(CSV_FILE, encoding='latin1')
    df['Order Date'] = pd.to_datetime(df['Order Date'])

    if last_loaded:
        new_data = df[df['Order Date'].dt.date > last_loaded].copy()
    else:
        new_data = df[df['Order Date'].dt.year == 2017].copy()

    # ── Feature 8: No-data branch ─────────────────────────────
    if len(new_data) == 0:
        print("⚠️ No new data found — pipeline will be skipped")
        log_alert(run_id, 'prepare_incremental', 'warning',
                  'No new data found, pipeline skipped',
                  {'last_loaded': str(last_loaded)})

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_watermark
                (dag_id, run_id, run_type, row_count, status, finished_at)
            VALUES (%s, %s, 'skip', 0, 'skipped', NOW())
        """, ('superstore_pipeline', run_id))
        conn.commit()
        conn.close()

        context['ti'].xcom_push(key='has_new_data', value=False)
        context['ti'].xcom_push(key='watermark_id', value=None)
        return

    new_data['Order Date'] = new_data['Order Date'].dt.strftime('%m/%d/%Y')
    new_data['Ship Date']  = pd.to_datetime(new_data['Ship Date']).dt.strftime('%m/%d/%Y')

    output = f'{DBT_DIR}/seeds/superstore.csv'
    new_data.to_csv(output, index=False, encoding='utf-8')

    duration  = round(time.time() - start, 2)
    row_count = len(new_data)
    print(f"✅ Incremental: {row_count} new rows → {output} [{duration}s]")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_watermark
            (dag_id, run_id, run_type, row_count, status)
        VALUES (%s, %s, 'incremental', %s, 'running')
        RETURNING id
    """, ('superstore_pipeline', run_id, row_count))
    wm_id = cur.fetchone()[0]
    conn.commit()
    conn.close()

    log_task(run_id, 'prepare_incremental', 'ingestion', row_count, duration, 'success')

    context['ti'].xcom_push(key='watermark_id', value=wm_id)
    context['ti'].xcom_push(key='has_new_data', value=True)
    context['ti'].xcom_push(key='row_count', value=row_count)


# ── Feature 8: check_has_new_data ────────────────────────────
def check_has_new_data(**context):
    """
    Branch after prepare_incremental:
    - has new data  → run_pipeline
    - no new data   → skip_pipeline
    """
    # Check both possible sources
    has_new = context['ti'].xcom_pull(
        key='has_new_data',
        task_ids=['prepare_initial', 'prepare_incremental']
    )
    has_new = next((x for x in (has_new or []) if x is not None), False)

    if has_new:
        print("✅ New data available → running pipeline")
        return 'run_pipeline'
    else:
        print("⏭️ No new data → skipping pipeline")
        return 'skip_pipeline'


# ── Feature 2: validate_row_counts ───────────────────────────
def validate_row_counts(**context):
    """
    Feature 2: Validate row counts after each layer:
    - Row count must not be 0
    - Row count anomaly: must not drop > 20% vs previous run
    """
    start  = time.time()
    run_id = context['run_id']

    conn = get_conn()
    cur = conn.cursor()

    # Get previous run counts for anomaly detection
    cur.execute("""
        SELECT layer, row_count FROM pipeline_task_log
        WHERE dag_id = 'superstore_pipeline'
        AND task_id = 'validate_row_counts'
        AND status = 'success'
        ORDER BY logged_at DESC
        LIMIT 10
    """)
    prev_counts = {row[0]: row[1] for row in cur.fetchall()}

    checks = {
        'raw_superstore': 'SELECT COUNT(*) FROM raw_superstore',
        'stg_orders':     'SELECT COUNT(*) FROM stg_orders',
        'fct_sales':      'SELECT COUNT(*) FROM fct_sales',
        'dim_customer':   'SELECT COUNT(*) FROM dim_customer',
        'dim_product':    'SELECT COUNT(*) FROM dim_product',
    }

    results = {}
    for table, query in checks.items():
        cur.execute(query)
        count = cur.fetchone()[0]
        results[table] = count

        # Check: not zero
        if count == 0:
            log_alert(run_id, 'validate_row_counts', 'error',
                      f'Table {table} has 0 rows!', {'table': table})
            conn.close()
            raise ValueError(f"❌ VALIDATION FAILED: {table} has 0 rows!")

        # Check: anomaly (drop > 20% vs previous run)
        if table in prev_counts and prev_counts[table] > 0:
            prev = prev_counts[table]
            drop_pct = (prev - count) / prev * 100
            if drop_pct > 20:
                msg = f"Row count anomaly: {table} dropped {drop_pct:.1f}% ({prev} → {count})"
                print(f"⚠️ WARNING: {msg}")
                log_alert(run_id, 'validate_row_counts', 'anomaly', msg,
                          {'table': table, 'previous': prev, 'current': count, 'drop_pct': drop_pct})

        print(f"✅ {table}: {count} rows")

        # Log each table check
        cur.execute("""
            INSERT INTO pipeline_task_log
                (dag_id, run_id, task_id, layer, row_count, status)
            VALUES (%s, %s, 'validate_row_counts', %s, %s, 'success')
        """, ('superstore_pipeline', run_id, table, count))

    duration = round(time.time() - start, 2)
    conn.commit()
    conn.close()

    log_task(run_id, 'validate_row_counts', 'validation',
             results.get('fct_sales', 0), duration, 'success')

    print(f"\n✅ All validations passed in {duration}s: {results}")
    context['ti'].xcom_push(key='validation_results', value=results)


# ── Feature 3+4: update_watermark ────────────────────────────
def update_watermark(**context):
    """
    Feature 3+4: Update watermark with max order_date loaded.
    Marks pipeline run as 'success'.
    """
    run_id = context['run_id']

    wm_ids = context['ti'].xcom_pull(
        key='watermark_id',
        task_ids=['prepare_initial', 'prepare_incremental']
    )
    wm_id = next((x for x in (wm_ids or []) if x is not None), None)

    if not wm_id:
        print("No watermark_id found — skipping")
        return

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT MAX(order_date) FROM fct_sales")
    max_date = cur.fetchone()[0]

    cur.execute("""
        UPDATE pipeline_watermark
        SET loaded_until  = %s,
            finished_at   = NOW(),
            duration_sec  = EXTRACT(EPOCH FROM (NOW() - started_at)),
            status        = 'success'
        WHERE id = %s
    """, (max_date, wm_id))

    conn.commit()
    conn.close()
    print(f"✅ Watermark updated: loaded_until = {max_date}, run_id = {run_id}")


# ── DAG ───────────────────────────────────────────────────────
with DAG(
    dag_id='superstore_pipeline',
    default_args={**default_args, 'on_failure_callback': on_failure_callback},
    description='Superstore medallion pipeline — 8 production features',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['superstore', 'dbt', 'medallion', 'incremental'],
) as dag:

    # ── Feature 7: FileSensor ─────────────────────────────────
    wait_for_csv = FileSensor(
        task_id='wait_for_csv',
        filepath=CSV_FILE,
        poke_interval=30,       # cek setiap 30 detik
        timeout=300,            # timeout setelah 5 menit
        mode='poke',
    )

    # ── Feature 4: Branch initial vs incremental ──────────────
    check_load = BranchPythonOperator(
        task_id='check_load_type',
        python_callable=check_load_type,
    )

    prepare_init = PythonOperator(
        task_id='prepare_initial',
        python_callable=prepare_initial,
    )

    prepare_incr = PythonOperator(
        task_id='prepare_incremental',
        python_callable=prepare_incremental,
    )

    # Join after prepare
    join_prepare = EmptyOperator(
        task_id='join_prepare',
        trigger_rule='none_failed_min_one_success',
    )

    # ── Feature 8: Branch has_new_data ───────────────────────
    check_data = BranchPythonOperator(
        task_id='check_has_new_data',
        python_callable=check_has_new_data,
        trigger_rule='none_failed_min_one_success',
    )

    run_pipeline = EmptyOperator(task_id='run_pipeline')
    skip_pipeline = EmptyOperator(task_id='skip_pipeline')

    # ── dbt tasks ─────────────────────────────────────────────
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} seed {DBT_PROFILES}',
    )

    dbt_run_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select bronze {DBT_PROFILES}',
    )

    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select silver {DBT_PROFILES}',
    )

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select gold {DBT_PROFILES}',
    )

    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select marts {DBT_PROFILES}',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} test {DBT_PROFILES}',
    )

    # ── Feature 2: Validate row counts ───────────────────────
    validate = PythonOperator(
        task_id='validate_row_counts',
        python_callable=validate_row_counts,
    )

    # ── Feature 3+4: Update watermark ────────────────────────
    update_wm = PythonOperator(
        task_id='update_watermark',
        python_callable=update_watermark,
        trigger_rule='none_failed_min_one_success',
    )

    # ── Dependencies ──────────────────────────────────────────
    wait_for_csv >> check_load
    check_load >> [prepare_init, prepare_incr] >> join_prepare
    join_prepare >> check_data
    check_data >> run_pipeline >> dbt_seed
    check_data >> skip_pipeline
    dbt_seed >> dbt_run_bronze >> dbt_run_silver >> dbt_run_gold >> dbt_run_marts >> dbt_test >> validate >> update_wm
