"""
Airflow DAG — Plant Monitoring ETL
────────────────────────────────────
Schedule  : Every 30 minutes (change `schedule_interval` as needed)
Strategy  : Incremental — reads the last watermark from SQL Server,
            fetches only newer rows from Google Sheets, and upserts them.

Task graph
──────────
start → check_connection → get_watermark → extract → transform → load → update_watermark → end
"""

import sys
import os

# Make ETL modules importable inside Airflow's container
sys.path.insert(0, "/opt/airflow/etl")

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ──────────────────────────────────────────────
# Default args
# ──────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner"           : "etl_team",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
}

# ──────────────────────────────────────────────
# Task functions
# ──────────────────────────────────────────────

def task_check_connection(**context):
    """Verify the SQL Server connection before doing any work."""
    from load.sqlserver_loader import SQLServerLoader
    loader = SQLServerLoader()
    if not loader.test_connection():
        raise ConnectionError("Cannot reach SQL Server. Check your .env / Docker network.")
    print("✅ SQL Server connection OK.")


def task_get_watermark(**context):
    """Read the last processed timestamp and push to XCom."""
    from load.sqlserver_loader import SQLServerLoader
    loader    = SQLServerLoader()
    watermark = loader.get_watermark()
    context["ti"].xcom_push(key="watermark", value=watermark)
    print(f"Watermark pulled: {watermark}")


def task_extract(**context):
    """Extract new rows from Google Sheets since the watermark."""
    import json
    from extract.google_sheets_extractor import GoogleSheetsExtractor

    watermark = context["ti"].xcom_pull(key="watermark", task_ids="get_watermark")
    extractor = GoogleSheetsExtractor()
    df        = extractor.extract(last_watermark=watermark)

    # Serialise DataFrame as JSON for XCom (keep it small; use temp files for large data)
    context["ti"].xcom_push(key="raw_rows",   value=df.to_json(orient="records", date_format="iso"))
    context["ti"].xcom_push(key="row_count",  value=len(df))
    print(f"Extracted {len(df)} new rows.")


def task_transform(**context):
    """Clean and validate the raw data."""
    import pandas as pd
    from transform.plant_transformer import PlantTransformer

    raw_json = context["ti"].xcom_pull(key="raw_rows", task_ids="extract")
    df       = pd.read_json(raw_json, orient="records")

    transformer = PlantTransformer()
    clean_df    = transformer.transform(df)

    context["ti"].xcom_push(key="clean_rows",  value=clean_df.to_json(orient="records", date_format="iso"))
    context["ti"].xcom_push(key="clean_count", value=len(clean_df))
    print(f"Transform complete. Clean rows: {len(clean_df)}")


def task_load(**context):
    """Upsert clean data into SQL Server."""
    import pandas as pd
    from load.sqlserver_loader import SQLServerLoader

    clean_json = context["ti"].xcom_pull(key="clean_rows", task_ids="transform")
    df         = pd.read_json(clean_json, orient="records")

    loader      = SQLServerLoader()
    rows_loaded = loader.load(df)
    context["ti"].xcom_push(key="rows_loaded", value=rows_loaded)
    print(f"Loaded {rows_loaded} rows into SQL Server.")


def task_update_watermark(**context):
    """Advance the watermark to the latest timestamp in this batch."""
    import pandas as pd
    from load.sqlserver_loader import SQLServerLoader

    clean_json = context["ti"].xcom_pull(key="clean_rows", task_ids="transform")
    df         = pd.read_json(clean_json, orient="records")

    if df.empty:
        print("No new rows — watermark unchanged.")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    new_watermark   = df["timestamp"].max().isoformat()

    loader = SQLServerLoader()
    loader.update_watermark(new_watermark)
    print(f"Watermark updated to: {new_watermark}")


# ──────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────
with DAG(
    dag_id           = "plant_monitoring_etl",
    description      = "Incremental ETL: Google Sheets → SQL Server (Plant Data)",
    default_args     = DEFAULT_ARGS,
    start_date       = datetime(2025, 1, 1),
    schedule_interval= "*/30 * * * *",   # Every 30 minutes — adjust as needed
    catchup          = False,
    max_active_runs  = 1,
    tags             = ["plant", "etl", "google-sheets", "sql-server"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    check_connection = PythonOperator(
        task_id        = "check_connection",
        python_callable= task_check_connection,
    )
    get_watermark = PythonOperator(
        task_id        = "get_watermark",
        python_callable= task_get_watermark,
    )
    extract = PythonOperator(
        task_id        = "extract",
        python_callable= task_extract,
    )
    transform = PythonOperator(
        task_id        = "transform",
        python_callable= task_transform,
    )
    load = PythonOperator(
        task_id        = "load",
        python_callable= task_load,
    )
    update_watermark = PythonOperator(
        task_id        = "update_watermark",
        python_callable= task_update_watermark,
    )

    # Task graph
    (
        start
        >> check_connection
        >> get_watermark
        >> extract
        >> transform
        >> load
        >> update_watermark
        >> end
    )
