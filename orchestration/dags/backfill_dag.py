"""
Backfill DAG — resets watermarks and re-runs ETL for a specific date range.

Trigger: manual only
Params:
  - from_date: YYYY-MM-DD (start of backfill window)
  - to_date:   YYYY-MM-DD (end of backfill window, defaults to today)
  - tables:    comma-separated list of tables to reset (defaults to all)

Usage:
    In Airflow UI → Trigger DAG w/ Config:
    {
        "from_date": "2025-01-01",
        "to_date":   "2025-12-31",
        "tables":    "reservations,restaurant_orders"
    }
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import psycopg2
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "hotelmind-data",
    "retries": 0,
}


def _warehouse_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_DB_HOST"],
        port=int(os.environ.get("WAREHOUSE_DB_PORT", "5432")),
        dbname=os.environ["WAREHOUSE_DB_NAME"],
        user=os.environ["WAREHOUSE_DB_USER"],
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
    )


@dag(
    dag_id="hotelmind_backfill",
    description="Reset watermarks and re-extract data for a date range",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "from_date": Param("2025-01-01", type="string", description="Start date YYYY-MM-DD"),
        "to_date":   Param("", type="string", description="End date YYYY-MM-DD (empty = today)"),
        "tables":    Param("", type="string", description="Comma-separated tables (empty = all)"),
    },
    tags=["hotelmind", "backfill", "manual"],
)
def backfill_dag():

    @task(task_id="reset_watermarks")
    def reset_watermarks(**context) -> dict:
        import sys; sys.path.insert(0, "/opt/airflow")

        params    = context["params"]
        from_date = params["from_date"]
        tables    = [t.strip() for t in params["tables"].split(",") if t.strip()] or None

        from_dt = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)

        wh = _warehouse_conn()
        wh.autocommit = False

        with wh.cursor() as cur:
            if tables:
                for t in tables:
                    cur.execute(
                        "UPDATE hotelmind_warehouse.etl_watermarks SET last_extracted = %s WHERE table_name = %s",
                        (from_dt, t),
                    )
            else:
                cur.execute(
                    "UPDATE hotelmind_warehouse.etl_watermarks SET last_extracted = %s",
                    (from_dt,),
                )
        wh.commit()
        wh.close()

        return {"from_date": from_date, "tables_reset": tables or "ALL"}

    @task.bash(task_id="run_etl")
    def run_etl(**context) -> str:
        return "python /opt/airflow/scripts/run_etl.py --skip-lake"

    @task.bash(task_id="dbt_run")
    def dbt_run() -> str:
        project_dir  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
        profiles_dir = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
        return f"dbt run --project-dir {project_dir} --profiles-dir {profiles_dir} --full-refresh"

    reset = reset_watermarks()
    etl   = run_etl()
    dbt   = dbt_run()

    reset >> etl >> dbt


backfill_dag()
