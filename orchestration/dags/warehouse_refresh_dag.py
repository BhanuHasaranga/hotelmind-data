"""
Warehouse Refresh DAG — dbt only, no extract.

Use this when:
  - You change a dbt model and want to refresh without waiting for the daily ETL
  - You need to rebuild the warehouse after a schema change
  - You want to run dbt tests independently

Trigger: manual only (no schedule)
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "hotelmind-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="warehouse_refresh",
    description="dbt-only warehouse refresh — no data extraction",
    schedule=None,              # manual trigger only
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["hotelmind", "dbt", "manual"],
)
def warehouse_refresh_dag():

    @task.bash(task_id="dbt_deps")
    def dbt_deps() -> str:
        project_dir  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
        profiles_dir = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
        return f"dbt deps --project-dir {project_dir} --profiles-dir {profiles_dir}"

    @task.bash(task_id="dbt_run")
    def dbt_run() -> str:
        project_dir  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
        profiles_dir = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
        return f"dbt run --project-dir {project_dir} --profiles-dir {profiles_dir} --target dev"

    @task.bash(task_id="dbt_test")
    def dbt_test() -> str:
        project_dir  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
        profiles_dir = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
        return f"dbt test --project-dir {project_dir} --profiles-dir {profiles_dir} --target dev"

    @task.bash(task_id="dbt_docs_generate")
    def dbt_docs_generate() -> str:
        project_dir  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
        profiles_dir = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
        return f"dbt docs generate --project-dir {project_dir} --profiles-dir {profiles_dir}"

    deps = dbt_deps()
    run  = dbt_run()
    test = dbt_test()
    docs = dbt_docs_generate()

    deps >> run >> test >> docs


warehouse_refresh_dag()
