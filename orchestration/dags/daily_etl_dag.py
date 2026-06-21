"""
HotelMind Daily ETL DAG

Schedule: 2:00 AM every day (after operational day ends)
Pipeline:
    extract_hotel      ─┐
    extract_booking    ─┤→ lake_load → staging_load → quality_gate → dbt_run → dbt_test
    extract_restaurant ─┤
    extract_staff      ─┘

Failure behaviour:
  - If any extract fails, its downstream lake/staging steps are skipped.
  - If quality_gate fails, dbt_run is skipped (warehouse stays at last good state).
  - dbt_test failure does NOT roll back — it raises an alert for human review.

XCom strategy: extract tasks write rows to temp JSON files on the Airflow worker's
local filesystem and pass only {table_name: {path, watermark}} dicts via XCom.
This avoids the ~48 KB Airflow metadata DB XCom limit for large datasets.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import psycopg2
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ── DAG defaults ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "hotelmind-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Connection helpers ────────────────────────────────────────────────────────

def _source_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ["SOURCE_DB_HOST"],
        port=int(os.environ.get("SOURCE_DB_PORT", "5432")),
        dbname=os.environ["SOURCE_DB_NAME"],
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
        connect_timeout=30,
    )


def _warehouse_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_DB_HOST"],
        port=int(os.environ.get("WAREHOUSE_DB_PORT", "5432")),
        dbname=os.environ["WAREHOUSE_DB_NAME"],
        user=os.environ["WAREHOUSE_DB_USER"],
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
        connect_timeout=30,
    )


def _json_serialize(obj: object) -> str:
    """JSON serializer for types not handled by default encoder."""
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, UUID):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _write_domain_to_disk(results: list) -> dict[str, dict]:
    """Write ExtractResult rows to temp JSON files; return {table: {path, watermark}}."""
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="hotelmind_etl_"))
    meta: dict[str, dict] = {}
    for r in results:
        p = tmp / f"{r.table_name}.json"
        p.write_text(json.dumps(r.rows, default=_json_serialize), encoding="utf-8")
        meta[r.table_name] = {
            "path": str(p),
            "watermark": r.watermark.isoformat(),
        }
    return meta


def _read_domain_from_disk(meta: dict[str, dict]) -> list:
    """Reconstruct ExtractResult list from file-path XCom metadata."""
    import sys; sys.path.insert(0, "/opt/airflow")
    from etl.extract.base_extractor import ExtractResult

    results = []
    for table_name, data in meta.items():
        rows = json.loads(pathlib.Path(data["path"]).read_text(encoding="utf-8"))
        results.append(ExtractResult(
            table_name=table_name,
            rows=rows,
            watermark=datetime.fromisoformat(data["watermark"]),
        ))
    return results


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="daily_hotelmind_etl",
    description="HotelMind incremental ETL: extract → lake → staging → quality → dbt",
    schedule="0 2 * * *",       # 2:00 AM daily
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["hotelmind", "etl", "daily"],
    doc_md=__doc__,
)
def daily_etl_dag():

    @task(task_id="extract_hotel")
    def extract_hotel(**context) -> dict:
        import sys; sys.path.insert(0, "/opt/airflow")
        from etl.extract.hotel_extractor import HotelExtractor
        from etl.watermark.watermark_store import WatermarkStore

        src = _source_conn()
        wh  = _warehouse_conn()
        wh.autocommit = False
        try:
            extractor = HotelExtractor(src, WatermarkStore(wh))
            results   = extractor.extract_all()
        finally:
            src.close()
            wh.close()

        return _write_domain_to_disk(results)

    @task(task_id="extract_booking")
    def extract_booking(**context) -> dict:
        import sys; sys.path.insert(0, "/opt/airflow")
        from etl.extract.booking_extractor import BookingExtractor
        from etl.watermark.watermark_store import WatermarkStore

        src = _source_conn()
        wh  = _warehouse_conn()
        wh.autocommit = False
        try:
            extractor = BookingExtractor(src, WatermarkStore(wh))
            results   = extractor.extract_all()
        finally:
            src.close()
            wh.close()

        return _write_domain_to_disk(results)

    @task(task_id="extract_restaurant")
    def extract_restaurant(**context) -> dict:
        import sys; sys.path.insert(0, "/opt/airflow")
        from etl.extract.restaurant_extractor import RestaurantExtractor
        from etl.watermark.watermark_store import WatermarkStore

        src = _source_conn()
        wh  = _warehouse_conn()
        wh.autocommit = False
        try:
            extractor = RestaurantExtractor(src, WatermarkStore(wh))
            results   = extractor.extract_all()
        finally:
            src.close()
            wh.close()

        return _write_domain_to_disk(results)

    @task(task_id="extract_staff")
    def extract_staff(**context) -> dict:
        import sys; sys.path.insert(0, "/opt/airflow")
        from etl.extract.staff_extractor import StaffExtractor
        from etl.watermark.watermark_store import WatermarkStore

        src = _source_conn()
        wh  = _warehouse_conn()
        wh.autocommit = False
        try:
            extractor = StaffExtractor(src, WatermarkStore(wh))
            results   = extractor.extract_all()
        finally:
            src.close()
            wh.close()

        return _write_domain_to_disk(results)

    @task(task_id="lake_load")
    def lake_load(hotel: dict, booking: dict, restaurant: dict, staff: dict, **context) -> int:
        import sys; sys.path.insert(0, "/opt/airflow")
        from etl.load.lake_loader import LakeLoader

        run_id     = context["run_id"]
        lake       = LakeLoader(run_id=run_id)
        all_results = []
        for domain_meta in [hotel, booking, restaurant, staff]:
            all_results.extend(_read_domain_from_disk(domain_meta))

        keys = lake.write_all(all_results)
        return len([k for k in keys if k])

    @task(task_id="staging_load")
    def staging_load(hotel: dict, booking: dict, restaurant: dict, staff: dict, **context) -> dict:
        import sys; sys.path.insert(0, "/opt/airflow")
        from etl.load.staging_loader import StagingLoader

        all_results = []
        for domain_meta in [hotel, booking, restaurant, staff]:
            all_results.extend(_read_domain_from_disk(domain_meta))

        wh = _warehouse_conn()
        wh.autocommit = False
        try:
            loader    = StagingLoader(wh)
            summaries = loader.load_all(all_results)
        finally:
            wh.close()

        return {
            "total_rows": sum(s.rows_loaded for s in summaries),
            "failures": [s.table_name for s in summaries if not s.success],
        }

    @task(task_id="quality_gate")
    def quality_gate(staging_result: dict, **context) -> bool:
        import sys; sys.path.insert(0, "/opt/airflow")
        from quality.validator import run_staging_validation

        if staging_result["failures"]:
            log.warning("Staging had failures: %s — skipping quality gate", staging_result["failures"])
            return False

        wh = _warehouse_conn()
        try:
            summary = run_staging_validation(wh)
        finally:
            wh.close()

        if not summary.all_passed:
            raise ValueError(f"Data quality gates failed: {summary.failed_suites}")
        return True

    @task.bash(task_id="dbt_run")
    def dbt_run(**context) -> str:
        project_dir  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
        profiles_dir = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
        return f"dbt run --project-dir {project_dir} --profiles-dir {profiles_dir} --target dev"

    @task.bash(task_id="dbt_test")
    def dbt_test(**context) -> str:
        project_dir  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
        profiles_dir = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
        return f"dbt test --project-dir {project_dir} --profiles-dir {profiles_dir} --target dev"

    # ── Wire the DAG ──────────────────────────────────────────────────────────
    hotel_data      = extract_hotel()
    booking_data    = extract_booking()
    restaurant_data = extract_restaurant()
    staff_data      = extract_staff()

    # lake_load and staging_load both read from temp files written by extract tasks.
    # lake_load must complete before staging_load (lake is the durable raw copy).
    lake_task    = lake_load(hotel_data, booking_data, restaurant_data, staff_data)
    staging_task = staging_load(hotel_data, booking_data, restaurant_data, staff_data)

    quality_ok    = quality_gate(staging_task)
    dbt_run_task  = dbt_run()
    dbt_test_task = dbt_test()

    lake_task >> staging_task
    staging_task >> quality_ok
    quality_ok   >> dbt_run_task
    dbt_run_task >> dbt_test_task


daily_etl_dag()
