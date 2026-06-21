"""
One-time warehouse initialisation script.

Runs:
  1. Creates hotelmind_staging + hotelmind_warehouse schemas
  2. Creates all staging tables and the etl_watermarks table
  3. Verifies connectivity to both source and warehouse databases

Usage:
    python scripts/init_warehouse.py
"""

import logging
import sys
from pathlib import Path

import psycopg2

# Make project root importable when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings

logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL), format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

MIGRATION_FILE = Path(__file__).parent.parent / "warehouse" / "migrations" / "001_create_schemas.sql"


def _connect(url: str, label: str) -> psycopg2.extensions.connection:
    log.info("Connecting to %s …", label)
    conn = psycopg2.connect(url)
    conn.autocommit = True
    log.info("Connected to %s", label)
    return conn


def verify_source_db() -> None:
    conn = _connect(settings.SOURCE_DB_URL, "source DB (operational)")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
        count = cur.fetchone()[0]
        log.info("Source DB: %d tables in public schema", count)
    conn.close()


def run_migration(conn: psycopg2.extensions.connection) -> None:
    sql = MIGRATION_FILE.read_text(encoding="utf-8")
    log.info("Running migration: %s", MIGRATION_FILE.name)
    with conn.cursor() as cur:
        cur.execute(sql)
    log.info("Migration completed successfully")


def verify_warehouse(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'hotelmind_staging' ORDER BY 1"
        )
        staging_tables = [r[0] for r in cur.fetchall()]
        log.info("Staging tables created (%d): %s", len(staging_tables), staging_tables)

        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'hotelmind_warehouse' ORDER BY 1"
        )
        wh_tables = [r[0] for r in cur.fetchall()]
        log.info("Warehouse tables created (%d): %s", len(wh_tables), wh_tables)


def main() -> None:
    log.info("─── HotelMind Warehouse Initialisation ───────────────────────")

    # Verify source DB is reachable
    try:
        verify_source_db()
    except Exception as e:
        log.warning("Could not connect to source DB (operational). Is docker-compose up? Error: %s", e)

    # Run warehouse migration
    try:
        wh_conn = _connect(settings.WAREHOUSE_DB_URL, "warehouse DB")
        run_migration(wh_conn)
        verify_warehouse(wh_conn)
        wh_conn.close()
    except Exception as e:
        log.error("Warehouse initialisation failed: %s", e)
        sys.exit(1)

    log.info("─── Initialisation complete ──────────────────────────────────")
    log.info("Next steps:")
    log.info("  python scripts/run_etl.py           # run the full ETL pipeline")
    log.info("  cd dbt && dbt run                   # run dbt transformations")


if __name__ == "__main__":
    main()
