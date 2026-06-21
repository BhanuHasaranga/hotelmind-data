"""
Backfill script — resets watermarks and re-extracts a date range.

Usage:
    python scripts/backfill.py --from 2025-01-01 --to 2025-06-30
    python scripts/backfill.py --full-reset         # resets ALL watermarks to epoch

WARNING: --full-reset will re-extract ALL historical data from the source DB.
This is appropriate when the staging schema changes or you want a clean rebuild.
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HotelMind ETL backfill utility")
    p.add_argument("--from",       dest="from_date", type=str, help="Start date YYYY-MM-DD")
    p.add_argument("--to",         dest="to_date",   type=str, help="End date YYYY-MM-DD")
    p.add_argument("--full-reset", action="store_true", help="Reset all watermarks to epoch (full re-extract)")
    p.add_argument("--tables",     nargs="+", help="Specific tables to reset (default: all)")
    return p.parse_args()


def reset_watermarks(conn: psycopg2.extensions.connection, tables: list[str] | None, to: datetime) -> None:
    with conn.cursor() as cur:
        if tables:
            for t in tables:
                cur.execute(
                    "UPDATE hotelmind_warehouse.etl_watermarks SET last_extracted = %s WHERE table_name = %s",
                    (to, t),
                )
                log.info("Reset watermark for '%s' → %s", t, to.isoformat())
        else:
            cur.execute(
                "UPDATE hotelmind_warehouse.etl_watermarks SET last_extracted = %s",
                (to,),
            )
            log.info("Reset ALL watermarks → %s", to.isoformat())
    conn.commit()


def main() -> None:
    args = parse_args()

    if not args.full_reset and not args.from_date:
        log.error("Provide --from <date> or --full-reset")
        sys.exit(1)

    wh_conn = psycopg2.connect(settings.WAREHOUSE_DB_URL)
    wh_conn.autocommit = False

    if args.full_reset:
        log.warning("Full reset — all watermarks will be set to epoch (1970-01-01)")
        reset_watermarks(wh_conn, args.tables, EPOCH)
    else:
        from_dt = datetime.fromisoformat(args.from_date).replace(tzinfo=timezone.utc)
        log.info("Setting watermarks to %s for tables: %s", from_dt.isoformat(), args.tables or "ALL")
        reset_watermarks(wh_conn, args.tables, from_dt)

    wh_conn.close()
    log.info("Watermarks updated. Run 'python scripts/run_etl.py' to re-extract.")


if __name__ == "__main__":
    main()
