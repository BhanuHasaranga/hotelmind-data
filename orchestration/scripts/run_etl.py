"""
Run the full HotelMind ETL pipeline for the current date.

Steps:
  1. Extract: all 4 domain extractors pull incremental data from the source DB
  2. Lake:    write raw JSON snapshots to MinIO
  3. Stage:   upsert extracted rows into hotelmind_staging
  4. dbt:     run dbt transformations (staging → warehouse → marts)
  5. Test:    run dbt tests

Usage:
    python scripts/run_etl.py
    python scripts/run_etl.py --skip-dbt      # ETL only, no dbt
    python scripts/run_etl.py --skip-lake     # skip MinIO write
    python scripts/run_etl.py --dry-run       # extract only, no writes
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from etl.extract.booking_extractor import BookingExtractor
from etl.extract.hotel_extractor import HotelExtractor
from etl.extract.restaurant_extractor import RestaurantExtractor
from etl.extract.staff_extractor import StaffExtractor
from etl.load.lake_loader import LakeLoader
from etl.load.staging_loader import StagingLoader
from etl.watermark.watermark_store import WatermarkStore

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("hotelmind.etl")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HotelMind incremental ETL pipeline")
    p.add_argument("--skip-dbt",  action="store_true", help="Skip dbt run + test")
    p.add_argument("--skip-lake", action="store_true", help="Skip MinIO lake write")
    p.add_argument("--dry-run",   action="store_true", help="Extract only, no writes")
    return p.parse_args()


def run_dbt(project_dir: str, profiles_dir: str, target: str) -> bool:
    """Run `dbt run` then `dbt test`. Returns True if both succeed."""
    for cmd in [
        ["dbt", "run",  "--project-dir", project_dir, "--profiles-dir", profiles_dir, "--target", target],
        ["dbt", "test", "--project-dir", project_dir, "--profiles-dir", profiles_dir, "--target", target],
    ]:
        log.info("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=False)
        if result.returncode != 0:
            log.error("dbt command failed: %s", " ".join(cmd))
            return False
    return True


def main() -> None:
    args = parse_args()
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log.info("═══ HotelMind ETL run_id=%s ════════════════════════════════", run_id)

    # ── Connections ────────────────────────────────────────────────────────────
    log.info("Connecting to source DB …")
    source_conn = psycopg2.connect(settings.SOURCE_DB_URL)
    source_conn.autocommit = True

    log.info("Connecting to warehouse DB …")
    wh_conn = psycopg2.connect(settings.WAREHOUSE_DB_URL)
    wh_conn.autocommit = False

    wm_store = WatermarkStore(wh_conn)

    # ── Extract ────────────────────────────────────────────────────────────────
    log.info("─── Phase 1: Extract ─────────────────────────────────────────")
    all_results = []
    for Extractor in [HotelExtractor, BookingExtractor, RestaurantExtractor, StaffExtractor]:
        extractor = Extractor(source_conn, wm_store)
        results = extractor.extract_all()
        all_results.extend(results)
        log.info(
            "%s: %d tables, %d total rows",
            Extractor.__name__,
            len(results),
            sum(r.row_count for r in results),
        )

    total_rows = sum(r.row_count for r in all_results)
    log.info("Total extracted: %d rows across %d tables", total_rows, len(all_results))

    if args.dry_run:
        log.info("Dry-run mode — stopping after extract")
        source_conn.close()
        wh_conn.close()
        return

    # ── Lake write ─────────────────────────────────────────────────────────────
    if not args.skip_lake:
        log.info("─── Phase 2: Lake write (MinIO) ──────────────────────────")
        try:
            lake = LakeLoader(run_id=run_id)
            keys = lake.write_all(all_results)
            log.info("Lake: wrote %d objects", len([k for k in keys if k]))
        except Exception as exc:
            log.warning("Lake write failed (non-fatal): %s", exc)

    # ── Staging load ───────────────────────────────────────────────────────────
    log.info("─── Phase 3: Staging load ────────────────────────────────────")
    staging_loader = StagingLoader(wh_conn)
    summaries = staging_loader.load_all(all_results)
    failed = [s for s in summaries if not s.success]
    if failed:
        log.error("Staging load had %d failures. Check logs above.", len(failed))

    # ── dbt ───────────────────────────────────────────────────────────────────
    if not args.skip_dbt:
        log.info("─── Phase 4: dbt run + test ──────────────────────────────")
        dbt_ok = run_dbt(
            project_dir=settings.DBT_PROJECT_DIR,
            profiles_dir=settings.DBT_PROFILES_DIR,
            target=settings.DBT_TARGET,
        )
        if not dbt_ok:
            log.error("dbt step failed — warehouse may be stale")
            sys.exit(1)

    source_conn.close()
    wh_conn.close()
    log.info("═══ ETL complete ════════════════════════════════════════════")


if __name__ == "__main__":
    main()
