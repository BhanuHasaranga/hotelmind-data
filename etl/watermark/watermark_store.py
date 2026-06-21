"""
Watermark store — persists per-table ETL high-water marks.

A watermark is the maximum `updated_at` value seen in the last successful
extraction for a given source table. The next run queries:

    WHERE updated_at > watermark

This makes the pipeline incremental: only changed/new rows are fetched.
Watermarks are stored in `hotelmind_warehouse.etl_watermarks` so they
survive process restarts and are inspectable via SQL.
"""

import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extensions

log = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

UPSERT_SQL = """
    INSERT INTO hotelmind_warehouse.etl_watermarks
        (table_name, last_extracted, last_run_at, rows_extracted)
    VALUES
        (%(table_name)s, %(last_extracted)s, NOW(), %(rows_extracted)s)
    ON CONFLICT (table_name)
    DO UPDATE SET
        last_extracted = EXCLUDED.last_extracted,
        last_run_at    = NOW(),
        rows_extracted = EXCLUDED.rows_extracted
"""

SELECT_SQL = """
    SELECT last_extracted
    FROM hotelmind_warehouse.etl_watermarks
    WHERE table_name = %(table_name)s
"""


class WatermarkStore:
    """Reads and writes ETL watermarks in the warehouse database."""

    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self._conn = conn

    def get(self, table_name: str) -> datetime:
        """
        Return the last successfully extracted timestamp for *table_name*.
        Returns the Unix epoch (1970-01-01) if no watermark exists yet
        — which causes the first run to extract all historical rows.
        """
        with self._conn.cursor() as cur:
            cur.execute(SELECT_SQL, {"table_name": table_name})
            row = cur.fetchone()
            if row is None:
                log.debug("No watermark for '%s', using epoch", table_name)
                return _EPOCH
            wm = row[0]
            if wm.tzinfo is None:
                wm = wm.replace(tzinfo=timezone.utc)
            log.debug("Watermark for '%s': %s", table_name, wm.isoformat())
            return wm

    def set(self, table_name: str, watermark: datetime, rows_extracted: int) -> None:
        """Persist the new watermark after a successful extraction."""
        if watermark.tzinfo is None:
            watermark = watermark.replace(tzinfo=timezone.utc)
        with self._conn.cursor() as cur:
            cur.execute(
                UPSERT_SQL,
                {
                    "table_name": table_name,
                    "last_extracted": watermark,
                    "rows_extracted": rows_extracted,
                },
            )
        self._conn.commit()
        log.info(
            "Watermark updated: table='%s'  new_mark=%s  rows=%d",
            table_name,
            watermark.isoformat(),
            rows_extracted,
        )
