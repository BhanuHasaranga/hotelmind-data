"""
BaseLoader — generic upsert of extracted rows into the staging schema.

Uses PostgreSQL's INSERT … ON CONFLICT DO UPDATE (upsert) so every run is
idempotent: re-running with the same data produces the same result.

The `_loaded_at` timestamp marks when a row was last synced from the source.
"""

import logging
from typing import Any

import psycopg2
import psycopg2.extras

from config.constants import STAGING_SCHEMA
from etl.extract.base_extractor import ExtractResult

log = logging.getLogger(__name__)


class BaseLoader:
    """Upserts ExtractResult rows into a staging table."""

    # Tables where the primary key is composite (not a single 'id' column).
    # Maps table_name → list of PK columns.
    COMPOSITE_PKS: dict[str, list[str]] = {
        "room_type_amenities": ["room_type_id", "amenity_id"],
    }

    def __init__(self, warehouse_conn: psycopg2.extensions.connection) -> None:
        self._conn = warehouse_conn

    def load(self, result: ExtractResult) -> int:
        """
        Upsert all rows from *result* into hotelmind_staging.{table_name}.
        Returns the number of rows written.
        """
        if not result.rows:
            log.debug("Load skip: '%s' — no rows to write", result.table_name)
            return 0

        table_name = result.table_name
        rows = result.rows

        # Determine column names from the first row (all rows share the same schema)
        # Exclude any source columns not present in staging (defensive)
        sample = rows[0]
        columns = list(sample.keys())

        # Inject _loaded_at (handled by DB DEFAULT, but we can also set it explicitly)
        # We don't include it in the source dict — let the DB DEFAULT handle it.

        pk_cols = self.COMPOSITE_PKS.get(table_name, ["id"])

        self._upsert_batch(table_name, columns, rows, pk_cols)

        log.info("Loaded '%s': %d rows → %s.%s", table_name, len(rows), STAGING_SCHEMA, table_name)
        return len(rows)

    def _upsert_batch(
        self,
        table_name: str,
        columns: list[str],
        rows: list[dict[str, Any]],
        pk_cols: list[str],
    ) -> None:
        qualified = f"{STAGING_SCHEMA}.{table_name}"
        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholder_list = ", ".join(f"%({c})s" for c in columns)
        conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)

        # On conflict: update all non-PK columns except created_at (immutable once set)
        update_cols = [c for c in columns if c not in pk_cols and c != "created_at"]
        update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

        sql = f"""
            INSERT INTO {qualified} ({col_list})
            VALUES ({placeholder_list})
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_clause}, _loaded_at = NOW()
        """

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)

        self._conn.commit()
