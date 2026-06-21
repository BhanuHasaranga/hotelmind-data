"""
BaseExtractor — generic incremental extraction from the operational PostgreSQL DB.

Extraction strategy:
  - Tables with updated_at:  WHERE updated_at > last_watermark  (incremental)
  - Junction tables (no updated_at):  full extract each run (small, rarely changes)

After a successful extract the WatermarkStore is updated with:
    max(updated_at) of the extracted rows (or NOW() for junction tables).

Usage pattern:
    extractor = HotelExtractor(source_conn, watermark_store)
    result = extractor.extract_all()
    # result.data is list[dict], result.table_name, result.watermark
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras

from etl.watermark.watermark_store import WatermarkStore

log = logging.getLogger(__name__)


@dataclass
class ExtractResult:
    table_name: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    watermark: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def row_count(self) -> int:
        return len(self.rows)


class BaseExtractor:
    """
    Extracts one or more source tables incrementally.

    Subclasses declare which tables to extract by overriding `_tables()`.
    Each entry is (source_table, has_updated_at).
    """

    def __init__(
        self,
        source_conn: psycopg2.extensions.connection,
        watermark_store: WatermarkStore,
    ) -> None:
        self._conn = source_conn
        self._wm = watermark_store

    def _tables(self) -> list[tuple[str, bool]]:
        """Return list of (table_name, has_updated_at) to extract."""
        raise NotImplementedError

    def extract_all(self) -> list[ExtractResult]:
        """Extract all tables declared by this extractor. Returns list of ExtractResult."""
        results = []
        for table_name, has_updated_at in self._tables():
            result = self._extract_table(table_name, has_updated_at)
            results.append(result)
        return results

    def _extract_table(self, table_name: str, has_updated_at: bool) -> ExtractResult:
        now = datetime.now(timezone.utc)

        if has_updated_at:
            watermark = self._wm.get(table_name)
            rows = self._query_incremental(table_name, watermark)
            # Use max(updated_at) from extracted rows as the new watermark.
            # If no rows were extracted the watermark stays the same — do not regress it.
            new_wm = max(
                (r["updated_at"] for r in rows if r.get("updated_at")),
                default=watermark,
            )
            if new_wm.tzinfo is None:
                new_wm = new_wm.replace(tzinfo=timezone.utc)
        else:
            # Full extract for junction/static tables
            rows = self._query_full(table_name)
            new_wm = now

        result = ExtractResult(table_name=table_name, rows=rows, watermark=new_wm)

        log.info(
            "Extracted '%s': %d rows  new_watermark=%s",
            table_name,
            result.row_count,
            new_wm.isoformat(),
        )

        if result.row_count > 0 or not has_updated_at:
            self._wm.set(table_name, new_wm, result.row_count)

        return result

    def _query_incremental(self, table_name: str, since: datetime) -> list[dict[str, Any]]:
        """Extract rows WHERE updated_at > since, ordered by updated_at ASC."""
        sql = f"""
            SELECT *
            FROM public.{table_name}
            WHERE updated_at > %(since)s
            ORDER BY updated_at ASC
        """
        return self._execute(sql, {"since": since})

    def _query_full(self, table_name: str) -> list[dict[str, Any]]:
        """Full extract — used for tables without updated_at (junction tables)."""
        sql = f"SELECT * FROM public.{table_name}"
        return self._execute(sql, {})

    def _execute(self, sql: str, params: dict) -> list[dict[str, Any]]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            # Convert RealDictRow → plain dict so downstream code is free of psycopg2 types
            return [dict(r) for r in rows]
