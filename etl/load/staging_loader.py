"""
StagingLoader — orchestrates loading all extracted results into staging.

Wraps BaseLoader with domain-level logging and error isolation:
if one table fails to load, others still proceed and the error is reported.
"""

import logging
from dataclasses import dataclass

import psycopg2.extensions

from etl.extract.base_extractor import ExtractResult
from etl.load.base_loader import BaseLoader

log = logging.getLogger(__name__)


@dataclass
class LoadSummary:
    table_name: str
    rows_loaded: int
    success: bool
    error: str | None = None


class StagingLoader:
    """Loads a list of ExtractResult objects into the hotelmind_staging schema."""

    def __init__(self, warehouse_conn: psycopg2.extensions.connection) -> None:
        self._loader = BaseLoader(warehouse_conn)

    def load_all(self, results: list[ExtractResult]) -> list[LoadSummary]:
        summaries: list[LoadSummary] = []

        for result in results:
            try:
                rows_loaded = self._loader.load(result)
                summaries.append(
                    LoadSummary(
                        table_name=result.table_name,
                        rows_loaded=rows_loaded,
                        success=True,
                    )
                )
            except Exception as exc:
                log.error(
                    "Failed to load '%s' into staging: %s",
                    result.table_name,
                    exc,
                    exc_info=True,
                )
                summaries.append(
                    LoadSummary(
                        table_name=result.table_name,
                        rows_loaded=0,
                        success=False,
                        error=str(exc),
                    )
                )

        total = sum(s.rows_loaded for s in summaries)
        failures = [s for s in summaries if not s.success]

        log.info("Staging load complete: %d total rows, %d tables failed", total, len(failures))

        if failures:
            log.warning("Failed tables: %s", [f.table_name for f in failures])

        return summaries
