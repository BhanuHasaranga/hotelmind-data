"""
LakeLoader — writes raw extraction results to the MinIO (S3) data lake.

Raw zone path pattern:
    s3://{bucket}/raw/{table_name}/year={YYYY}/month={MM}/day={DD}/{run_id}.json

This gives:
  - Time-partitioned layout for efficient historical replay
  - One file per table per ETL run (idempotent: same run_id overwrites)
  - JSON format preserves original types (including UUIDs, dates, decimals)

The raw zone is never modified by transforms — it is the source of truth
for replay. If the warehouse schema changes, we re-process from raw without
re-extracting from the operational DB.
"""

import json
import logging
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import boto3
from botocore.config import Config

from config.settings import settings
from etl.extract.base_extractor import ExtractResult

log = logging.getLogger(__name__)


def _serialise(obj: Any) -> Any:
    """JSON-serialise types not handled by the default encoder."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Cannot serialise {type(obj)}: {obj!r}")


class LakeLoader:
    """Writes ExtractResult objects to the MinIO raw zone."""

    def __init__(self, run_id: str | None = None) -> None:
        self._run_id = run_id or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.S3_ENDPOINT_URL,
            aws_access_key_id=settings.S3_ACCESS_KEY,
            aws_secret_access_key=settings.S3_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )
        self._bucket = settings.S3_BUCKET_RAW

    def write(self, result: ExtractResult) -> str:
        """
        Write one ExtractResult to S3/MinIO.
        Returns the S3 key of the written object.
        """
        if not result.rows:
            log.info("Lake skip: '%s' — 0 rows extracted, nothing to write", result.table_name)
            return ""

        today = date.today()
        key = (
            f"raw/{result.table_name}/"
            f"year={today.year:04d}/month={today.month:02d}/day={today.day:02d}/"
            f"{self._run_id}.json"
        )

        payload = json.dumps(
            {
                "table": result.table_name,
                "run_id": self._run_id,
                "extracted_at": datetime.utcnow().isoformat(),
                "watermark": result.watermark.isoformat(),
                "row_count": result.row_count,
                "rows": result.rows,
            },
            default=_serialise,
            ensure_ascii=False,
        ).encode("utf-8")

        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
        )

        log.info(
            "Lake write: s3://%s/%s  (%d rows, %d bytes)",
            self._bucket,
            key,
            result.row_count,
            len(payload),
        )
        return key

    def write_all(self, results: list[ExtractResult]) -> list[str]:
        """Write a list of ExtractResults. Returns list of S3 keys."""
        return [self.write(r) for r in results]
