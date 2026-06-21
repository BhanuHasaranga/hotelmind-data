# ADR-003: MinIO for Data Lake

**Status:** Accepted
**Date:** 2026-06-19

## Decision

Use MinIO (S3-compatible object storage) as the raw data lake, running locally in Docker.

## Context

A data lake provides: durable raw data storage, replay capability (re-process raw data if transforms break), audit trail of what was extracted and when, and future support for Parquet/Delta Lake formats.

## Consequences

**Positive:**
- 100% S3-compatible API — boto3 code is identical whether targeting MinIO or real S3
- Phase 8 migration = change `S3_ENDPOINT_URL` env var from `http://minio:9000` to `""` and add AWS credentials
- MinIO Console UI at port 9001 provides visual browsing of raw files
- Time-partitioned paths (`year=YYYY/month=MM/day=DD/`) enable efficient historical queries
- Raw JSON provides replay: if warehouse schema changes, re-derive from raw without re-extracting from source

**Negative:**
- JSON is not columnar — reading large raw files is slower than Parquet
- Local MinIO has no durability guarantees (data lost if docker volume deleted)

## Phase 8 Migration Path

```bash
# In .env for production:
S3_ENDPOINT_URL=""          # empty = use AWS S3
AWS_DEFAULT_REGION=us-east-1
# AWS credentials via IAM role (no keys in env)
```

All boto3 code in `etl/load/lake_loader.py` works unchanged.
