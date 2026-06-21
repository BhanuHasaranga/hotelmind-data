# ADR-004: Incremental Watermark Pattern

**Status:** Accepted
**Date:** 2026-06-19

## Decision

Use a per-table `updated_at` watermark stored in `hotelmind_warehouse.etl_watermarks` for incremental extraction from the operational database, rather than full table scans or CDC (Change Data Capture).

## Context

Full table scans on every ETL run work for small datasets but become expensive as data grows. The operational database (Phase 2) already has `updated_at` on every table (from the `Base` model in `db/base.py`). This makes a simple watermark pattern possible without any schema changes to the source.

## Implementation

```sql
-- etl_watermarks table
table_name  | last_extracted         | last_run_at         | rows_extracted
hotels      | 2026-06-18 02:00:15+00 | 2026-06-19 02:00:12 | 3
reservations| 2026-06-18 22:47:33+00 | 2026-06-19 02:00:14 | 47
```

```python
# Extraction query
SELECT * FROM public.hotels WHERE updated_at > %(since)s ORDER BY updated_at ASC
```

After extract: `last_extracted = max(updated_at)` from the extracted rows.

## Consequences

**Positive:**
- Zero additional infrastructure — no Kafka, no Debezium, no replication slots
- Fully inspectable: `SELECT * FROM etl_watermarks` shows exact ETL state
- Idempotent: re-running with the same watermark produces the same result
- Backfill is trivial: reset watermark to a past date and re-run

**Negative:**
- If a row is updated but `updated_at` is not set (bug or direct DB write), the row is missed
- Latency: minimum 1 full day between source change and warehouse availability
- Deletes in source are invisible (soft-deletes via `is_active` flag are captured)

## Mitigation

Phase 7 (Real-Time) adds Kafka streaming for near-real-time updates. The batch ETL remains as a daily reconciliation layer to catch anything the stream missed.

## Why Not CDC?

CDC (Debezium + Kafka) would give sub-second latency but requires:
- PostgreSQL logical replication enabled on the source DB
- Debezium connector running
- Kafka cluster
- Consumer process that handles offset management

This is 5× the operational complexity for a use case that doesn't need sub-second latency in Phase 3. Phase 7 adds Kafka anyway — CDC can be revisited then.
