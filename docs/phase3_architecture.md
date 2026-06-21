# HotelMind AI — Phase 3: Data Engineering Architecture

> **Author:** Bhanu Hasaranga
> **Stack:** Python 3.12 · dbt 1.8 · Apache Airflow 2.9 · PostgreSQL 16 · MinIO
> **Phase:** 3 — Data Engineering Layer

---

## 1. Overview

Phase 3 transforms HotelMind from a transactional operational system into an **analytical data platform**. The operational database (Phase 2) stores live hotel data optimised for writes. The data warehouse stores historical, denormalised data optimised for reads, reporting, and machine learning.

### What Was Built

| Component | Technology | Purpose |
|---|---|---|
| Incremental ETL | Python + psycopg2 | Extract changed rows from source DB |
| Data Lake (raw zone) | MinIO (S3-compatible) | Durable raw JSON snapshot storage |
| Staging Schema | PostgreSQL (hotelmind_staging) | Mirror of source tables post-extraction |
| Data Warehouse | PostgreSQL (hotelmind_warehouse) | Star schema with dims, facts, and marts |
| Transformations | dbt 1.8 | Staging → Warehouse → Mart SQL models |
| Data Quality | Great Expectations 0.18 | Validation gates between ETL stages |
| Orchestration | Apache Airflow 2.9 | Daily scheduling and dependency management |

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ OPERATIONAL LAYER (Phase 2)                                         │
│                                                                     │
│   FastAPI → PostgreSQL (hotelmind_db) — 17 tables, OLTP writes      │
└────────────────────────────┬────────────────────────────────────────┘
                             │  Incremental Extract
                             │  WHERE updated_at > last_watermark
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│ EXTRACTION LAYER                                                    │
│                                                                     │
│   HotelExtractor      BookingExtractor                              │
│   RestaurantExtractor StaffExtractor                                │
│                                                                     │
│   WatermarkStore → etl_watermarks (warehouse DB)                   │
└──────────┬────────────────────────────────────────────────────────--┘
           │                      │
           ▼                      ▼
┌──────────────────┐   ┌──────────────────────────────────────────────┐
│ DATA LAKE        │   │ STAGING SCHEMA (hotelmind_staging)           │
│ MinIO (S3)       │   │                                              │
│                  │   │  hotels, branches, rooms, room_types         │
│ raw/             │   │  guests, reservations, occupancy_snapshots   │
│  table/          │   │  food_categories, menu_items                 │
│    year=YYYY/    │   │  restaurant_tables, restaurant_orders        │
│    month=MM/     │   │  order_items, departments, employees         │
│    day=DD/       │   │  schedules, attendance                       │
│    run_id.json   │   │                                              │
└──────────────────┘   └────────────────┬─────────────────────────────┘
                                        │  dbt: staging models (views)
                                        │  stg_hotels, stg_reservations …
                                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ WAREHOUSE SCHEMA (hotelmind_warehouse)                              │
│                                                                     │
│ DIMENSIONS (tables)                                                 │
│   dim_date       dim_hotel      dim_branch     dim_room             │
│   dim_guest      dim_employee   dim_menu_item                       │
│                                                                     │
│ FACTS (incremental tables)                                          │
│   fact_booking           fact_restaurant_sale                       │
│   fact_occupancy_daily   fact_staff_attendance                      │
│                                                                     │
│ MARTS (aggregated tables)                                           │
│   mart_revenue_daily     mart_occupancy_daily                       │
│   mart_restaurant_daily  mart_staff_daily                           │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             │  Consumed by
           ┌─────────────────┼───────────────────┐
           ▼                 ▼                   ▼
     Phase 4 ML        Phase 5 GenAI       Phase 10 BI
     (forecasting)     (AI Assistant)      (Executive Dashboard)
```

---

## 3. Data Flow

### Daily Pipeline (Airflow DAG: `daily_hotelmind_etl`)

```
02:00 AM ──► extract_hotel ──┐
              extract_booking ─┤
              extract_restaurant─┤──► lake_load ──► staging_load ──► quality_gate ──► dbt_run ──► dbt_test
              extract_staff ──┘
```

**Step 1 — Extract** (parallel, 4 tasks)
- Each domain extractor reads `WHERE updated_at > last_watermark` from the source DB
- Results are held in Airflow XCom as serialised dicts
- WatermarkStore is updated after each successful extract

**Step 2 — Lake Load** (single task)
- Writes raw JSON to MinIO `hotelmind-raw/{table}/year={Y}/month={M}/day={D}/{run_id}.json`
- Failure is logged but non-fatal — pipeline continues

**Step 3 — Staging Load** (single task)
- Upserts rows into `hotelmind_staging.*` tables using `INSERT … ON CONFLICT DO UPDATE`
- Each domain is isolated — one table's failure doesn't block others

**Step 4 — Quality Gate** (single task)
- Runs Great Expectations suites against staging tables
- Validates: null checks, accepted values, uniqueness, value ranges
- Failure blocks dbt run (warehouse stays at last known-good state)

**Step 5 — dbt Run** (single task)
- Runs `dbt run` — builds staging views, dimension tables, fact tables, mart tables
- Incremental models only process new/changed rows

**Step 6 — dbt Test** (single task)
- Runs `dbt test` — validates all schema tests defined in `schema.yml`
- Test failures alert but don't roll back

---

## 4. Incremental Strategy

### Watermark-Based Extraction

Every source table has an `updated_at` column (Phase 2 design decision). The ETL extracts only rows where `updated_at > last_watermark`:

```python
# From etl/extract/base_extractor.py
SELECT * FROM public.{table} WHERE updated_at > %(since)s ORDER BY updated_at ASC
```

The new watermark is `max(updated_at)` from extracted rows. This ensures:
- First run: extracts all history (watermark = 1970-01-01)
- Subsequent runs: extract only changes since last run
- Rerunning the same day: extracts 0 rows (idempotent)

Junction table (`room_type_amenities`) has no `updated_at` → full extract every run. It is small and rarely changes.

### dbt Incremental Models

Fact tables use `{{ is_incremental() }}` to process only new rows:

```sql
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

First run: full table scan. Subsequent runs: only new/updated records.

---

## 5. Data Quality Architecture

### Validation Points

```
Source DB → [Extract] → Lake → Staging → [Quality Gate] → dbt → Warehouse
                                              ▲
                                    Great Expectations suites
```

### Suites

| Suite | Table | Key Checks |
|---|---|---|
| staging_reservations | reservations | not_null: id, room_id, guest_id; accepted_values: status; non-negative: total_amount |
| staging_restaurant_orders | restaurant_orders | not_null: id, branch_id, status; accepted_values: status |
| staging_employees | employees | not_null: id, email, department_id; unique: email |

### dbt Tests

All staging models have schema tests:
- `not_null` on all PK and FK columns
- `unique` on all PK columns
- `accepted_values` on all status enumerations
- `relationships` between FK columns (where applicable)

---

## 6. dbt Layer Design

### Model Layers (Staging → Warehouse → Mart)

```
hotelmind_staging (source)
    └── stg_* models (views — rename, cast, compute derived fields)
        └── dim_* models (tables — denormalised, surrogate keyed)
        └── fact_* models (incremental tables — measures, FK joins)
            └── mart_* models (tables — pre-aggregated for BI/ML)
```

### Why Three Layers?

**Staging (`stg_`):**
- Views — no storage overhead
- Single concern: rename columns and fix types
- Isolates upstream schema changes (only staging model changes if source changes)

**Warehouse Dimensions/Facts:**
- Tables — materialised for join performance
- Apply business logic: surrogate keys, status flags, computed measures
- Fact tables are incremental — only process deltas

**Marts:**
- Pre-aggregated tables — fast for BI dashboards and ML feature extraction
- Contain window functions: rolling averages, cumulative sums (MTD/YTD)
- Single source for Phase 4 ML feature engineering

---

## 7. Star Schema Design

See [warehouse_schema.md](warehouse_schema.md) for complete table definitions.

**Design Principles Applied:**
- Surrogate keys (MD5 hash of natural key via `dbt_utils.generate_surrogate_key`)
- SCD Type 1 for all dimensions (overwrite — no history tracking needed in Phase 3)
- Degenerate dimensions in fact tables (reservation_id, order_id — stored in fact, not joined to a dim)
- Conformed dimensions (dim_branch, dim_date used across all facts)
- Grain declared in each fact table header comment

---

## 8. Infrastructure

### Docker Services (hotelmind-data/docker-compose.yml)

| Service | Port | Purpose |
|---|---|---|
| warehouse-db | 5433 | Analytical PostgreSQL (staging + warehouse schemas) |
| minio | 9000/9001 | S3-compatible data lake (API / Console UI) |
| airflow-db | 5434 | Airflow metadata PostgreSQL |
| airflow-webserver | 8080 | Airflow UI |
| airflow-scheduler | — | DAG scheduler |
| minio-init | — | One-shot bucket creation |
| airflow-init | — | One-shot DB migration + admin user |

### Port Summary

| Port | Service | Note |
|---|---|---|
| 5432 | Source (ops) DB | From Phase 1/2 docker-compose |
| 5433 | Warehouse DB | New in Phase 3 |
| 5434 | Airflow metadata DB | New in Phase 3 |
| 6379 | Redis | From Phase 1/2 |
| 8080 | Airflow UI | New in Phase 3 |
| 9000 | MinIO S3 API | New in Phase 3 |
| 9001 | MinIO Console | New in Phase 3 |

---

## 9. Lessons Learned

### Async → Sync Boundary
Phase 2 backend uses `asyncpg` (async). Phase 3 ETL uses `psycopg2` (sync). This is intentional — ETL runs in a separate process/container and does not share the FastAPI event loop. The clean separation means the ETL can be scaled independently.

### Watermark vs CDC
Change Data Capture (CDC) with Debezium/Kafka would give sub-second latency but adds significant operational complexity. The `updated_at` watermark approach is appropriate for daily batch ETL and has zero infrastructure overhead. Phase 7 (Real-Time) will add Kafka streaming as a complement, not replacement.

### PostgreSQL as Warehouse
Using PostgreSQL for both the operational DB and warehouse is unconventional but defensible for this scale. The schema separation (`hotelmind_staging`, `hotelmind_warehouse`) enforces logical isolation. In Phase 8, the warehouse connection string is the only change needed to point to Redshift.

### dbt Incremental on Fact Tables
Fact tables using `{{ is_incremental() }}` mean the first `dbt run` does a full table scan, which is slow at scale. Subsequent runs are fast. For a portfolio project this is fine; for production at scale, partition pruning or a surrogate date filter would be added.

### dim_date Generated in SQL
Using `generate_series()` in `dim_date.sql` avoids a seed CSV dependency. The date spine is generated fresh each time `dim_date` is rebuilt. This is more maintainable than a 3,650-row CSV file.

### Great Expectations `ephemeral` Mode
GE 0.18 supports `mode="ephemeral"` for in-process validation without a GE context directory. This dramatically simplifies the integration — no `great_expectations.yml`, no `checkpoints/` directories, no CLI setup. Pure programmatic Python.

---

## 10. Future Enhancements

### Phase 4 Readiness
- `mart_occupancy_daily` has 30-day rolling avg and lag features ready for Prophet/XGBoost
- `mart_revenue_daily` has ADR and RevPAR metrics for dynamic pricing model
- `dim_guest.lifetime_spend` enables churn prediction model features

### Phase 7 Real-Time
- Kafka consumer writes to `hotelmind_staging` directly (same upsert pattern as batch ETL)
- Airflow DAG remains for daily full reconciliation
- The watermark store becomes a hybrid: batch uses it, streaming bypasses it

### Phase 8 Cloud
- Swap `WAREHOUSE_DB_HOST/PORT` in `.env` to point to Redshift endpoint
- Change dbt profile type from `postgres` to `redshift`
- Replace MinIO with real S3 by setting `S3_ENDPOINT_URL=""` and using IAM roles
- Deploy Airflow to AWS MWAA (Managed Workflows for Apache Airflow)

### SCD Type 2 for Rooms and Pricing
Currently dim_room is SCD Type 1 (overwrites). If room prices change frequently, Type 2 (add row with effective dates) would allow historical revenue analysis at the correct historical price. This is a natural Phase 4 enhancement.

### Data Lineage
dbt generates a lineage DAG visible in `dbt docs serve`. Integrate with Airflow's data-aware scheduling in a future phase for event-driven (not time-driven) pipeline triggers.
