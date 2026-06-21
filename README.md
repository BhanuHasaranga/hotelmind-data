# HotelMind Data Engineering (Phase 3)

> **AI-Powered Hospitality Intelligence Platform — Data Engineering Layer**

Phase 3 builds the analytical data platform on top of the Phase 2 operational system. It extracts hotel operational data incrementally, loads it into a data lake and staging schema, transforms it into a star-schema data warehouse using dbt, validates quality with Great Expectations, and orchestrates everything with Apache Airflow.

---

## Architecture

```
[Operational DB: PostgreSQL]
          │  Incremental Extract (updated_at watermark)
          ▼
[MinIO Data Lake] ← raw JSON snapshots, partitioned by date
          │
          ▼
[hotelmind_staging] ← 19 staging tables (upserted)
          │  dbt transformations
          ▼
[hotelmind_warehouse]
  Dimensions:  dim_date · dim_hotel · dim_branch · dim_room
               dim_guest · dim_employee · dim_menu_item
  Facts:       fact_booking · fact_restaurant_sale
               fact_occupancy_daily · fact_staff_attendance
  Marts:       mart_revenue_daily · mart_occupancy_daily
               mart_restaurant_daily · mart_staff_daily
          │
          ▼
[Airflow DAG] orchestrates: extract → lake → staging → quality gate → dbt
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Transformations | dbt-core 1.8 + dbt-postgres |
| Data Lake | MinIO (S3-compatible) |
| Extract/Load | Python 3.12 + psycopg2 |
| Data Quality | Great Expectations 0.18 |
| Warehouse | PostgreSQL 16 |
| Config | pydantic-settings 2.x |
| Tests | pytest 7.x |

---

## Quick Start

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Create Python env
python -m venv .venv && .venv\Scripts\activate  # Windows
pip install -r requirements.txt

# 3. Initialise warehouse
cp .env.example .env
python scripts/init_warehouse.py

# 4. Install dbt packages
cd dbt && cp profiles.yml.example profiles.yml && dbt deps && cd ..

# 5. Run full ETL pipeline
python scripts/run_etl.py

# 6. Open dashboards
#   Airflow:  http://localhost:8080  (admin/admin)
#   MinIO:    http://localhost:9001  (hotelmind_minio/hotelmind_minio_secret)
```

See [docs/pipeline_runbook.md](docs/pipeline_runbook.md) for full operational guide.

---

## Key Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Warehouse DB + MinIO + Airflow |
| `config/settings.py` | Centralised config (env-driven) |
| `etl/extract/base_extractor.py` | Watermark-based incremental extractor |
| `etl/watermark/watermark_store.py` | Per-table ETL watermark persistence |
| `etl/load/staging_loader.py` | Idempotent staging upserts |
| `etl/load/lake_loader.py` | MinIO raw JSON writer |
| `dbt/models/` | Staging → Dimensions → Facts → Marts |
| `quality/validator.py` | Great Expectations staging validation |
| `orchestration/dags/daily_etl_dag.py` | Airflow master DAG |
| `scripts/run_etl.py` | CLI pipeline runner |
| `scripts/backfill.py` | Backfill / watermark reset |
| `warehouse/migrations/001_create_schemas.sql` | Schema DDL |

---

## Documentation

- [Architecture](docs/phase3_architecture.md)
- [Warehouse Schema](docs/warehouse_schema.md)
- [Pipeline Runbook](docs/pipeline_runbook.md)
- [ADR-001: PostgreSQL as Warehouse](docs/adr/ADR-001-warehouse-in-postgres.md)
- [ADR-002: dbt for Transforms](docs/adr/ADR-002-dbt-for-transforms.md)
- [ADR-003: MinIO Data Lake](docs/adr/ADR-003-minio-for-data-lake.md)
- [ADR-004: Watermark Incremental ETL](docs/adr/ADR-004-incremental-watermark.md)
- [ADR-005: Star Schema Design](docs/adr/ADR-005-star-schema-design.md)

---

## Future Phases

| Phase | Uses This Layer As |
|---|---|
| Phase 4 (ML) | mart_occupancy_daily, mart_revenue_daily → feature tables for forecasting models |
| Phase 5 (GenAI) | dim_guest, fact_booking, mart_revenue_daily → structured knowledge base for AI Assistant |
| Phase 6 (MLOps) | Airflow DAG extended to trigger model retraining after ETL |
| Phase 7 (Real-Time) | Kafka consumer writes to staging (same upsert pattern) |
| Phase 8 (Cloud) | Replace MinIO → S3, PostgreSQL warehouse → Redshift, Airflow → MWAA |
