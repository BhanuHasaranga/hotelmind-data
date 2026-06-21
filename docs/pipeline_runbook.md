# HotelMind Data Engineering — Pipeline Runbook

## Prerequisites

- Docker Desktop running
- Python 3.12+ with virtualenv
- Phase 1 & 2 stack running (`docker compose up -d` from `HotelMind_AI/`)

---

## First-Time Setup

### 1. Clone and configure

```bash
cd HotelMind_AI/hotelmind-data

# Create .env from template
cp .env.example .env
# Edit .env if defaults don't match your Phase 1/2 setup
```

### 2. Start the data stack

```bash
docker compose up -d
# Starts: warehouse-db (:5433) + MinIO (:9000/:9001) + Airflow (:8080)
# First start takes ~2 minutes for airflow-init to complete

docker compose ps
# All services should show "healthy" or "running"
```

### 3. Create Python environment

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

### 4. Initialise the warehouse

```bash
python scripts/init_warehouse.py
# Creates hotelmind_staging + hotelmind_warehouse schemas
# Creates all 19 staging tables and etl_watermarks table
```

### 5. Install dbt packages

```bash
cd dbt
cp profiles.yml.example profiles.yml
# Edit profiles.yml with your warehouse DB credentials if not using defaults
dbt deps
cd ..
```

---

## Running the Pipeline

### Manual run (full pipeline)

```bash
python scripts/run_etl.py
# Extracts → Lake → Staging → dbt run → dbt test
```

### Extract only (no warehouse changes)

```bash
python scripts/run_etl.py --dry-run
```

### ETL without dbt (for debugging staging)

```bash
python scripts/run_etl.py --skip-dbt
```

### dbt only (no extract)

```bash
cd dbt
dbt run
dbt test
```

---

## Airflow UI

Open `http://localhost:8080` (user: `admin`, password: `admin`)

### Available DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `daily_hotelmind_etl` | 2:00 AM daily | Full pipeline |
| `warehouse_refresh` | Manual | dbt-only rebuild |
| `hotelmind_backfill` | Manual | Re-extract date range |

### Trigger a manual run

1. Click the DAG name
2. Click "Trigger DAG" (▷ button)
3. For `hotelmind_backfill`, add JSON config: `{"from_date": "2025-01-01"}`

---

## Backfilling Historical Data

### Backfill all tables from a date

```bash
python scripts/backfill.py --from 2025-01-01
python scripts/run_etl.py
```

### Backfill specific tables only

```bash
python scripts/backfill.py --from 2025-06-01 --tables reservations restaurant_orders
python scripts/run_etl.py --skip-dbt
cd dbt && dbt run --select fact_booking fact_restaurant_sale mart_revenue_daily
```

### Full reset (re-extract everything)

```bash
python scripts/backfill.py --full-reset
python scripts/run_etl.py
```

---

## Querying the Warehouse

### Connect

```bash
psql -h localhost -p 5433 -U hotelmind_dw -d hotelmind_warehouse
```

### Example queries

```sql
-- Daily revenue for a branch
SELECT date, total_revenue, room_revenue, fb_revenue, revpar
FROM hotelmind_warehouse.mart_revenue_daily
WHERE branch_id = '<your-branch-uuid>'
ORDER BY date DESC
LIMIT 30;

-- Occupancy trend (last 30 days)
SELECT occupancy_date, occupancy_pct, occupancy_7day_avg
FROM hotelmind_warehouse.mart_occupancy_daily
WHERE branch_id = '<your-branch-uuid>'
ORDER BY occupancy_date DESC
LIMIT 30;

-- Check ETL watermarks (what was last extracted and when)
SELECT table_name, last_extracted, last_run_at, rows_extracted
FROM hotelmind_warehouse.etl_watermarks
ORDER BY last_run_at DESC;
```

---

## MinIO Console

Open `http://localhost:9001` (user: `hotelmind_minio`, password: `hotelmind_minio_secret`)

Navigate to `hotelmind-raw` bucket to view raw extraction files:
```
hotelmind-raw/
  hotels/
    year=2026/month=06/day=19/20260619T020000Z.json
  reservations/
    year=2026/month=06/day=19/20260619T020000Z.json
  ...
```

---

## Troubleshooting

### Warehouse DB not reachable

```bash
docker compose ps warehouse-db
# Should show "healthy"
# If not: docker compose logs warehouse-db
```

### dbt profile not found

```bash
ls dbt/profiles.yml
# If missing: cp dbt/profiles.yml.example dbt/profiles.yml
```

### Staging tables don't exist

```bash
python scripts/init_warehouse.py
# Re-runs migration (idempotent)
```

### Airflow DAGs not showing

```bash
docker compose logs airflow-scheduler | tail -50
# Check for import errors in DAG files
```

### Reset everything (start fresh)

```bash
docker compose down -v    # destroys all volumes
docker compose up -d
python scripts/init_warehouse.py
python scripts/backfill.py --full-reset
python scripts/run_etl.py
```

---

## dbt Docs

```bash
cd dbt
dbt docs generate
dbt docs serve
# Open http://localhost:8081 for interactive lineage + model docs
```
