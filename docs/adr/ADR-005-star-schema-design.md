# ADR-005: Kimball Star Schema

**Status:** Accepted
**Date:** 2026-06-19

## Decision

Design the analytical warehouse using Kimball's dimensional modelling (star schema): wide denormalised dimension tables + narrow fact tables with numeric measures.

## Context

Three alternative warehouse models were considered:
1. **3NF (Third Normal Form):** Normalised like the source DB
2. **OBT (One Big Table):** All columns in one flat table
3. **Star Schema (Kimball):** Dimension + fact tables

## Why Star Schema?

### For BI Dashboards
Dashboards need simple queries: `SELECT SUM(total_revenue) FROM fact_booking JOIN dim_date … WHERE year = 2026`. Star schema makes these 2-3 table joins. 3NF requires 8+ table joins for the same query. OBT works but wastes storage and makes column discovery confusing.

### For ML Feature Engineering
Phase 4 ML models need feature vectors per day per branch. The mart layer (`mart_revenue_daily`, `mart_occupancy_daily`) provides this directly — one row per branch per date with all features pre-computed. ML models can read directly from marts with `pd.read_sql()`.

### For Phase 5 GenAI/RAG
The AI Assistant needs to answer natural language questions about hotel data. A star schema with clear table names and well-named columns is far easier to map to SQL than a normalised 3NF schema. The mart tables provide named aggregates ("What was total revenue last week?" maps directly to `SUM(mart_revenue_daily.total_revenue)`).

## Grain Decisions

| Fact Table | Grain | Rationale |
|---|---|---|
| fact_booking | Per reservation | Single reservation = single fact row; allows flexible aggregation |
| fact_restaurant_sale | Per order_item | Most granular — can roll up to order, category, day, branch |
| fact_occupancy_daily | Per branch × date | Daily is the right cadence; computed from reservations not from snapshots |
| fact_staff_attendance | Per attendance record | Single clock-in = single fact row |

## Consequences

**Positive:**
- Joins are simple: fact → dim (2 tables max for most queries)
- Dimension tables are wide and human-readable
- Aggregations on fact tables are fast (narrow tables = more rows per page)
- Conformed dimensions (dim_date, dim_branch) work across all facts

**Negative:**
- Denormalization means dimension updates require full table rebuild (acceptable for SCD Type 1)
- More tables to maintain vs OBT
- Surrogate keys (MD5 hashes) add a column that's not in the source
