# ADR-001: Data Warehouse in PostgreSQL

**Status:** Accepted
**Date:** 2026-06-19

## Decision

Use a dedicated PostgreSQL 16 instance (port 5433) with separate schemas (`hotelmind_staging`, `hotelmind_warehouse`) as the analytical data warehouse, rather than a cloud data warehouse (Redshift, BigQuery, Snowflake).

## Context

HotelMind AI is a portfolio/self-learning project. The data volumes in Phases 3–7 are small (single hotel, demo data). Cloud data warehouses incur cost and require cloud accounts, which adds friction for local development and portfolio demonstration.

## Consequences

**Positive:**
- Zero cloud cost for local development and demo
- Same database technology as the operational DB — reduces cognitive overhead
- Trivially replaceable: change the dbt `profiles.yml` connection from `postgres` to `redshift` in Phase 8
- Full SQL compatibility — all dbt models work unchanged in Redshift

**Negative:**
- PostgreSQL lacks columnar storage — large analytical queries are slower than a true data warehouse
- No auto-scaling, no separation of compute and storage
- Not realistic for production at scale (>100M rows)

## Mitigation

The schema/model design follows Redshift best practices (star schema, sort keys as date_key, no complex joins in hot paths). Migration to Redshift in Phase 8 requires only connection string changes.
