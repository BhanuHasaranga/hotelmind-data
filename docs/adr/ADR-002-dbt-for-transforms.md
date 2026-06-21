# ADR-002: dbt for Transformations

**Status:** Accepted
**Date:** 2026-06-19

## Decision

Use dbt (data build tool) for all SQL transformations from staging to warehouse to marts, rather than raw Python + SQL scripts.

## Context

The transformation layer needs: version control, testability, documentation, lineage visibility, and incremental processing. Raw SQL scripts in Python lack all of these without significant custom tooling.

## Consequences

**Positive:**
- Built-in data lineage DAG (visible in `dbt docs serve`)
- Schema tests (`not_null`, `unique`, `accepted_values`) are co-located with models
- Incremental materialisation handles delta processing without custom logic
- Industry-standard tool — appears on most data engineering job requirements
- dbt docs generate → portfolio artifact (interactive lineage diagram)
- Adding a new model is 1 SQL file + 1 schema.yml entry

**Negative:**
- dbt is SQL-only — Python models require `dbt-python` which has more setup
- The three-layer model structure (staging → warehouse → mart) adds files compared to a single SQL script
- Requires understanding of Jinja templating (`{{ ref() }}`, `{{ source() }}`, `{% if is_incremental() %}`)

## Alternatives Considered

**Raw Python/pandas:** More flexible but produces no lineage, no tests, no docs.
**SQLAlchemy ORM:** Too low-level for analytical transforms; designed for OLTP not OLAP.
**Spark:** Massive overkill for this data volume.
