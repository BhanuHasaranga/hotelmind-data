-- =============================================================================
-- HotelMind Data Warehouse — Migration 001
-- Creates: staging schema, warehouse schema, ETL watermark table,
--          all staging tables (mirrors of source), warehouse placeholder tables.
--
-- Idempotent: safe to run multiple times (uses IF NOT EXISTS throughout).
-- Run via: python scripts/init_warehouse.py
-- =============================================================================

-- ── Schemas ───────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS hotelmind_staging;
CREATE SCHEMA IF NOT EXISTS hotelmind_warehouse;

-- ── ETL watermark table ───────────────────────────────────────────────────────
-- Records the last successfully extracted timestamp per source table.
-- Enables incremental extraction: only rows WHERE updated_at > last_watermark.
CREATE TABLE IF NOT EXISTS hotelmind_warehouse.etl_watermarks (
    table_name       VARCHAR(100) PRIMARY KEY,
    last_extracted   TIMESTAMPTZ  NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    last_run_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    rows_extracted   INTEGER      NOT NULL DEFAULT 0
);

COMMENT ON TABLE hotelmind_warehouse.etl_watermarks IS
    'Tracks per-table ETL watermarks for incremental extraction from the operational DB.';

-- =============================================================================
-- STAGING SCHEMA
-- Mirrors of operational tables — raw data after extraction, before transformation.
-- Upserted on each ETL run. No computed columns, no business logic.
-- =============================================================================

-- ── Hotel domain ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hotelmind_staging.hotels (
    id           UUID         PRIMARY KEY,
    name         VARCHAR(200) NOT NULL,
    star_rating  INTEGER,
    address      TEXT,
    city         VARCHAR(100),
    country      VARCHAR(100),
    phone        VARCHAR(30),
    email        VARCHAR(200),
    is_active    BOOLEAN,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    _loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.branches (
    id             UUID         PRIMARY KEY,
    hotel_id       UUID         NOT NULL,
    name           VARCHAR(200) NOT NULL,
    address        TEXT,
    city           VARCHAR(100),
    phone          VARCHAR(30),
    is_main_branch BOOLEAN,
    created_at     TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ,
    _loaded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.floors (
    id           UUID    PRIMARY KEY,
    branch_id    UUID    NOT NULL,
    floor_number INTEGER NOT NULL,
    name         VARCHAR(100),
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.room_types (
    id             UUID           PRIMARY KEY,
    branch_id      UUID           NOT NULL,
    name           VARCHAR(100)   NOT NULL,
    base_price     NUMERIC(10, 2) NOT NULL,
    max_occupancy  INTEGER,
    description    TEXT,
    created_at     TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ,
    _loaded_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.amenities (
    id         UUID        PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    icon       VARCHAR(50),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.room_type_amenities (
    room_type_id UUID NOT NULL,
    amenity_id   UUID NOT NULL,
    _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (room_type_id, amenity_id)
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.rooms (
    id           UUID        PRIMARY KEY,
    floor_id     UUID        NOT NULL,
    room_type_id UUID        NOT NULL,
    room_number  VARCHAR(20) NOT NULL,
    status       VARCHAR(20),
    is_active    BOOLEAN,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Booking domain ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hotelmind_staging.guests (
    id          UUID        PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(200) NOT NULL,
    phone       VARCHAR(30),
    id_type     VARCHAR(50),
    id_number   VARCHAR(100),
    nationality VARCHAR(100),
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ,
    _loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.reservations (
    id                  UUID           PRIMARY KEY,
    room_id             UUID           NOT NULL,
    guest_id            UUID           NOT NULL,
    check_in_date       DATE           NOT NULL,
    check_out_date      DATE           NOT NULL,
    status              VARCHAR(20)    NOT NULL,
    adults              INTEGER,
    children            INTEGER,
    total_amount        NUMERIC(10, 2) NOT NULL,
    paid_amount         NUMERIC(10, 2),
    special_requests    TEXT,
    cancelled_at        TIMESTAMPTZ,
    cancellation_reason TEXT,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ,
    _loaded_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.occupancy_snapshots (
    id            UUID           PRIMARY KEY,
    branch_id     UUID           NOT NULL,
    snapshot_date DATE           NOT NULL,
    total_rooms   INTEGER        NOT NULL,
    occupied_rooms INTEGER       NOT NULL,
    occupancy_pct NUMERIC(5, 2)  NOT NULL,
    created_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    _loaded_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- ── Restaurant domain ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hotelmind_staging.food_categories (
    id            UUID        PRIMARY KEY,
    branch_id     UUID        NOT NULL,
    name          VARCHAR(100) NOT NULL,
    display_order INTEGER,
    created_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.menu_items (
    id           UUID           PRIMARY KEY,
    category_id  UUID           NOT NULL,
    name         VARCHAR(200)   NOT NULL,
    description  TEXT,
    price        NUMERIC(10, 2) NOT NULL,
    is_available BOOLEAN,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    _loaded_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.restaurant_tables (
    id           UUID        PRIMARY KEY,
    branch_id    UUID        NOT NULL,
    table_number VARCHAR(20) NOT NULL,
    capacity     INTEGER,
    status       VARCHAR(20),
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.restaurant_orders (
    id           UUID           PRIMARY KEY,
    branch_id    UUID           NOT NULL,
    table_id     UUID,
    status       VARCHAR(20)    NOT NULL,
    opened_at    TIMESTAMPTZ    NOT NULL,
    closed_at    TIMESTAMPTZ,
    total_amount NUMERIC(10, 2),
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    _loaded_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.order_items (
    id           UUID           PRIMARY KEY,
    order_id     UUID           NOT NULL,
    menu_item_id UUID           NOT NULL,
    quantity     INTEGER        NOT NULL,
    unit_price   NUMERIC(10, 2) NOT NULL,
    subtotal     NUMERIC(10, 2) NOT NULL,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    _loaded_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- ── Staff domain ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hotelmind_staging.departments (
    id         UUID        PRIMARY KEY,
    branch_id  UUID        NOT NULL,
    name       VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.employees (
    id            UUID        PRIMARY KEY,
    department_id UUID        NOT NULL,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(200) NOT NULL,
    phone         VARCHAR(30),
    role          VARCHAR(100),
    hire_date     DATE        NOT NULL,
    is_active     BOOLEAN,
    created_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.schedules (
    id          UUID        PRIMARY KEY,
    employee_id UUID        NOT NULL,
    shift_date  DATE        NOT NULL,
    shift_start TIME        NOT NULL,
    shift_end   TIME        NOT NULL,
    notes       TEXT,
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ,
    _loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotelmind_staging.attendance (
    id          UUID        PRIMARY KEY,
    schedule_id UUID        NOT NULL,
    employee_id UUID        NOT NULL,
    clock_in    TIMESTAMPTZ NOT NULL,
    clock_out   TIMESTAMPTZ,
    status      VARCHAR(20),
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ,
    _loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Staging indexes ───────────────────────────────────────────────────────────
-- Support incremental ETL lookups and dbt join performance

CREATE INDEX IF NOT EXISTS idx_stg_reservations_dates
    ON hotelmind_staging.reservations (check_in_date, check_out_date);

CREATE INDEX IF NOT EXISTS idx_stg_reservations_status
    ON hotelmind_staging.reservations (status);

CREATE INDEX IF NOT EXISTS idx_stg_orders_branch_status
    ON hotelmind_staging.restaurant_orders (branch_id, status);

CREATE INDEX IF NOT EXISTS idx_stg_attendance_employee
    ON hotelmind_staging.attendance (employee_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_order
    ON hotelmind_staging.order_items (order_id);

-- =============================================================================
-- Done. Run `python scripts/init_warehouse.py` to execute this migration.
-- =============================================================================
