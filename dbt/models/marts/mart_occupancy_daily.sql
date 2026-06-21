{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

-- Mart: Daily occupancy metrics per branch with rolling averages.
-- Grain: branch_id × date
-- Primary input for Phase 4 ML occupancy forecasting model.

with occupancy as (
    select
        fod.surrogate_key,
        fod.branch_key,
        fod.branch_id,
        fod.hotel_key,
        fod.date_key,
        fod.occupancy_date,
        fod.total_rooms,
        fod.occupied_rooms,
        fod.available_rooms,
        fod.occupancy_pct
    from {{ ref('fact_occupancy_daily') }} fod
),

branches as (
    select branch_key, branch_id, hotel_key, branch_name, hotel_name, city, country
    from {{ ref('dim_branch') }}
),

dates as (
    select date_key, date, year, month, quarter, week_of_year, day_of_week, day_name, is_weekend
    from {{ ref('dim_date') }}
),

enriched as (
    select
        o.branch_id,
        o.hotel_key,
        b.branch_name,
        b.hotel_name,
        b.city,
        b.country,
        o.date_key,
        o.occupancy_date,
        d.year,
        d.month,
        d.quarter,
        d.week_of_year,
        d.day_of_week,
        d.day_name,
        d.is_weekend,
        o.total_rooms,
        o.occupied_rooms,
        o.available_rooms,
        o.occupancy_pct,
        -- Rolling averages (for ML feature engineering)
        avg(o.occupancy_pct) over (
            partition by o.branch_id
            order by o.date_key
            rows between 6 preceding and current row
        )::numeric(5,2)                             as occupancy_7day_avg,
        avg(o.occupancy_pct) over (
            partition by o.branch_id
            order by o.date_key
            rows between 29 preceding and current row
        )::numeric(5,2)                             as occupancy_30day_avg,
        -- Same day of week last week (useful feature for ML)
        lag(o.occupancy_pct, 7) over (
            partition by o.branch_id
            order by o.date_key
        )                                           as occupancy_pct_lag_7d,
        -- Same period last year
        lag(o.occupancy_pct, 365) over (
            partition by o.branch_id
            order by o.date_key
        )                                           as occupancy_pct_lag_365d,
        -- MTD average occupancy
        avg(o.occupancy_pct) over (
            partition by o.branch_id, d.year, d.month
            order by o.date_key
            rows between unbounded preceding and current row
        )::numeric(5,2)                             as occupancy_mtd_avg
    from occupancy o
    join branches b on o.branch_id = b.branch_id
    join dates d    on o.date_key  = d.date_key
)

select * from enriched
