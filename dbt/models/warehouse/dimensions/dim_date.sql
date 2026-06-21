{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

-- Date dimension: covers 2020-01-01 through 2030-12-31.
-- Generated via a PostgreSQL date series — no seed file dependency.
-- date_key follows YYYYMMDD integer format for fast integer joins.

with date_spine as (
    select
        generate_series(
            '{{ var("dim_date_start") }}'::date,
            '{{ var("dim_date_end") }}'::date,
            interval '1 day'
        )::date as date
),

enriched as (
    select
        -- Primary key: YYYYMMDD integer (e.g., 20260615)
        cast(to_char(date, 'YYYYMMDD') as integer)      as date_key,
        date,
        -- Year / Quarter / Month
        extract(year  from date)::integer                as year,
        extract(quarter from date)::integer              as quarter,
        extract(month from date)::integer                as month,
        trim(to_char(date, 'Month'))                     as month_name,
        to_char(date, 'Mon')                             as month_abbr,
        -- Week
        extract(week from date)::integer                 as week_of_year,
        -- Day
        extract(day from date)::integer                  as day_of_month,
        extract(dow  from date)::integer                 as day_of_week, -- 0=Sunday
        trim(to_char(date, 'Day'))                       as day_name,
        to_char(date, 'Dy')                              as day_abbr,
        -- Flags
        case when extract(dow from date) in (0, 6)
             then true else false end                    as is_weekend,
        -- is_holiday placeholder — can be enriched with a holiday feed in Phase 4
        false                                            as is_holiday,
        -- Fiscal year helpers (calendar year = fiscal year for now)
        extract(year from date)::integer                 as fiscal_year,
        extract(quarter from date)::integer              as fiscal_quarter,
        -- First/last day helpers for MTD/QTD/YTD calculations
        date_trunc('month', date)::date                  as first_day_of_month,
        (date_trunc('month', date) + interval '1 month - 1 day')::date
                                                         as last_day_of_month,
        date_trunc('year', date)::date                   as first_day_of_year
    from date_spine
)

select * from enriched
order by date_key
