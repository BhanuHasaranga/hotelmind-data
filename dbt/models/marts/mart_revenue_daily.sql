{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

-- Mart: Combined daily revenue per branch.
-- Grain: branch_id × date
-- Inputs: fact_booking (room revenue) + fact_restaurant_sale (F&B revenue)
-- Outputs: room_revenue, fb_revenue, total_revenue, ADR, RevPAR
--
-- This is the primary input for:
--   - Phase 4 ML: revenue forecasting model features
--   - Phase 5 GenAI: "What was revenue last week?" queries
--   - Phase 10 Executive Dashboard: revenue trend charts

with date_spine as (
    select date_key, date, year, month, quarter, week_of_year, is_weekend
    from {{ ref('dim_date') }}
    where date >= '2020-01-01'
      and date <= current_date
),

branches as (
    select branch_key, branch_id, hotel_key, branch_name, hotel_name, city, country
    from {{ ref('dim_branch') }}
),

-- Room revenue per branch per check-in date
room_revenue as (
    select
        branch_id,
        check_in_date_key                               as date_key,
        count(distinct reservation_id)                  as room_bookings,
        sum(total_amount)                               as room_revenue,
        sum(nights)                                     as total_nights_sold,
        avg(avg_daily_rate)::numeric(10,2)              as avg_daily_rate
    from {{ ref('fact_booking') }}
    where not is_terminal
    group by branch_id, check_in_date_key
),

-- F&B revenue per branch per order date
fb_revenue as (
    select
        branch_id,
        order_date_key                                  as date_key,
        count(distinct order_id)                        as total_orders,
        sum(subtotal)                                   as fb_revenue,
        sum(quantity)                                   as items_sold
    from {{ ref('fact_restaurant_sale') }}
    where order_status = 'CLOSED'
    group by branch_id, order_date_key
),

-- Occupancy for RevPAR calculation
occupancy as (
    select
        branch_id,
        date_key,
        total_rooms,
        occupancy_pct
    from {{ ref('fact_occupancy_daily') }}
),

combined as (
    select
        b.branch_key,
        b.branch_id,
        b.hotel_key,
        b.branch_name,
        b.hotel_name,
        b.city,
        b.country,
        ds.date_key,
        ds.date,
        ds.year,
        ds.month,
        ds.quarter,
        ds.week_of_year,
        ds.is_weekend,
        -- Room revenue metrics
        coalesce(rr.room_bookings, 0)                   as room_bookings,
        coalesce(rr.room_revenue, 0)::numeric(12,2)     as room_revenue,
        coalesce(rr.total_nights_sold, 0)               as total_nights_sold,
        coalesce(rr.avg_daily_rate, 0)::numeric(10,2)   as avg_daily_rate,
        -- F&B metrics
        coalesce(fb.total_orders, 0)                    as restaurant_orders,
        coalesce(fb.fb_revenue, 0)::numeric(12,2)       as fb_revenue,
        coalesce(fb.items_sold, 0)                      as items_sold,
        -- Combined totals
        (coalesce(rr.room_revenue, 0) + coalesce(fb.fb_revenue, 0))::numeric(12,2)
                                                        as total_revenue,
        -- RevPAR = Room Revenue / Total Rooms (not available rooms)
        case
            when coalesce(occ.total_rooms, 0) > 0
            then (coalesce(rr.room_revenue, 0) / occ.total_rooms)::numeric(10,2)
            else 0
        end                                             as revpar,
        -- Occupancy
        coalesce(occ.occupancy_pct, 0)                  as occupancy_pct
    from branches b
    cross join date_spine ds
    left join room_revenue rr   on b.branch_id = rr.branch_id and ds.date_key = rr.date_key
    left join fb_revenue fb     on b.branch_id = fb.branch_id and ds.date_key = fb.date_key
    left join occupancy occ     on b.branch_id = occ.branch_id and ds.date_key = occ.date_key
),

with_rolling as (
    select
        *,
        -- 7-day rolling revenue (for trend smoothing)
        avg(total_revenue) over (
            partition by branch_id
            order by date_key
            rows between 6 preceding and current row
        )::numeric(12,2)                                as revenue_7day_avg,
        -- MTD cumulative revenue
        sum(total_revenue) over (
            partition by branch_id, year, month
            order by date_key
            rows between unbounded preceding and current row
        )::numeric(12,2)                                as revenue_mtd,
        -- YTD cumulative revenue
        sum(total_revenue) over (
            partition by branch_id, year
            order by date_key
            rows between unbounded preceding and current row
        )::numeric(12,2)                                as revenue_ytd
    from combined
)

select * from with_rolling
