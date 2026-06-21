{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

-- Mart: Daily restaurant performance per branch.
-- Grain: branch_id × date
-- Input for Phase 4 ML: restaurant demand forecasting model.

with sales as (
    select
        branch_id,
        hotel_key,
        order_date_key,
        order_id,
        order_status,
        category_name,
        item_name,
        quantity,
        subtotal
    from {{ ref('fact_restaurant_sale') }}
    where order_status = 'CLOSED'
),

dates as (
    select date_key, date, year, month, quarter, week_of_year, is_weekend, day_of_week
    from {{ ref('dim_date') }}
),

branches as (
    select branch_key, branch_id, hotel_key, branch_name, hotel_name
    from {{ ref('dim_branch') }}
),

-- Daily aggregation
daily_agg as (
    select
        s.branch_id,
        s.hotel_key,
        s.order_date_key,
        count(distinct s.order_id)          as total_orders,
        sum(s.quantity)                     as items_sold,
        sum(s.subtotal)::numeric(12,2)      as total_revenue,
        avg(s.subtotal)::numeric(10,2)      as avg_item_value,
        -- Revenue by top categories
        sum(case when s.category_name ilike '%breakfast%' then s.subtotal else 0 end)::numeric(10,2)
                                            as breakfast_revenue,
        sum(case when s.category_name ilike '%lunch%'     then s.subtotal else 0 end)::numeric(10,2)
                                            as lunch_revenue,
        sum(case when s.category_name ilike '%dinner%'    then s.subtotal else 0 end)::numeric(10,2)
                                            as dinner_revenue
    from sales s
    group by s.branch_id, s.hotel_key, s.order_date_key
),

with_context as (
    select
        b.branch_name,
        b.hotel_name,
        d.date,
        d.year,
        d.month,
        d.quarter,
        d.week_of_year,
        d.is_weekend,
        d.day_of_week,
        da.*,
        -- Average order value
        case
            when da.total_orders > 0
            then da.total_revenue / da.total_orders
            else 0
        end::numeric(10,2)                  as avg_order_value,
        -- Rolling 7-day averages for ML features
        avg(da.total_revenue) over (
            partition by da.branch_id
            order by da.order_date_key
            rows between 6 preceding and current row
        )::numeric(12,2)                    as revenue_7day_avg,
        avg(da.total_orders) over (
            partition by da.branch_id
            order by da.order_date_key
            rows between 6 preceding and current row
        )::numeric(8,2)                     as orders_7day_avg
    from daily_agg da
    join branches b on da.branch_id       = b.branch_id
    join dates    d on da.order_date_key  = d.date_key
)

select * from with_context
