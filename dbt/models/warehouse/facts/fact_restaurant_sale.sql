{{
    config(
        materialized  = 'incremental',
        schema        = 'hotelmind_warehouse',
        unique_key    = 'surrogate_key',
        on_schema_change = 'append_new_columns'
    )
}}

-- Fact table: one row per order line item (order_item grain).
-- This gives maximum analytical flexibility — roll up to order, branch, or date.
-- Measures: quantity, unit_price, subtotal
-- Incremental on order_items.updated_at

with order_items as (
    select * from {{ ref('stg_order_items') }}

    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

orders as (
    select
        order_id,
        branch_id,
        order_status,
        opened_at,
        closed_at,
        duration_minutes
    from {{ ref('stg_restaurant_orders') }}
),

menu_items as (
    select menu_item_key, menu_item_id, branch_key, item_name, category_name, current_price
    from {{ ref('dim_menu_item') }}
),

branches as (
    select branch_key, branch_id, hotel_key
    from {{ ref('dim_branch') }}
),

order_dates as (
    select date_key, date
    from {{ ref('dim_date') }}
),

final as (
    select
        {{ generate_surrogate_key(['oi.order_item_id']) }}  as surrogate_key,

        -- Natural keys
        oi.order_item_id,
        oi.order_id,
        o.order_status,

        -- Foreign keys
        mi.menu_item_key,
        mi.menu_item_id,
        b.branch_key,
        b.branch_id,
        b.hotel_key,
        d.date_key                                          as order_date_key,

        -- Descriptive
        mi.item_name,
        mi.category_name,
        mi.current_price                                    as current_menu_price,

        -- Measures
        oi.quantity,
        oi.unit_price,                                      -- price at time of order
        oi.subtotal,
        o.duration_minutes                                  as order_duration_minutes,

        -- Price variance (current price vs price charged — useful for pricing analysis)
        (oi.unit_price - mi.current_price)::numeric(10,2)  as price_variance,

        -- Timestamps
        o.opened_at,
        o.closed_at,
        oi.created_at,
        oi.updated_at
    from order_items oi
    join orders o       on oi.order_id     = o.order_id
    join menu_items mi  on oi.menu_item_id = mi.menu_item_id
    join branches b     on o.branch_id     = b.branch_id
    left join order_dates d on o.opened_at::date = d.date
)

select * from final
