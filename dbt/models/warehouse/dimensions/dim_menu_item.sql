{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

with menu_items as (
    select * from {{ ref('stg_menu_items') }}
),

categories as (
    select
        id          as category_id,
        branch_id,
        name        as category_name,
        display_order
    from {{ source('hotelmind_staging', 'food_categories') }}
),

branches as (
    select branch_key, branch_id, hotel_key, hotel_name, branch_name
    from {{ ref('dim_branch') }}
),

final as (
    select
        {{ generate_surrogate_key(['mi.menu_item_id']) }}    as menu_item_key,
        mi.menu_item_id,
        b.branch_key,
        b.branch_id,
        b.hotel_name,
        b.branch_name,
        mi.category_id,
        c.category_name,
        c.display_order                                      as category_display_order,
        mi.item_name,
        mi.description,
        mi.price                                             as current_price,
        mi.is_available,
        mi.created_at,
        mi.updated_at
    from menu_items mi
    join categories c   on mi.category_id = c.category_id
    join branches b     on c.branch_id    = b.branch_id
)

select * from final
