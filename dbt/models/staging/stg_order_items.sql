with source as (
    select * from {{ source('hotelmind_staging', 'order_items') }}
),

renamed as (
    select
        id                              as order_item_id,
        order_id,
        menu_item_id,
        quantity,
        unit_price::numeric(10,2)       as unit_price,
        subtotal::numeric(10,2)         as subtotal,
        created_at,
        updated_at
    from source
)

select * from renamed
