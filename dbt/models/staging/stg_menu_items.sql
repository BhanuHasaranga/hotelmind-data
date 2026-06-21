with source as (
    select * from {{ source('hotelmind_staging', 'menu_items') }}
),

renamed as (
    select
        id                              as menu_item_id,
        category_id,
        name                            as item_name,
        description,
        price::numeric(10,2)            as price,
        coalesce(is_available, true)    as is_available,
        created_at,
        updated_at
    from source
)

select * from renamed
