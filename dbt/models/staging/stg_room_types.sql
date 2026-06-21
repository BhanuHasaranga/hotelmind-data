with source as (
    select * from {{ source('hotelmind_staging', 'room_types') }}
),

renamed as (
    select
        id                              as room_type_id,
        branch_id,
        name                            as room_type_name,
        base_price::numeric(10,2)       as base_price,
        coalesce(max_occupancy, 2)      as max_occupancy,
        description,
        created_at,
        updated_at
    from source
)

select * from renamed
