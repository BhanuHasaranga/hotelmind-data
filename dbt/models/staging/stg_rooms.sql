with source as (
    select * from {{ source('hotelmind_staging', 'rooms') }}
),

renamed as (
    select
        id                              as room_id,
        floor_id,
        room_type_id,
        room_number,
        coalesce(status, 'AVAILABLE')   as room_status,
        coalesce(is_active, true)       as is_active,
        created_at,
        updated_at
    from source
)

select * from renamed
