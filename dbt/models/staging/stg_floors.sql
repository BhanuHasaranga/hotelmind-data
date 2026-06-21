with source as (
    select * from {{ source('hotelmind_staging', 'floors') }}
),

renamed as (
    select
        id              as floor_id,
        branch_id,
        floor_number,
        name            as floor_name,
        created_at,
        updated_at
    from source
)

select * from renamed
