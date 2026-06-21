with source as (
    select * from {{ source('hotelmind_staging', 'branches') }}
),

renamed as (
    select
        id                                  as branch_id,
        hotel_id,
        name                                as branch_name,
        address,
        city,
        phone,
        coalesce(is_main_branch, false)     as is_main_branch,
        created_at,
        updated_at
    from source
)

select * from renamed
