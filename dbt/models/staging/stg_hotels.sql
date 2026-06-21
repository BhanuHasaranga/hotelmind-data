with source as (
    select * from {{ source('hotelmind_staging', 'hotels') }}
),

renamed as (
    select
        id                              as hotel_id,
        name                            as hotel_name,
        coalesce(star_rating, 3)        as star_rating,
        address,
        city,
        country,
        phone,
        email                           as contact_email,
        coalesce(is_active, true)       as is_active,
        created_at,
        updated_at
    from source
)

select * from renamed
