with source as (
    select * from {{ source('hotelmind_staging', 'guests') }}
),

renamed as (
    select
        id                                          as guest_id,
        first_name,
        last_name,
        first_name || ' ' || last_name              as full_name,
        lower(email)                                as email,
        phone,
        id_type,
        id_number,
        nationality,
        created_at,
        updated_at
    from source
)

select * from renamed
