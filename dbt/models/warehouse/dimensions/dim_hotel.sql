{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

with hotels as (
    select * from {{ ref('stg_hotels') }}
),

final as (
    select
        -- Surrogate key (stable hash of the natural key)
        {{ generate_surrogate_key(['hotel_id']) }}   as hotel_key,
        hotel_id,
        hotel_name,
        star_rating,
        address,
        city,
        country,
        phone,
        contact_email,
        is_active,
        created_at,
        updated_at
    from hotels
)

select * from final
