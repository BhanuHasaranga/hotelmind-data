{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

with branches as (
    select * from {{ ref('stg_branches') }}
),

hotels as (
    select hotel_key, hotel_id, hotel_name, city as hotel_city, country
    from {{ ref('dim_hotel') }}
),

final as (
    select
        {{ generate_surrogate_key(['b.branch_id']) }}    as branch_key,
        b.branch_id,
        h.hotel_key,
        h.hotel_id,
        h.hotel_name,
        b.branch_name,
        coalesce(b.city, h.hotel_city)                  as city,
        h.country,
        b.address,
        b.phone,
        b.is_main_branch,
        b.created_at,
        b.updated_at
    from branches b
    left join hotels h on b.hotel_id = h.hotel_id
)

select * from final
