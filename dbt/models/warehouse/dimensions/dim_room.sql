{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

-- Rooms denormalized with room_type, branch, and aggregated amenity list.
-- Amenity names are concatenated as a pipe-delimited string for easy filtering.

with rooms as (
    select * from {{ ref('stg_rooms') }}
),

room_types as (
    select * from {{ ref('stg_room_types') }}
),

floors as (
    select id as floor_id, branch_id, floor_number
    from {{ source('hotelmind_staging', 'floors') }}
),

branches as (
    select branch_key, branch_id, hotel_key, hotel_name, branch_name
    from {{ ref('dim_branch') }}
),

-- Aggregate amenity names per room_type
amenity_agg as (
    select
        rta.room_type_id,
        string_agg(a.name, ' | ' order by a.name)  as amenity_names,
        count(*)                                     as amenity_count
    from {{ source('hotelmind_staging', 'room_type_amenities') }} rta
    join {{ source('hotelmind_staging', 'amenities') }} a
        on rta.amenity_id = a.id
    group by rta.room_type_id
),

final as (
    select
        {{ generate_surrogate_key(['r.room_id']) }}      as room_key,
        r.room_id,
        b.branch_key,
        b.branch_id,
        b.hotel_key,
        b.hotel_name,
        b.branch_name,
        f.floor_number,
        r.room_number,
        rt.room_type_id,
        rt.room_type_name,
        rt.base_price,
        rt.max_occupancy,
        rt.description                                   as room_type_description,
        coalesce(aa.amenity_names, '')                   as amenity_names,
        coalesce(aa.amenity_count, 0)                    as amenity_count,
        r.room_status                                    as current_status,
        r.is_active,
        r.created_at,
        r.updated_at
    from rooms r
    join room_types rt   on r.room_type_id   = rt.room_type_id
    join floors f        on r.floor_id       = f.floor_id
    join branches b      on f.branch_id      = b.branch_id
    left join amenity_agg aa on rt.room_type_id = aa.room_type_id
)

select * from final
