{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

with guests as (
    select * from {{ ref('stg_guests') }}
),

-- Enrich with lifetime booking statistics for ML feature engineering
booking_stats as (
    select
        guest_id,
        count(*)                        as lifetime_bookings,
        sum(total_amount)               as lifetime_spend,
        min(check_in_date)              as first_stay_date,
        max(check_in_date)              as last_stay_date
    from {{ ref('stg_reservations') }}
    where not is_terminal
    group by guest_id
),

final as (
    select
        {{ generate_surrogate_key(['g.guest_id']) }}     as guest_key,
        g.guest_id,
        g.first_name,
        g.last_name,
        g.full_name,
        g.email,
        g.phone,
        g.id_type,
        g.nationality,
        -- Lifetime value metrics (updated on each dbt run)
        coalesce(bs.lifetime_bookings, 0)               as lifetime_bookings,
        coalesce(bs.lifetime_spend, 0)::numeric(12,2)   as lifetime_spend,
        bs.first_stay_date,
        bs.last_stay_date,
        g.created_at,
        g.updated_at
    from guests g
    left join booking_stats bs on g.guest_id = bs.guest_id
)

select * from final
