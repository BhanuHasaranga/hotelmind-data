{{
    config(
        materialized  = 'incremental',
        schema        = 'hotelmind_warehouse',
        unique_key    = 'surrogate_key',
        on_schema_change = 'append_new_columns'
    )
}}

-- Fact table: one row per reservation.
-- Grain: reservation_id
-- Measures: total_amount, paid_amount, nights, adults, children, outstanding_amount
-- Incremental: only processes reservations updated since last run.

with reservations as (
    select * from {{ ref('stg_reservations') }}

    {% if is_incremental() %}
        -- Only process rows updated since last run
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

rooms as (
    select room_key, room_id, branch_key, branch_id, hotel_key, room_type_name, base_price
    from {{ ref('dim_room') }}
),

guests as (
    select guest_key, guest_id
    from {{ ref('dim_guest') }}
),

check_in_dates as (
    select date_key, date
    from {{ ref('dim_date') }}
),

check_out_dates as (
    select date_key, date
    from {{ ref('dim_date') }}
),

final as (
    select
        -- Surrogate key: stable hash of the reservation UUID
        {{ generate_surrogate_key(['r.reservation_id']) }}  as surrogate_key,

        -- Natural keys (degenerate dimensions)
        r.reservation_id,
        r.reservation_status,

        -- Foreign keys to dimensions
        rm.room_key,
        rm.branch_key,
        rm.branch_id,
        rm.hotel_key,
        g.guest_key,
        ci.date_key                                         as check_in_date_key,
        co.date_key                                         as check_out_date_key,

        -- Descriptive attributes
        rm.room_type_name,
        rm.base_price                                       as room_base_price,

        -- Measures
        r.nights,
        r.adults,
        r.children,
        r.total_guests,
        r.total_amount,
        r.paid_amount,
        r.outstanding_amount,

        -- Computed measures
        case
            when r.nights > 0
            then r.total_amount / r.nights
            else r.total_amount
        end::numeric(10,2)                                  as avg_daily_rate,

        -- Flags
        r.is_terminal,
        r.is_completed,

        -- Timestamps
        r.created_at,
        r.updated_at
    from reservations r
    join rooms rm   on r.room_id  = rm.room_id
    join guests g   on r.guest_id = g.guest_id
    join check_in_dates  ci on r.check_in_date  = ci.date
    join check_out_dates co on r.check_out_date = co.date
)

select * from final
