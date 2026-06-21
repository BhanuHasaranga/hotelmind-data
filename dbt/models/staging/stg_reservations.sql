with source as (
    select * from {{ source('hotelmind_staging', 'reservations') }}
),

enriched as (
    select
        id                                              as reservation_id,
        room_id,
        guest_id,
        check_in_date,
        check_out_date,
        -- Computed derived fields
        (check_out_date - check_in_date)::integer       as nights,
        status                                          as reservation_status,
        coalesce(adults, 1)                             as adults,
        coalesce(children, 0)                           as children,
        coalesce(adults, 1) + coalesce(children, 0)     as total_guests,
        total_amount::numeric(10,2)                     as total_amount,
        coalesce(paid_amount, 0)::numeric(10,2)         as paid_amount,
        (total_amount - coalesce(paid_amount, 0))::numeric(10,2) as outstanding_amount,
        special_requests,
        cancelled_at,
        cancellation_reason,
        -- Flags derived from status
        case when status in ('CANCELLED', 'NO_SHOW') then true else false end  as is_terminal,
        case when status = 'CHECKED_OUT'             then true else false end  as is_completed,
        created_at,
        updated_at
    from source
)

select * from enriched
