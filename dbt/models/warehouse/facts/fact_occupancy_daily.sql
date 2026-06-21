{{
    config(
        materialized  = 'table',
        schema        = 'hotelmind_warehouse'
    )
}}

-- Fact table: daily occupancy snapshot per branch.
-- Grain: branch_id × date
--
-- Strategy: we compute occupancy from reservations (not from occupancy_snapshots)
-- because the reservation-derived calculation is more accurate for historical dates
-- (the snapshot table captures same-day state, but past dates may have
-- check-outs that moved rooms back to AVAILABLE).
--
-- For each branch × calendar date we count how many rooms had an active
-- reservation where check_in_date <= date < check_out_date.
--
-- This uses a cross-join of branches × date_spine for the last 2 years,
-- which is the canonical Kimball approach for occupancy facts.

with date_spine as (
    select date_key, date
    from {{ ref('dim_date') }}
    where date >= current_date - interval '730 days'
      and date <= current_date
),

branches as (
    select branch_key, branch_id, hotel_key
    from {{ ref('dim_branch') }}
),

-- Total active rooms per branch (static at query time)
room_counts as (
    select
        branch_id,
        count(*) as total_rooms
    from {{ ref('dim_room') }}
    where is_active = true
    group by branch_id
),

-- Active reservations for the date window
active_reservations as (
    select
        fb.branch_id,
        dd_in.date  as check_in_date,
        dd_out.date as check_out_date
    from {{ ref('fact_booking') }} fb
    join {{ ref('dim_date') }} dd_in  on fb.check_in_date_key  = dd_in.date_key
    join {{ ref('dim_date') }} dd_out on fb.check_out_date_key = dd_out.date_key
    where not fb.is_terminal
),

-- Cross-join branches × dates, then count overlapping reservations
occupancy_raw as (
    select
        b.branch_key,
        b.branch_id,
        b.hotel_key,
        ds.date_key,
        ds.date                                         as occupancy_date,
        coalesce(rc.total_rooms, 0)                     as total_rooms,
        -- Count reservations where check_in <= date < check_out (the overlap test)
        count(ar.branch_id)                             as occupied_rooms
    from branches b
    cross join date_spine ds
    left join room_counts rc    on b.branch_id = rc.branch_id
    left join active_reservations ar
        on b.branch_id      = ar.branch_id
        and ds.date        >= ar.check_in_date
        and ds.date         < ar.check_out_date
    group by b.branch_key, b.branch_id, b.hotel_key, ds.date_key, ds.date, rc.total_rooms
),

final as (
    select
        {{ generate_surrogate_key(['branch_id', 'date_key']) }}           as surrogate_key,
        branch_key,
        branch_id,
        hotel_key,
        date_key,
        occupancy_date,
        total_rooms,
        occupied_rooms,
        total_rooms - occupied_rooms                    as available_rooms,
        case
            when total_rooms > 0
            then round(occupied_rooms::numeric / total_rooms * 100, 2)
            else 0
        end                                             as occupancy_pct
    from occupancy_raw
)

select * from final
