with source as (
    select * from {{ source('hotelmind_staging', 'attendance') }}
),

enriched as (
    select
        id                                                                          as attendance_id,
        schedule_id,
        employee_id,
        clock_in,
        clock_out,
        coalesce(status, 'PRESENT')                                                 as attendance_status,
        -- Actual hours worked (null if not clocked out yet)
        case
            when clock_out is not null and clock_in is not null
            then extract(epoch from (clock_out - clock_in)) / 3600.0
        end::numeric(5,2)                                                           as actual_hours,
        created_at,
        updated_at
    from source
)

select * from enriched
