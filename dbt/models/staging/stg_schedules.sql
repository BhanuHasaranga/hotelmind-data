with source as (
    select * from {{ source('hotelmind_staging', 'schedules') }}
),

renamed as (
    select
        id                                                                  as schedule_id,
        employee_id,
        shift_date,
        shift_start,
        shift_end,
        -- Scheduled duration in hours (handles midnight-crossing shifts)
        case
            when shift_end::time < shift_start::time
            then extract(epoch from (shift_end::time - shift_start::time + interval '24 hours')) / 3600.0
            else extract(epoch from (shift_end::time - shift_start::time)) / 3600.0
        end                                                                 as scheduled_hours,
        notes,
        created_at,
        updated_at
    from source
)

select * from renamed
