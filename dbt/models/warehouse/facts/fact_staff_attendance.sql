{{
    config(
        materialized  = 'incremental',
        schema        = 'hotelmind_warehouse',
        unique_key    = 'surrogate_key',
        on_schema_change = 'append_new_columns'
    )
}}

-- Fact table: one row per attendance record.
-- Grain: attendance_id
-- Measures: scheduled_hours, actual_hours, variance_hours
-- Incremental on attendance.updated_at

with attendance as (
    select * from {{ ref('stg_attendance') }}

    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

schedules as (
    select schedule_id, shift_date, scheduled_hours
    from {{ ref('stg_schedules') }}
),

employees as (
    select employee_key, employee_id, branch_key, branch_id, hotel_key, department_id, department_name, full_name, role
    from {{ ref('dim_employee') }}
),

shift_dates as (
    select date_key, date
    from {{ ref('dim_date') }}
),

final as (
    select
        {{ generate_surrogate_key(['a.attendance_id']) }}    as surrogate_key,

        -- Natural keys
        a.attendance_id,
        a.attendance_status,

        -- Foreign keys
        e.employee_key,
        e.employee_id,
        e.branch_key,
        e.branch_id,
        e.hotel_key,
        e.department_id,
        d.date_key                                           as shift_date_key,

        -- Descriptive
        e.full_name                                          as employee_name,
        e.role,
        e.department_name,

        -- Measures
        coalesce(s.scheduled_hours, 0)::numeric(5,2)         as scheduled_hours,
        coalesce(a.actual_hours, 0)::numeric(5,2)            as actual_hours,
        (coalesce(a.actual_hours, 0) - coalesce(s.scheduled_hours, 0))::numeric(5,2)
                                                             as variance_hours,

        -- Flags
        case when a.attendance_status = 'PRESENT' then true else false end  as is_present,
        case when a.attendance_status = 'LATE'    then true else false end  as is_late,

        -- Timestamps
        a.clock_in,
        a.clock_out,
        a.created_at,
        a.updated_at
    from attendance a
    join schedules s    on a.schedule_id  = s.schedule_id
    join employees e    on a.employee_id  = e.employee_id
    left join shift_dates d on s.shift_date = d.date
)

select * from final
