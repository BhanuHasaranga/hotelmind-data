{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

-- Mart: Daily staff attendance metrics per branch × department.
-- Grain: branch_id × department_id × date
-- Input for Phase 4 ML: staff optimization model.

with attendance as (
    select
        employee_id,
        branch_id,
        hotel_key,
        branch_key,
        department_id,
        department_name,
        shift_date_key,
        scheduled_hours,
        actual_hours,
        variance_hours,
        is_present,
        is_late,
        attendance_status
    from {{ ref('fact_staff_attendance') }}
),

dates as (
    select date_key, date, year, month, quarter, week_of_year, is_weekend, day_of_week
    from {{ ref('dim_date') }}
),

branches as (
    select branch_key, branch_id, hotel_key, branch_name, hotel_name
    from {{ ref('dim_branch') }}
),

-- Total headcount per branch × department (for denominator in attendance rate)
headcount as (
    select
        branch_id,
        department_id,
        count(*) as total_employees
    from {{ ref('dim_employee') }}
    where is_active = true
    group by branch_id, department_id
),

daily_dept_agg as (
    select
        a.branch_id,
        a.hotel_key,
        a.department_id,
        a.department_name,
        a.shift_date_key,
        count(*)                                            as scheduled_employees,
        sum(case when a.is_present then 1 else 0 end)      as present_employees,
        sum(case when a.is_late    then 1 else 0 end)      as late_employees,
        sum(case when a.attendance_status = 'ABSENT'
                 then 1 else 0 end)                        as absent_employees,
        sum(a.scheduled_hours)::numeric(10,2)              as total_scheduled_hours,
        sum(a.actual_hours)::numeric(10,2)                 as total_actual_hours,
        avg(a.actual_hours)::numeric(5,2)                  as avg_hours_per_employee
    from attendance a
    group by a.branch_id, a.hotel_key, a.department_id, a.department_name, a.shift_date_key
),

final as (
    select
        b.branch_name,
        b.hotel_name,
        d.date,
        d.year,
        d.month,
        d.quarter,
        d.week_of_year,
        d.is_weekend,
        d.day_of_week,
        da.*,
        coalesce(hc.total_employees, da.scheduled_employees)   as total_active_employees,
        -- Attendance rate %
        case
            when coalesce(hc.total_employees, da.scheduled_employees) > 0
            then round(
                da.present_employees::numeric
                / coalesce(hc.total_employees, da.scheduled_employees) * 100, 2
            )
            else 0
        end                                                 as attendance_rate_pct,
        -- Hours utilisation %
        case
            when da.total_scheduled_hours > 0
            then round(da.total_actual_hours / da.total_scheduled_hours * 100, 2)
            else 0
        end                                                 as hours_utilisation_pct
    from daily_dept_agg da
    join branches b  on da.branch_id      = b.branch_id
    join dates d     on da.shift_date_key = d.date_key
    left join headcount hc
        on da.branch_id     = hc.branch_id
        and da.department_id = hc.department_id
)

select * from final
