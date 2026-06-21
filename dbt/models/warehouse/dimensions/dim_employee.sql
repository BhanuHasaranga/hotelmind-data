{{
    config(
        materialized = 'table',
        schema       = 'hotelmind_warehouse'
    )
}}

with employees as (
    select * from {{ ref('stg_employees') }}
),

departments as (
    select department_id, department_name, branch_id
    from {{ ref('stg_departments') }}
),

branches as (
    select branch_key, branch_id, hotel_key, hotel_name, branch_name
    from {{ ref('dim_branch') }}
),

final as (
    select
        {{ generate_surrogate_key(['e.employee_id']) }}  as employee_key,
        e.employee_id,
        b.branch_key,
        b.branch_id,
        b.hotel_key,
        b.hotel_name,
        b.branch_name,
        e.department_id,
        d.department_name,
        e.first_name,
        e.last_name,
        e.full_name,
        e.email,
        e.phone,
        e.role,
        e.hire_date,
        -- Tenure in years (approximate)
        extract(year from age(current_date, e.hire_date))::integer  as tenure_years,
        e.is_active,
        e.created_at,
        e.updated_at
    from employees e
    join departments d  on e.department_id = d.department_id
    join branches b     on d.branch_id     = b.branch_id
)

select * from final
