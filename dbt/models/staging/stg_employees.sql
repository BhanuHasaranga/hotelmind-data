with source as (
    select * from {{ source('hotelmind_staging', 'employees') }}
),

renamed as (
    select
        id                                              as employee_id,
        department_id,
        first_name,
        last_name,
        first_name || ' ' || last_name                  as full_name,
        lower(email)                                    as email,
        phone,
        coalesce(role, 'Unknown')                       as role,
        hire_date,
        coalesce(is_active, true)                       as is_active,
        created_at,
        updated_at
    from source
)

select * from renamed
