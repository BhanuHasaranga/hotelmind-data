with source as (
    select * from {{ source('hotelmind_staging', 'departments') }}
),

renamed as (
    select
        id          as department_id,
        branch_id,
        name        as department_name,
        created_at,
        updated_at
    from source
)

select * from renamed
