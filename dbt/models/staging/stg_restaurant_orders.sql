with source as (
    select * from {{ source('hotelmind_staging', 'restaurant_orders') }}
),

renamed as (
    select
        id                                          as order_id,
        branch_id,
        table_id,
        status                                      as order_status,
        opened_at,
        closed_at,
        -- Duration in minutes when closed
        case
            when closed_at is not null
            then extract(epoch from (closed_at - opened_at)) / 60.0
        end::numeric(10,2)                          as duration_minutes,
        coalesce(total_amount, 0)::numeric(10,2)    as total_amount,
        created_at,
        updated_at
    from source
)

select * from renamed
