with source_data as (
    select
        order_id,
        user_id,
        status          as order_status,
        gender,
        created_at      as order_created_at,
        returned_at     as order_returned_at,
        shipped_at      as order_shipped_at,
        delivered_at    as order_delivered_at,
        num_of_item     as number_of_items
    from {{ source('thelook_ecommerce', 'orders') }}
)

select * from source_data
