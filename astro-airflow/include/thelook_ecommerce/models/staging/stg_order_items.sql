with source_data as (
    select
        id                  as order_item_id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,
        status              as order_item_status,
        created_at          as order_item_created_at,
        shipped_at          as order_item_shipped_at,
        delivered_at        as order_item_delivered_at,
        returned_at         as order_item_returned_at,
        sale_price
    from {{ source('thelook_ecommerce', 'order_items') }}
)

select * from source_data
