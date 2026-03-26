with source_data as (
    select
        id                              as inventory_item_id,
        product_id,
        created_at                      as inventory_item_created_at,
        sold_at                         as inventory_item_sold_at,
        cost                            as inventory_item_cost,
        product_category,
        product_name,
        product_brand,
        product_retail_price,
        product_department,
        product_sku,
        product_distribution_center_id
    from {{ source('thelook_ecommerce', 'inventory_items') }}
)

select * from source_data
