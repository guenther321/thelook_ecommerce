with order_items as (
    select * from {{ ref('stg_order_items') }}
    where 1=1
        {{ get_incremental_filter(
            source_partition_date='date(order_item_created_at)',
            target_partition_date='date(order_item_created_at)',
            floor_interval='2 year'
        ) }}
),

products as (
    select * from {{ ref('stg_products') }}
),

inventory_items as (
    select * from {{ ref('stg_inventory_items') }} 
),

distribution_centers as (
    select * from {{ ref('stg_distribution_centers') }}
),

joined as (
    select
        order_items.order_item_id,
        order_items.order_id,
        order_items.user_id,
        order_items.product_id,
        order_items.inventory_item_id,
        order_items.order_item_status,
        order_items.order_item_created_at,
        order_items.order_item_shipped_at,
        order_items.order_item_delivered_at,
        order_items.order_item_returned_at,
        order_items.sale_price,

        -- product details
        products.product_name,
        products.product_category,
        products.product_brand,
        products.product_department,
        products.product_sku,
        products.product_cost,
        products.product_retail_price,

        -- distribution center
        distribution_centers.distribution_center_name,
        distribution_centers.latitude as distribution_center_latitude,
        distribution_centers.longitude as distribution_center_longitude,

        -- inventory timing
        inventory_items.inventory_item_created_at,
        inventory_items.inventory_item_sold_at,

        -- calculated fields
        order_items.sale_price - products.product_cost as gross_margin,
        order_items.order_item_returned_at is not null as is_returned,

        timestamp_diff(
            inventory_items.inventory_item_sold_at,
            inventory_items.inventory_item_created_at,
            day
        ) as inventory_holding_days,

        timestamp_diff(
            order_items.order_item_shipped_at,
            order_items.order_item_created_at,
            day
        ) as days_to_ship,

        timestamp_diff(
            order_items.order_item_delivered_at,
            order_items.order_item_shipped_at,
            day
        ) as days_to_deliver

    from order_items
    left join products
        on order_items.product_id = products.product_id
    left join inventory_items
        on order_items.inventory_item_id = inventory_items.inventory_item_id
    left join distribution_centers
        on products.distribution_center_id = distribution_centers.distribution_center_id
)

select * from joined
