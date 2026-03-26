with orders as (
    select * from {{ ref('stg_orders') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

order_items_agg as (
    select
        order_id,
        sum(sale_price) as order_total_revenue,
        sum(product_cost) as order_total_cost,
        sum(gross_margin) as order_total_margin,
        count(*) as number_of_items,
        avg(days_to_ship) as avg_days_to_ship,
        avg(days_to_deliver) as avg_days_to_deliver,
        logical_or(is_returned) as has_return
    from {{ ref('int_order_items_enriched') }}
    group by order_id
)

select
    orders.order_id,
    orders.order_status,
    orders.order_created_at,
    orders.order_shipped_at,
    orders.order_delivered_at,
    orders.order_returned_at,

    -- customer info
    users.user_id,
    users.first_name,
    users.last_name,
    users.email,
    users.country,
    users.city,
    users.state,
    users.traffic_source as user_traffic_source,

    -- order metrics
    order_items_agg.order_total_revenue,
    order_items_agg.order_total_cost,
    order_items_agg.order_total_margin,
    order_items_agg.number_of_items,
    order_items_agg.avg_days_to_ship,
    order_items_agg.avg_days_to_deliver,
    order_items_agg.has_return

from orders
left join users
    on orders.user_id = users.user_id
left join order_items_agg
    on orders.order_id = order_items_agg.order_id
