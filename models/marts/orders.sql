select
    order_id,
    order_status,
    order_created_at,
    order_shipped_at,
    order_delivered_at,
    order_returned_at,

    -- customer (no PII)
    user_id,
    country,
    city,
    state,
    user_traffic_source,

    -- order metrics
    order_total_revenue,
    order_total_cost,
    order_total_margin,
    number_of_items,
    avg_days_to_ship,
    avg_days_to_deliver,
    has_return

from {{ ref('int_orders_enriched') }}
