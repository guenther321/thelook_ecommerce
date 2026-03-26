with sessions as (
    select * from {{ ref('int_user_sessions') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

-- attribute orders to sessions: order must fall within session window + 5 min buffer
-- (order_created_at is typically seconds after the last purchase event)
session_orders as (
    select
        sessions.session_id,
        orders.order_id,
        orders.order_status,
        orders.order_created_at,
        orders.number_of_items,
        orders.order_total_revenue,
        orders.order_total_cost,
        orders.order_total_margin
    from sessions
    inner join orders
        on sessions.user_id = orders.user_id
        and orders.order_created_at between sessions.session_started_at
            and timestamp_add(sessions.session_ended_at, interval 5 minute)
),

session_order_metrics as (
    select
        session_id,
        count(distinct order_id) as orders_in_session,
        sum(number_of_items) as items_in_session,
        sum(order_total_revenue) as session_revenue,
        sum(order_total_cost) as session_cost,
        sum(order_total_margin) as session_margin
    from session_orders
    group by session_id
)

select
    sessions.session_id,
    sessions.user_id,
    sessions.session_started_at,
    sessions.session_ended_at,
    sessions.session_duration_seconds,
    sessions.number_of_events,
    sessions.entry_uri,
    sessions.exit_uri,
    sessions.browser,
    sessions.traffic_source,
    sessions.city,
    sessions.state,

    -- attributed booking metrics
    coalesce(session_order_metrics.orders_in_session, 0) as orders_in_session,
    coalesce(session_order_metrics.items_in_session, 0) as items_in_session,
    coalesce(session_order_metrics.session_revenue, 0) as session_revenue,
    coalesce(session_order_metrics.session_cost, 0) as session_cost,
    coalesce(session_order_metrics.session_margin, 0) as session_margin,
    session_order_metrics.orders_in_session > 0 as has_converted

from sessions
left join session_order_metrics
    on sessions.session_id = session_order_metrics.session_id
