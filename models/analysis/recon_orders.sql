-- Reconciliation: orders mart — old pipeline vs new pipeline
--
-- Pattern: FULL JOIN on the business key, then compare metrics side by side.
-- A row appears in the diff CTE only when something actually changed.
--
-- How to use:
--   1. Run old pipeline → results land in a snapshot or a versioned dataset
--   2. Run new pipeline → results land in the current mart
--   3. Run this model — any non-empty output = discrepancy to investigate
--
-- In this project we use snap_int_chargeback_transactions as the "old"
-- reference and the live mart as "new". In a real migration you'd point
-- old_ at the legacy table and new_ at the rebuilt mart.

with old_pipeline as (

    -- In a real migration this points at the legacy table in a separate dataset,
    -- e.g. `legacy_dataset.orders_v1`. Here we self-join the same mart to
    -- show the pattern — swap this source when you have a real legacy table.
    select
        order_id,
        order_status,
        order_total_revenue,
        number_of_items
    from {{ ref('orders') }}   -- replace with legacy source in a real migration

),

new_pipeline as (

    select
        order_id,
        order_status,
        order_total_revenue,
        number_of_items
    from {{ ref('orders') }}

),

-- ── Level 1: aggregate summary ─────────────────────────────────────────────
summary as (

    select
        'old' as pipeline,
        count(*)                        as row_count,
        count(distinct order_id)        as distinct_orders,
        round(sum(order_total_revenue), 2) as total_revenue
    from old_pipeline

    union all

    select
        'new' as pipeline,
        count(*)                        as row_count,
        count(distinct order_id)        as distinct_orders,
        round(sum(order_total_revenue), 2) as total_revenue
    from new_pipeline

),

-- ── Level 2: key-level diff — only rows that differ ───────────────────────
diff as (

    select
        coalesce(o.order_id, n.order_id)            as order_id,

        o.order_status                               as old_status,
        n.order_status                               as new_status,

        o.order_total_revenue                        as old_revenue,
        n.order_total_revenue                        as new_revenue,
        round(
            coalesce(n.order_total_revenue, 0)
            - coalesce(o.order_total_revenue, 0),
        2)                                           as revenue_delta,

        o.number_of_items                            as old_items,
        n.number_of_items                            as new_items,

        case
            when o.order_id is null then 'new_only'   -- exists in new, missing from old
            when n.order_id is null then 'old_only'   -- exists in old, missing from new
            when o.order_total_revenue != n.order_total_revenue then 'revenue_mismatch'
            when o.order_status        != n.order_status        then 'status_mismatch'
            else 'match'
        end                                          as reconciliation_status

    from old_pipeline o
    full join new_pipeline n using (order_id)

)

-- Swap between these two selects depending on what you want to see:

-- Option A — aggregate summary (run this first)
-- select * from summary

-- Option B — key-level diff, only mismatches (run this to investigate)
select *
from diff
where reconciliation_status != 'match'
order by reconciliation_status, abs(revenue_delta) desc
