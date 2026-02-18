{{
  config(
    materialized='incremental',
    unique_key='transaction_ref',
    on_schema_change='sync_all_columns',
    incremental_strategy='merge',
    merge_update_columns=['has_chargeback'],
  )
}}

-- Using a CTE for the watermark instead of run_query keeps compilation fast
with watermark as (
  {% if is_incremental() %}
    select max(transaction_date_time) as max_date from {{ this }}
  {% else %}
    select cast('1900-01-01' as timestamp) as max_date
  {% endif %}
),

latest_chargebacks as (
  select
    external_ref,
    has_chargeback,
    row_number() over (partition by external_ref order by dbt_scd_id desc) as rn
  from {{ ref('snap_int_chargeback_transactions') }} snap
  where 
    {% if is_incremental() %}
      -- Capture any chargeback record updated since the last run
      snap.updated_at >= (select max_date from watermark)
    {% else %}
      snap.dbt_valid_to is null
    {% endif %}
),

payments as (
  select
    p.external_ref,
    p.status,
    p.source,
    p.transaction_ref,
    p.transaction_date_time,
    p.transaction_date,
    p.transaction_state,
    p.is_accepted,
    p.is_declined,
    p.cvv_provided,
    p.amount_local,
    p.amount_usd,
    p.country,
    p.currency
  from {{ ref('int_acceptance_transactions') }} p
  
  {% if is_incremental() %}
    left join latest_chargebacks lc on p.external_ref = lc.external_ref
    where 
      p.transaction_date_time > (select max_date from watermark)
      or lc.external_ref is not null
  {% endif %}
)

select
  p.transaction_ref,
  p.external_ref,
  p.status,
  p.source,
  p.transaction_date_time,
  p.transaction_date,
  p.transaction_state,
  p.is_accepted,
  p.is_declined,
  p.cvv_provided,
  p.amount_local,
  p.amount_usd,
  p.country,
  p.currency,
  coalesce(lc.has_chargeback, false) as has_chargeback
from payments p
left join latest_chargebacks lc 
  on p.external_ref = lc.external_ref
  and lc.rn = 1
