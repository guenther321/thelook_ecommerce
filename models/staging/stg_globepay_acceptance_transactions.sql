with source_data as (
  select
    external_ref,
    status,
    source,
    ref as transaction_ref,
    date_time as transaction_date_time,
    state as transaction_state,
    cvv_provided,
    amount as amount_local,
    country,
    currency,
    rates,
    cast(date_time as date) as transaction_date
  from {{ source('globepay', 'globepay_acceptance_transactions') }}
),

enriched as (
  select
    *,
    case when transaction_state = 'ACCEPTED' then 1 else 0 end as is_accepted,
    case when transaction_state = 'DECLINED' then 1 else 0 end as is_declined
  from source_data
)

select * from enriched
