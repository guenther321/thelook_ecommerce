with source_data as (
  select
    external_ref,
    status,
    source,
    case 
      when chargeback = 'TRUE' then true
      when chargeback = 'FALSE' then false
    end as has_chargeback
  from {{ source('globepay', 'globepay_chargeback_transactions') }}
)

select * from source_data
