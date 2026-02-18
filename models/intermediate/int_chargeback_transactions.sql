select
  external_ref,
  status,
  source,
  has_chargeback
from {{ ref('stg_globepay_chargeback_transactions') }}
