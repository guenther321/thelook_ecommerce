-- Check if EXTERNAL_REF is unique in chargeback transactions
--
-- RESULTS:
-- Total Records: 5430
-- Unique External Refs: 5430
-- External Ref is Unique: True
--
-- Finding: 100% unique - each chargeback maps to exactly one transaction (1:1 relationship)

select
  count(*) as total_records,
  count(distinct external_ref) as unique_external_refs,
  count(*) = count(distinct external_ref) as external_ref_is_unique
from {{ source('globepay', 'globepay_chargeback_transactions') }}
