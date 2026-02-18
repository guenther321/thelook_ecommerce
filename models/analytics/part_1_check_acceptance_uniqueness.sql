-- Check if EXTERNAL_REF and REF are unique
--
-- RESULTS:
-- Total Records: 5430
-- Unique External Refs: 5430
-- External Ref is Unique: True
-- Unique Refs: 5430
-- Ref is Unique: True
--
-- Finding: Both external_ref and ref are 100% unique - no duplicate transactions detected

select
  count(*) as total_records,
  count(distinct external_ref) as unique_external_refs,
  count(*) = count(distinct external_ref) as external_ref_is_unique,
  count(distinct ref) as unique_refs,
  count(*) = count(distinct ref) as ref_is_unique
from {{ source('globepay', 'globepay_acceptance_transactions') }}
