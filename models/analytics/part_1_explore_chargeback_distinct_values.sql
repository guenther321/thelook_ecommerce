-- Explore distinct values in chargeback transactions
-- 
-- RESULTS:
-- STATUS     SOURCE          CHARGEBACK     
-- ============================================================
-- True       GLOBALPAY       False          
-- True       GLOBALPAY       True
--
-- Findings:
-- - STATUS: Always TRUE (all records marked as valid/confirmed)
-- - SOURCE: Always GLOBALPAY (consistent data source)
-- - CHARGEBACK: Both TRUE and FALSE values present (indicating chargedback vs non-chargedback transactions)

select distinct 
  status,
  source,
  chargeback
from {{ source('globepay', 'globepay_chargeback_transactions') }}
