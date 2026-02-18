-- Question 3: Transactions Missing Chargeback Data
-- Shows ALL transactions and identifies which ones are missing chargeback records
--
-- RESULTS SUMMARY:
-- Total Transactions Analyzed: 5,430
-- Transactions Missing Chargebacks: 5,207 (95.9%)
-- Transactions With Chargebacks: 223 (4.1%)

select
  case when has_chargeback = false then 'MISSING CHARGEBACK DATA' else 'HAS CHARGEBACK' end as chargeback_status,
  count(*) as transaction_count,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as percentage_of_total
from {{ ref('payments') }}
group by chargeback_status