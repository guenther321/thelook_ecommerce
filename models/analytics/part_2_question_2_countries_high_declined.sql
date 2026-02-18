-- Question 2: Countries with High Declined Transaction Amounts
-- Lists countries where declined transactions exceeded $25M
--
-- RESULTS SUMMARY:
-- Total Countries Exceeding $25M: 4
-- 1. France (FR): $32.6M declined (271 transactions, avg $120k per transaction)
-- 2. United Kingdom (UK): $27.5M declined (258 transactions, avg $106k per transaction)
-- 3. United Arab Emirates (AE): $26.3M declined (291 transactions, avg $90k per transaction)
-- 4. United States (US): $25.1M declined (297 transactions, avg $84k per transaction)
-- Key Finding: France has highest absolute declined amount, US has highest transaction count

select
  country,
  count(*) as declined_transaction_count,
  sum(amount_usd) as total_declined_amount_usd,
  round(sum(amount_usd) / count(*), 2) as avg_declined_amount_usd
from {{ ref('payments') }}
where is_declined = true
group by country
having sum(amount_usd) > 25000000
order by total_declined_amount_usd desc
