-- Question 1: Acceptance Rate Over Time
-- Shows acceptance rate by day (last 30 days) with transaction counts and percentages
--
-- RESULTS SUMMARY:
-- Date Range: June 1-30, 2019
-- Acceptance Rate Range: 60-86.67%
-- Average Acceptance Rate: ~71%
-- Daily Trend: Consistent variations between 60-87%, no clear upward/downward trend
-- Key Finding: Fairly stable acceptance rates across all days with daily fluctuations

select
  transaction_date,
  count(*) as total_transactions,
  sum(case when is_accepted = true then 1 else 0 end) as accepted_transactions,
  sum(case when is_declined = true then 1 else 0 end) as declined_transactions,
  round(100.0 * sum(case when is_accepted = true then 1 else 0 end) / count(*), 2) as acceptance_rate_percent
from {{ ref('payments') }}
group by transaction_date
order by transaction_date desc
limit 30