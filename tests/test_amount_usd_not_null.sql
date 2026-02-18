-- Test that amount_usd is not null (catches missing/zero exchange rates)

select *
from {{ ref('int_acceptance_transactions') }}
where amount_usd is null
