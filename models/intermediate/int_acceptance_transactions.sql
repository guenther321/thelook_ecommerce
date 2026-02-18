select
  transaction_ref,
  external_ref,
  status,
  source,
  transaction_date_time,
  transaction_state,
  cvv_provided,
  amount_local,
  -- Calculate USD amount using exchange rate from rates JSON
  -- Divide by rate to convert from local currency to USD
  -- Example: 1509.12 MXN / 25.099848 = 60.12 USD
  round(
    case
      when currency = 'USD' then amount_local
      else amount_local / cast(
        get(parse_json(rates), currency) as number(38, 6)
      )
    end,
    2
  ) as amount_usd,
  country,
  currency,
  transaction_date,
  is_accepted,
  is_declined
from {{ ref('stg_globepay_acceptance_transactions') }}
