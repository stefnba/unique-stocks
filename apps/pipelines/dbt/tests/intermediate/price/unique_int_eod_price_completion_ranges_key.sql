SELECT
    data_provider,
    provider_exchange_code,
    provider_instrument_code
FROM {{ ref('int_eod_price_completion_ranges') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
