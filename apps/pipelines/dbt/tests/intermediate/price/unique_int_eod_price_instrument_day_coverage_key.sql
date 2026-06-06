SELECT
    data_provider,
    provider_exchange_code,
    provider_instrument_code,
    bar_date
FROM {{ ref('int_eod_price_instrument_day_coverage') }}
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
