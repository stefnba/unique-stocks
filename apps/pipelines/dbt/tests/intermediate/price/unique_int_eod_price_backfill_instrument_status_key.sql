SELECT
    data_provider,
    provider_exchange_code,
    provider_instrument_code
FROM {{ ref('int_eod_price_backfill_instrument_status') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
