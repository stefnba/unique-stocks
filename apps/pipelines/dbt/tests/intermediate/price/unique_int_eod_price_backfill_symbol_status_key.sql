SELECT
    data_provider,
    provider_exchange_code,
    provider_symbol
FROM {{ ref('int_eod_price_backfill_symbol_status') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
