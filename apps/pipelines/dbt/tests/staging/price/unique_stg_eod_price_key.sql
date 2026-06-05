SELECT
    provider_exchange_code,
    provider_instrument_code,
    bar_date,
    data_provider,
    COUNT(*) AS record_count
FROM {{ ref('stg_eod_price') }}
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
