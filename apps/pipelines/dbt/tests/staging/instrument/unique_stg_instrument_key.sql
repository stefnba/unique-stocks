SELECT
    snapshot_date,
    provider_exchange_code,
    provider_instrument_code,
    data_provider,
    COUNT(*) AS record_count
FROM {{ ref('stg_instrument') }}
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
