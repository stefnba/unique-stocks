SELECT
    data_provider,
    provider_exchange_code,
    COUNT(*) AS record_count
FROM {{ ref('int_exchange_provider_ingestion_universe') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
