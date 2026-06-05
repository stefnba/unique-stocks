SELECT
    data_provider,
    provider_exchange_code,
    provider_symbol
FROM {{ ref('int_fundamental_ingestion_universe') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
