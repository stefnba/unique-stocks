SELECT
    data_provider,
    provider_exchange_code,
    COUNT(*) AS record_count
FROM {{ ref('eodhd_provider_namespaces') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
