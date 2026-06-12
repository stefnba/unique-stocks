SELECT
    data_provider,
    provider_exchange_code,
    COUNT(*) AS record_count
FROM {{ ref('provider_namespace_policy') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
