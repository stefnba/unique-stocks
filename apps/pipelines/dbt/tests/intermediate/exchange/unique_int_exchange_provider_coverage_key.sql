SELECT
    data_provider,
    provider_exchange_code,
    COALESCE(catalog_operating_mic, '__provider_bucket__') AS catalog_operating_mic_key,
    COUNT(*) AS record_count
FROM {{ ref('int_exchange_provider_coverage') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
