SELECT
    data_provider,
    mic,
    COUNT(*) AS record_count
FROM {{ ref('stg_exchange_mic_registry') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
