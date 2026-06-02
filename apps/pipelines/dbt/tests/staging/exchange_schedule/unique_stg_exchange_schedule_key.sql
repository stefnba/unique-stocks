SELECT
    snapshot_date,
    provider_schedule_exchange_code,
    data_provider,
    COUNT(*) AS record_count
FROM {{ ref('stg_exchange_schedule') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
