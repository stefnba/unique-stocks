SELECT
    snapshot_date,
    provider_schedule_exchange_code,
    holiday_date,
    data_provider,
    COUNT(*) AS record_count
FROM {{ ref('stg_exchange_holiday') }}
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
