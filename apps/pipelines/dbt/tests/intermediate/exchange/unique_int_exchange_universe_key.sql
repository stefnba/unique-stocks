SELECT
    mic,
    COUNT(*) AS record_count
FROM {{ ref('int_exchange_universe') }}
GROUP BY 1
HAVING COUNT(*) > 1
