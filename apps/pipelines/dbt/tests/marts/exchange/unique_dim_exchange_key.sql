SELECT
    exchange_id,
    COUNT(*) AS record_count
FROM {{ ref('dim_exchange') }}
GROUP BY 1
HAVING COUNT(*) > 1
