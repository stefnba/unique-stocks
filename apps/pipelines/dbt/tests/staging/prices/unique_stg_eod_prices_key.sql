SELECT
    ticker,
    bar_date,
    provider,
    COUNT(*) AS record_count
FROM {{ ref('stg_eod_prices') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
