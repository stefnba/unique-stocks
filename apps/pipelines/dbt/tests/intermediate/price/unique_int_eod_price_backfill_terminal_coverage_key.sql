SELECT
    data_provider,
    unit_key_hash,
    status,
    COUNT(*) AS row_count
FROM {{ ref('int_eod_price_backfill_terminal_coverage') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
