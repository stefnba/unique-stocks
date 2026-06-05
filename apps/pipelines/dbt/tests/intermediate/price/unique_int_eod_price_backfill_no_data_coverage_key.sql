SELECT
    data_provider,
    provider_exchange_code,
    provider_symbol,
    from_date,
    to_date
FROM {{ ref('int_eod_price_backfill_no_data_coverage') }}
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(*) > 1
