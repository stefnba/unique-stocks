SELECT
    data_provider,
    provider_exchange_code,
    bar_date
FROM {{ ref('int_eod_price_exchange_day_status') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
