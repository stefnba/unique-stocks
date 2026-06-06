SELECT
    data_provider,
    provider_exchange_code,
    bar_date
FROM {{ ref('int_exchange_trading_day') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
