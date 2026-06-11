SELECT
    provider_exchange_code,
    MAX(bar_date) AS latest_expected_bar_date
FROM {{ trading_day_relation }}
WHERE data_provider = {{ param() }}
    AND provider_exchange_code IN ({{ code_placeholders }})
    AND bar_date <= {{ param() }}
    AND (is_trading_day OR NOT is_calendar_known)
GROUP BY 1
