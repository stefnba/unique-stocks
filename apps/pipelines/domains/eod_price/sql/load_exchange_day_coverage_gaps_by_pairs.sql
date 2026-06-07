SELECT
    data_provider,
    provider_exchange_code,
    bar_date,
    exchange_day_status,
    expected_instruments,
    priced_instruments,
    missing_price_instruments,
    known_no_data_instruments,
    unknown_calendar_instruments
FROM {{ status_relation }}
WHERE data_provider = {{ param() }}
    AND ({{ pair_predicates }})
    AND exchange_day_status IN ('missing_price', 'unknown_calendar')
ORDER BY provider_exchange_code, bar_date
