SELECT
    data_provider,
    provider_exchange_code,
    bar_date,
    exchange_day_status,
    expected_instruments,
    priced_instruments,
    missing_price_instruments,
    known_no_data_instruments,
    unknown_calendar_instruments,
    unknown_calendar_coverage_instruments,
    unknown_instrument_lifecycle_instruments
FROM {{ status_relation }}
WHERE data_provider = {{ param() }}
    AND ({{ pair_predicates }})
    AND exchange_day_status IN (
        'missing_price',
        'unknown_calendar',
        'unknown_calendar_coverage',
        'unknown_instrument_lifecycle'
    )
ORDER BY provider_exchange_code, bar_date
