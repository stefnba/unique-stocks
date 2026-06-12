SELECT
    data_provider,
    provider_exchange_code,
    bar_date,
    universe_tier,
    daily_coverage_mode,
    historical_coverage_mode,
    is_daily_coverage_blocking,
    latest_expected_bar_date,
    is_latest_expected_trading_day,
    is_blocking_coverage_gap,
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
    AND provider_exchange_code IN ({{ code_placeholders }})
    {% if from_date_filter %}
        AND bar_date >= {{ param() }}
    {% endif %}
    {% if to_date_filter %}
        AND bar_date <= {{ param() }}
    {% endif %}
    AND is_blocking_coverage_gap
ORDER BY provider_exchange_code, bar_date
