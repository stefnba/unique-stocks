WITH instrument_universe AS (
    SELECT provider_instrument_code
    FROM {{ instrument_universe_relation }}
    WHERE data_provider = {{ param() }}
        AND provider_exchange_code = {{ param() }}
        AND is_tradable
),

exchange_scope AS (
    SELECT COUNT(*) AS expected_exchange_days
    FROM {{ trading_day_relation }}
    WHERE data_provider = {{ param() }}
        AND provider_exchange_code = {{ param() }}
        AND (is_trading_day OR NOT is_calendar_known)
        AND bar_date <= {{ param() }}
        AND ({{ param() }} IS NULL OR bar_date >= {{ param() }})
),

instrument_day_coverage AS (
    SELECT
        provider_instrument_code,
        COUNT(*) AS expected_instrument_days,
        SUM(CASE WHEN coverage_status = 'missing_price' THEN 1 ELSE 0 END) AS missing_price_days,
        SUM(CASE WHEN coverage_status = 'unknown_calendar' THEN 1 ELSE 0 END) AS unknown_calendar_days,
        MAX(CASE WHEN min_bar_date IS NOT NULL THEN 1 ELSE 0 END) AS has_observed_price_range
    FROM {{ instrument_day_coverage_relation }}
    WHERE data_provider = {{ param() }}
        AND provider_exchange_code = {{ param() }}
        AND bar_date <= {{ param() }}
        AND ({{ param() }} IS NULL OR bar_date >= {{ param() }})
    GROUP BY 1
),

terminal_coverage AS (
    SELECT
        coverage.data_provider,
        coverage.provider_exchange_code,
        coverage.provider_instrument_code,
        BOOL_OR(coverage.status = 'completed') AS has_completed_coverage,
        BOOL_OR(coverage.status = 'no_data') AS has_no_data_coverage
    FROM {{ terminal_coverage_relation }} AS coverage
    WHERE coverage.data_provider = {{ param() }}
        AND coverage.provider_exchange_code = {{ param() }}
        AND (({{ param() }} IS NULL AND coverage.from_date IS NULL) OR coverage.from_date = {{ param() }})
        AND coverage.to_date = {{ param() }}
    GROUP BY 1, 2, 3
),

scoped_symbols AS (
    SELECT
        instrument_universe.provider_instrument_code,
        COALESCE(terminal_coverage.has_completed_coverage, FALSE) AS has_completed_coverage,
        COALESCE(terminal_coverage.has_no_data_coverage, FALSE) AS has_no_data_coverage,
        COALESCE(instrument_day_coverage.expected_instrument_days, 0) AS expected_instrument_days,
        COALESCE(instrument_day_coverage.missing_price_days, 0) AS missing_price_days,
        COALESCE(instrument_day_coverage.unknown_calendar_days, 0) AS unknown_calendar_days,
        COALESCE(instrument_day_coverage.has_observed_price_range, 0) > 0 AS has_observed_price_range,
        exchange_scope.expected_exchange_days
    FROM instrument_universe
    CROSS JOIN exchange_scope
    LEFT JOIN instrument_day_coverage
        ON instrument_universe.provider_instrument_code = instrument_day_coverage.provider_instrument_code
    LEFT JOIN terminal_coverage
        ON instrument_universe.provider_instrument_code = terminal_coverage.provider_instrument_code
)

SELECT
    provider_instrument_code,
    expected_instrument_days,
    missing_price_days,
    unknown_calendar_days,
    COUNT(*) OVER () AS total_instruments,
    SUM(CASE WHEN has_completed_coverage THEN 1 ELSE 0 END) OVER () AS completed_coverage_instruments,
    SUM(CASE WHEN has_no_data_coverage THEN 1 ELSE 0 END) OVER () AS terminal_no_data_instruments
FROM scoped_symbols
WHERE expected_exchange_days > 0
    AND NOT has_completed_coverage
    AND NOT has_no_data_coverage
    AND (
        {{ param() }} IS NULL
        OR missing_price_days > 0
        OR expected_instrument_days = 0
        OR NOT has_observed_price_range
    )
ORDER BY provider_instrument_code
