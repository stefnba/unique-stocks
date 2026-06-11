SELECT *
FROM {{ ref('int_eod_price_exchange_day_status') }}
WHERE (exchange_day_status = 'unknown_calendar' AND is_calendar_known)
    OR (exchange_day_status = 'closed_exchange' AND is_trading_day)
    OR (exchange_day_status = 'no_expected_instruments' AND expected_instruments != 0)
    OR (exchange_day_status = 'unknown_calendar_coverage' AND unknown_calendar_coverage_instruments = 0)
    OR (exchange_day_status = 'unknown_instrument_lifecycle' AND unknown_instrument_lifecycle_instruments = 0)
    OR (exchange_day_status = 'missing_price' AND missing_price_instruments = 0)
    OR (
        exchange_day_status = 'complete'
        AND (
            expected_instruments = 0
            OR missing_price_instruments > 0
            OR unknown_calendar_instruments > 0
            OR unknown_calendar_coverage_instruments > 0
            OR unknown_instrument_lifecycle_instruments > 0
        )
    )
