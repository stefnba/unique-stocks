SELECT *
FROM {{ ref('int_eod_price_instrument_day_coverage') }}
WHERE (coverage_status = 'unknown_calendar' AND is_calendar_known)
    OR (coverage_status = 'priced' AND NOT has_price)
    OR (coverage_status = 'known_no_data' AND (has_price OR NOT has_no_data_coverage))
    OR (
        coverage_status = 'missing_price'
        AND (
            NOT is_calendar_known
            OR has_price
            OR has_no_data_coverage
        )
    )
