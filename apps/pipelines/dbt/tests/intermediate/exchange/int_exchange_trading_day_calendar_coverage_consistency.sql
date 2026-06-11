SELECT *
FROM {{ ref('int_exchange_trading_day') }}
WHERE (
    calendar_coverage_status = 'covered'
    AND NOT is_calendar_coverage_known
)
OR (
    calendar_coverage_status != 'covered'
    AND is_calendar_coverage_known
)
