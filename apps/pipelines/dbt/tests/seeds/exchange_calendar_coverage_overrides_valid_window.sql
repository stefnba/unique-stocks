SELECT *
FROM {{ ref('exchange_calendar_coverage_overrides') }}
WHERE coverage_start_date IS NULL
    OR coverage_end_date IS NULL
    OR coverage_start_date > coverage_end_date
