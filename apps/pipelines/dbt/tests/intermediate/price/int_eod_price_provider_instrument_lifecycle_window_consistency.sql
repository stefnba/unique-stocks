SELECT *
FROM {{ ref('int_eod_price_provider_instrument_lifecycle') }}
WHERE expected_price_start_date IS NULL
    OR (
        expected_price_end_date IS NOT NULL
        AND expected_price_start_date > expected_price_end_date
    )
    OR (
        provider_lifecycle_start_date_source IS NOT NULL
        AND provider_lifecycle_start_date IS NULL
    )
    OR (
        provider_lifecycle_start_date_source IS NULL
        AND provider_lifecycle_start_date IS NOT NULL
    )
    OR (
        provider_lifecycle_end_date_source IS NOT NULL
        AND provider_lifecycle_end_date IS NULL
    )
    OR (
        provider_lifecycle_end_date_source IS NULL
        AND provider_lifecycle_end_date IS NOT NULL
    )
    OR (
        has_fundamental_lifecycle_dates
        AND provider_lifecycle_start_date IS NULL
        AND provider_lifecycle_end_date IS NULL
    )
