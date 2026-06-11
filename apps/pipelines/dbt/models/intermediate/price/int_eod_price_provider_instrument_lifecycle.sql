{{ config(tags=['ingestion_control']) }}

WITH instrument AS (
    SELECT *
    FROM {{ ref('int_latest_instrument_universe') }}
    WHERE data_provider = 'eodhd'
        AND is_tradable
),

price_ranges AS (
    SELECT *
    FROM {{ ref('int_eod_price_completion_ranges') }}
),

terminal_coverage AS (
    SELECT *
    FROM {{ ref('int_eod_price_backfill_terminal_coverage') }}
),

fundamental_profile AS (
    SELECT *
    FROM {{ ref('int_fundamental_instrument_profile') }}
),

no_data_coverage AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        MIN(COALESCE(from_date, to_date)) AS first_no_data_coverage_date,
        MAX(to_date) AS last_no_data_coverage_date,
        BOOL_OR(from_date IS NULL) AS has_open_start_no_data_coverage,
        COUNT(*) AS no_data_coverage_windows
    FROM terminal_coverage
    WHERE status = 'no_data'
    GROUP BY 1, 2, 3
),

completed_coverage AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        MIN(COALESCE(from_date, to_date)) AS first_completed_coverage_date,
        MAX(to_date) AS last_completed_coverage_date,
        BOOL_OR(from_date IS NULL) AS has_open_start_completed_coverage,
        COUNT(*) AS completed_coverage_windows
    FROM terminal_coverage
    WHERE status = 'completed'
    GROUP BY 1, 2, 3
),

joined AS (
    SELECT
        instrument.instrument_universe_id,
        instrument.snapshot_date AS instrument_snapshot_date,
        instrument.data_provider,
        instrument.provider_exchange_code,
        instrument.provider_instrument_code,
        instrument.instrument_family,
        instrument.is_tradable,
        fundamental_profile.fundamental_profile_id,
        fundamental_profile.ipo_date,
        fundamental_profile.fund_inception_date,
        COALESCE(fundamental_profile.ipo_date, fundamental_profile.fund_inception_date)
            AS provider_lifecycle_start_date,
        CASE
            WHEN fundamental_profile.ipo_date IS NOT NULL THEN 'stock_ipo_date'
            WHEN fundamental_profile.fund_inception_date IS NOT NULL THEN 'fund_inception_date'
        END AS provider_lifecycle_start_date_source,
        COALESCE(fundamental_profile.is_delisted, FALSE) AS is_delisted,
        fundamental_profile.delisted_date AS provider_lifecycle_end_date,
        CASE
            WHEN fundamental_profile.delisted_date IS NOT NULL THEN 'stock_delisted_date'
        END AS provider_lifecycle_end_date_source,
        price_ranges.min_bar_date AS first_observed_price_date,
        price_ranges.max_bar_date AS last_observed_price_date,
        price_ranges.bar_count AS observed_price_days,
        no_data_coverage.first_no_data_coverage_date,
        no_data_coverage.last_no_data_coverage_date,
        COALESCE(no_data_coverage.has_open_start_no_data_coverage, FALSE)
            AS has_open_start_no_data_coverage,
        COALESCE(no_data_coverage.no_data_coverage_windows, 0) AS no_data_coverage_windows,
        completed_coverage.first_completed_coverage_date,
        completed_coverage.last_completed_coverage_date,
        COALESCE(completed_coverage.has_open_start_completed_coverage, FALSE)
            AS has_open_start_completed_coverage,
        COALESCE(completed_coverage.completed_coverage_windows, 0) AS completed_coverage_windows
    FROM instrument
    LEFT JOIN fundamental_profile
        ON instrument.data_provider = fundamental_profile.data_provider
        AND instrument.provider_exchange_code = fundamental_profile.provider_exchange_code
        AND instrument.provider_instrument_code = fundamental_profile.provider_instrument_code
    LEFT JOIN price_ranges
        ON instrument.data_provider = price_ranges.data_provider
        AND instrument.provider_exchange_code = price_ranges.provider_exchange_code
        AND instrument.provider_instrument_code = price_ranges.provider_instrument_code
    LEFT JOIN no_data_coverage
        ON instrument.data_provider = no_data_coverage.data_provider
        AND instrument.provider_exchange_code = no_data_coverage.provider_exchange_code
        AND instrument.provider_instrument_code = no_data_coverage.provider_instrument_code
    LEFT JOIN completed_coverage
        ON instrument.data_provider = completed_coverage.data_provider
        AND instrument.provider_exchange_code = completed_coverage.provider_exchange_code
        AND instrument.provider_instrument_code = completed_coverage.provider_instrument_code
),

final AS (
    SELECT
        *,
        first_observed_price_date IS NOT NULL AS has_observed_price_history,
        (
            provider_lifecycle_start_date IS NOT NULL
            OR provider_lifecycle_end_date IS NOT NULL
        ) AS has_fundamental_lifecycle_dates,
        no_data_coverage_windows > 0 AS has_terminal_no_data_coverage,
        completed_coverage_windows > 0 AS has_completed_backfill_coverage,
        (
            first_observed_price_date IS NOT NULL
            OR provider_lifecycle_start_date IS NOT NULL
            OR provider_lifecycle_end_date IS NOT NULL
            OR no_data_coverage_windows > 0
            OR completed_coverage_windows > 0
        ) AS has_provider_lifecycle_evidence,
        CASE
            WHEN provider_lifecycle_start_date IS NOT NULL AND first_observed_price_date IS NOT NULL
                THEN LEAST(provider_lifecycle_start_date, first_observed_price_date)
            WHEN provider_lifecycle_start_date IS NOT NULL THEN provider_lifecycle_start_date
            WHEN first_observed_price_date IS NOT NULL THEN first_observed_price_date
            ELSE CURRENT_DATE
        END AS expected_price_start_date,
        provider_lifecycle_end_date AS expected_price_end_date,
        CASE
            WHEN provider_lifecycle_start_date IS NOT NULL OR provider_lifecycle_end_date IS NOT NULL
                THEN 'provider_fundamental_profile'
            WHEN first_observed_price_date IS NOT NULL THEN 'observed_price_history'
            WHEN no_data_coverage_windows > 0 THEN 'terminal_no_data'
            WHEN completed_coverage_windows > 0 THEN 'completed_backfill'
            ELSE 'latest_universe_only'
        END AS lifecycle_evidence_source,
        CASE
            WHEN provider_lifecycle_start_date IS NOT NULL OR provider_lifecycle_end_date IS NOT NULL
                THEN 'provider_fundamental'
            WHEN first_observed_price_date IS NOT NULL THEN 'observed'
            WHEN no_data_coverage_windows > 0 THEN 'provider_terminal'
            WHEN completed_coverage_windows > 0 THEN 'provider_terminal'
            ELSE 'unconfirmed'
        END AS lifecycle_confidence
    FROM joined
)

SELECT * FROM final
