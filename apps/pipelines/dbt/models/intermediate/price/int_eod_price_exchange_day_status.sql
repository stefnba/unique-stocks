{{ config(tags=['ingestion_control']) }}

WITH coverage AS (
    SELECT *
    FROM {{ ref('int_eod_price_instrument_day_coverage') }}
),

trading_day AS (
    SELECT *
    FROM {{ ref('int_exchange_trading_day') }}
),

aggregated AS (
    SELECT
        data_provider,
        provider_exchange_code,
        bar_date,
        COUNT(*) AS expected_instruments,
        SUM(CASE WHEN coverage_status = 'priced' THEN 1 ELSE 0 END) AS priced_instruments,
        SUM(CASE WHEN coverage_status = 'missing_price' THEN 1 ELSE 0 END) AS missing_price_instruments,
        SUM(CASE WHEN coverage_status = 'known_no_data' THEN 1 ELSE 0 END) AS known_no_data_instruments,
        SUM(CASE WHEN coverage_status = 'unknown_calendar' THEN 1 ELSE 0 END) AS unknown_calendar_instruments,
        SUM(CASE WHEN coverage_status = 'unknown_calendar_coverage' THEN 1 ELSE 0 END)
            AS unknown_calendar_coverage_instruments,
        SUM(CASE WHEN coverage_status = 'unknown_instrument_lifecycle' THEN 1 ELSE 0 END)
            AS unknown_instrument_lifecycle_instruments
    FROM coverage
    GROUP BY 1, 2, 3
),

latest_expected_trading_day AS (
    SELECT
        data_provider,
        provider_exchange_code,
        MAX(bar_date) AS latest_expected_bar_date
    FROM trading_day
    WHERE bar_date <= CURRENT_DATE
        AND (is_trading_day OR NOT is_calendar_known)
    GROUP BY 1, 2
),

status_base AS (
    SELECT
        trading_day.data_provider
        || ':'
        || trading_day.provider_exchange_code
        || ':'
        || CAST(trading_day.bar_date AS VARCHAR) AS exchange_day_status_id,
        trading_day.data_provider,
        trading_day.provider_exchange_code,
        trading_day.provider_schedule_exchange_code,
        trading_day.exchange_schedule_name,
        trading_day.timezone,
        trading_day.universe_tier,
        trading_day.daily_coverage_mode,
        trading_day.historical_coverage_mode,
        trading_day.is_daily_coverage_blocking,
        trading_day.policy_priority,
        trading_day.policy_reason,
        trading_day.policy_owner,
        trading_day.policy_last_reviewed_on,
        trading_day.bar_date,
        latest_expected_trading_day.latest_expected_bar_date,
        COALESCE(
            trading_day.bar_date = latest_expected_trading_day.latest_expected_bar_date,
            FALSE
        ) AS is_latest_expected_trading_day,
        trading_day.is_calendar_known,
        trading_day.is_calendar_coverage_known,
        trading_day.calendar_coverage_status,
        trading_day.calendar_coverage_start_date,
        trading_day.calendar_coverage_end_date,
        trading_day.is_working_day,
        trading_day.is_full_holiday,
        trading_day.is_early_close,
        trading_day.is_trading_day,
        COALESCE(aggregated.expected_instruments, 0) AS expected_instruments,
        COALESCE(aggregated.priced_instruments, 0) AS priced_instruments,
        COALESCE(aggregated.missing_price_instruments, 0) AS missing_price_instruments,
        COALESCE(aggregated.known_no_data_instruments, 0) AS known_no_data_instruments,
        COALESCE(aggregated.unknown_calendar_instruments, 0) AS unknown_calendar_instruments,
        COALESCE(aggregated.unknown_calendar_coverage_instruments, 0)
            AS unknown_calendar_coverage_instruments,
        COALESCE(aggregated.unknown_instrument_lifecycle_instruments, 0)
            AS unknown_instrument_lifecycle_instruments,
        (
            CAST(COALESCE(aggregated.priced_instruments, 0) AS DOUBLE)
            / NULLIF(COALESCE(aggregated.expected_instruments, 0), 0)
        ) AS priced_instrument_ratio,
        CASE
            WHEN NOT trading_day.is_calendar_known THEN 'unknown_calendar'
            WHEN NOT trading_day.is_trading_day THEN 'closed_exchange'
            WHEN COALESCE(aggregated.expected_instruments, 0) = 0 THEN 'no_expected_instruments'
            WHEN COALESCE(aggregated.unknown_calendar_coverage_instruments, 0) > 0
                THEN 'unknown_calendar_coverage'
            WHEN COALESCE(aggregated.unknown_instrument_lifecycle_instruments, 0) > 0
                THEN 'unknown_instrument_lifecycle'
            WHEN COALESCE(aggregated.missing_price_instruments, 0) > 0 THEN 'missing_price'
            ELSE 'complete'
        END AS exchange_day_status
    FROM trading_day
    LEFT JOIN aggregated
        ON trading_day.data_provider = aggregated.data_provider
        AND trading_day.provider_exchange_code = aggregated.provider_exchange_code
        AND trading_day.bar_date = aggregated.bar_date
    LEFT JOIN latest_expected_trading_day
        ON trading_day.data_provider = latest_expected_trading_day.data_provider
        AND trading_day.provider_exchange_code = latest_expected_trading_day.provider_exchange_code
    WHERE trading_day.bar_date <= CURRENT_DATE
),

final AS (
    SELECT
        *,
        is_daily_coverage_blocking
        AND is_latest_expected_trading_day
        AND exchange_day_status IN (
            'missing_price',
            'unknown_calendar',
            'unknown_calendar_coverage',
            'unknown_instrument_lifecycle'
        )
            AS is_blocking_coverage_gap
    FROM status_base
)

SELECT * FROM final
