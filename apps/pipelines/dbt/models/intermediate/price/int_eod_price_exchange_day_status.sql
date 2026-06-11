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

final AS (
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
        trading_day.bar_date,
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
    WHERE trading_day.bar_date <= CURRENT_DATE
)

SELECT * FROM final
