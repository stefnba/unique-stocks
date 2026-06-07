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

no_data_coverage AS (
    SELECT *
    FROM {{ ref('int_eod_price_backfill_terminal_coverage') }}
    WHERE status = 'no_data'
),

trading_day AS (
    SELECT *
    FROM {{ ref('int_exchange_trading_day') }}
),

expected AS (
    SELECT DISTINCT
        instrument.data_provider,
        instrument.provider_exchange_code,
        instrument.provider_instrument_code,
        instrument.instrument_family,
        instrument.instrument_universe_id,
        instrument.snapshot_date AS instrument_snapshot_date,
        price_ranges.min_bar_date,
        price_ranges.max_bar_date,
        price_ranges.bar_count,
        trading_day.provider_schedule_exchange_code,
        trading_day.schedule_mapping_method,
        trading_day.schedule_mapping_confidence,
        trading_day.exchange_schedule_name,
        trading_day.timezone,
        trading_day.working_days,
        trading_day.bar_date,
        trading_day.day_name,
        trading_day.is_calendar_known,
        trading_day.is_working_day,
        trading_day.is_full_holiday,
        trading_day.is_early_close,
        trading_day.is_trading_day,
        trading_day.full_holiday_name,
        trading_day.early_close_name,
        trading_day.early_close_time
    FROM instrument
    INNER JOIN trading_day
        ON instrument.data_provider = trading_day.data_provider
        AND instrument.provider_exchange_code = trading_day.provider_exchange_code
    LEFT JOIN price_ranges
        ON instrument.data_provider = price_ranges.data_provider
        AND instrument.provider_exchange_code = price_ranges.provider_exchange_code
        AND instrument.provider_instrument_code = price_ranges.provider_instrument_code
    LEFT JOIN no_data_coverage
        ON instrument.data_provider = no_data_coverage.data_provider
        AND instrument.provider_exchange_code = no_data_coverage.provider_exchange_code
        AND instrument.provider_instrument_code = no_data_coverage.provider_instrument_code
    WHERE (trading_day.is_trading_day OR NOT trading_day.is_calendar_known)
        AND trading_day.bar_date <= CURRENT_DATE
        AND (
            trading_day.bar_date >= COALESCE(price_ranges.min_bar_date, CURRENT_DATE)
            OR (
                no_data_coverage.unit_key_hash IS NOT NULL
                AND (
                    no_data_coverage.from_date IS NULL
                    OR trading_day.bar_date >= no_data_coverage.from_date
                )
                AND trading_day.bar_date <= no_data_coverage.to_date
            )
        )
)

SELECT
    data_provider
    || ':'
    || provider_exchange_code
    || ':'
    || provider_instrument_code
    || ':'
    || CAST(bar_date AS VARCHAR) AS expected_instrument_day_id,
    data_provider,
    provider_exchange_code,
    provider_instrument_code,
    instrument_family,
    instrument_universe_id,
    instrument_snapshot_date,
    min_bar_date,
    max_bar_date,
    bar_count,
    provider_schedule_exchange_code,
    schedule_mapping_method,
    schedule_mapping_confidence,
    exchange_schedule_name,
    timezone,
    working_days,
    bar_date,
    day_name,
    is_calendar_known,
    is_working_day,
    is_full_holiday,
    is_early_close,
    is_trading_day,
    full_holiday_name,
    early_close_name,
    early_close_time
FROM expected
