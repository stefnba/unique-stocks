{{ config(tags=['ingestion_control']) }}

WITH instrument AS (
    SELECT *
    FROM {{ ref('int_eod_price_provider_instrument_lifecycle') }}
    WHERE is_tradable
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
        instrument.instrument_snapshot_date,
        instrument.first_observed_price_date AS min_bar_date,
        instrument.last_observed_price_date AS max_bar_date,
        instrument.observed_price_days AS bar_count,
        instrument.fundamental_profile_id,
        instrument.provider_lifecycle_start_date,
        instrument.provider_lifecycle_start_date_source,
        instrument.provider_lifecycle_end_date,
        instrument.provider_lifecycle_end_date_source,
        instrument.has_fundamental_lifecycle_dates,
        instrument.is_delisted,
        instrument.expected_price_start_date,
        instrument.expected_price_end_date,
        instrument.has_observed_price_history,
        instrument.has_provider_lifecycle_evidence,
        instrument.lifecycle_evidence_source,
        instrument.lifecycle_confidence,
        trading_day.universe_tier,
        trading_day.daily_coverage_mode,
        trading_day.historical_coverage_mode,
        trading_day.is_daily_coverage_blocking,
        trading_day.policy_priority,
        trading_day.policy_reason,
        trading_day.policy_owner,
        trading_day.policy_last_reviewed_on,
        trading_day.provider_schedule_exchange_code,
        trading_day.schedule_mapping_method,
        trading_day.schedule_mapping_confidence,
        trading_day.exchange_schedule_name,
        trading_day.timezone,
        trading_day.working_days,
        trading_day.bar_date,
        trading_day.day_name,
        trading_day.is_calendar_known,
        trading_day.is_calendar_coverage_known,
        trading_day.calendar_coverage_status,
        trading_day.calendar_coverage_start_date,
        trading_day.calendar_coverage_end_date,
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
    LEFT JOIN no_data_coverage
        ON instrument.data_provider = no_data_coverage.data_provider
        AND instrument.provider_exchange_code = no_data_coverage.provider_exchange_code
        AND instrument.provider_instrument_code = no_data_coverage.provider_instrument_code
    WHERE (trading_day.is_trading_day OR NOT trading_day.is_calendar_known)
        AND trading_day.bar_date <= CURRENT_DATE
        AND (
            (
                trading_day.bar_date >= instrument.expected_price_start_date
                AND (
                    instrument.expected_price_end_date IS NULL
                    OR trading_day.bar_date <= instrument.expected_price_end_date
                )
            )
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
    fundamental_profile_id,
    provider_lifecycle_start_date,
    provider_lifecycle_start_date_source,
    provider_lifecycle_end_date,
    provider_lifecycle_end_date_source,
    has_fundamental_lifecycle_dates,
    is_delisted,
    expected_price_start_date,
    expected_price_end_date,
    has_observed_price_history,
    has_provider_lifecycle_evidence,
    lifecycle_evidence_source,
    lifecycle_confidence,
    universe_tier,
    daily_coverage_mode,
    historical_coverage_mode,
    is_daily_coverage_blocking,
    policy_priority,
    policy_reason,
    policy_owner,
    policy_last_reviewed_on,
    provider_schedule_exchange_code,
    schedule_mapping_method,
    schedule_mapping_confidence,
    exchange_schedule_name,
    timezone,
    working_days,
    bar_date,
    day_name,
    is_calendar_known,
    is_calendar_coverage_known,
    calendar_coverage_status,
    calendar_coverage_start_date,
    calendar_coverage_end_date,
    is_working_day,
    is_full_holiday,
    is_early_close,
    is_trading_day,
    full_holiday_name,
    early_close_name,
    early_close_time
FROM expected
