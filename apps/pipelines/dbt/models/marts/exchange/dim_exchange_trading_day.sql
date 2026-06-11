WITH trading_day AS (
    SELECT *
    FROM {{ ref('int_exchange_trading_day') }}
),

exchange_calendar AS (
    SELECT *
    FROM {{ ref('int_exchange_calendar') }}
),

base AS (
    SELECT
        {{ surrogate_key([
            "trading_day.data_provider",
            "trading_day.provider_exchange_code",
            "trading_day.bar_date",
        ]) }}
            AS exchange_trading_day_pk,
        CASE
            WHEN trading_day.provider_schedule_exchange_code IS NOT NULL
                THEN {{ surrogate_key([
                    "trading_day.data_provider",
            "trading_day.provider_schedule_exchange_code",
        ]) }}
        END AS exchange_calendar_pk,
        CASE
            WHEN trading_day.mic IS NOT NULL
                THEN {{ surrogate_key(["trading_day.mic"]) }}
        END AS exchange_pk,
        trading_day.data_provider,
        trading_day.provider_exchange_code,
        trading_day.mic,
        trading_day.provider_code_kind,
        trading_day.source_kind,
        trading_day.exchange_catalog_name,
        trading_day.provider_schedule_exchange_code,
        trading_day.schedule_mapping_method,
        trading_day.schedule_mapping_confidence,
        trading_day.mic_mapping_method,
        trading_day.mic_mapping_confidence,
        trading_day.exchange_schedule_name,
        trading_day.timezone,
        trading_day.working_days,
        trading_day.day_name,
        CASE
            WHEN NOT trading_day.is_calendar_known THEN 'unknown_calendar'
            WHEN NOT trading_day.is_calendar_coverage_known THEN 'unknown_calendar_coverage'
            WHEN trading_day.is_full_holiday THEN 'closed_holiday'
            WHEN NOT trading_day.is_working_day THEN 'closed_non_working_day'
            WHEN trading_day.is_early_close THEN 'early_close'
            ELSE 'open'
        END AS trading_day_status,
        CASE
            WHEN NOT trading_day.is_calendar_known THEN 'unknown_calendar'
            WHEN NOT trading_day.is_calendar_coverage_known THEN 'unknown_calendar_coverage'
            WHEN trading_day.is_full_holiday THEN 'full_holiday'
            WHEN NOT trading_day.is_working_day THEN 'non_working_day'
        END AS closure_reason,
        trading_day.is_calendar_known,
        trading_day.is_calendar_coverage_known,
        trading_day.calendar_coverage_status,
        trading_day.is_working_day,
        trading_day.is_full_holiday,
        trading_day.is_early_close,
        trading_day.is_trading_day,
        trading_day.is_calendar_known AND NOT trading_day.is_trading_day AS is_closed,
        exchange_calendar.has_pre_market,
        exchange_calendar.has_after_hours,
        exchange_calendar.has_lunch_break,
        trading_day.full_holiday_name,
        trading_day.early_close_name,
        exchange_calendar.session_open,
        exchange_calendar.session_close,
        CASE
            WHEN trading_day.is_trading_day THEN exchange_calendar.session_open
        END AS effective_session_open,
        CASE
            WHEN trading_day.is_trading_day AND trading_day.is_early_close THEN trading_day.early_close_time
            WHEN trading_day.is_trading_day THEN exchange_calendar.session_close
        END AS effective_session_close,
        exchange_calendar.pre_market_open,
        exchange_calendar.pre_market_close,
        exchange_calendar.after_hours_open,
        exchange_calendar.after_hours_close,
        exchange_calendar.lunch_break_start,
        exchange_calendar.lunch_break_end,
        trading_day.early_close_time,
        trading_day.bar_date AS trading_date,
        trading_day.exchange_creation_date,
        trading_day.calendar_coverage_start_date,
        trading_day.calendar_coverage_end_date,
        trading_day.calendar_start_date,
        trading_day.calendar_start_date_source,
        exchange_calendar.schedule_snapshot_date,
        exchange_calendar.schedule_ingested_at
    FROM trading_day
    LEFT JOIN exchange_calendar
        ON trading_day.data_provider = exchange_calendar.data_provider
        AND trading_day.provider_schedule_exchange_code = exchange_calendar.provider_schedule_exchange_code
),

session_times AS (
    SELECT
        *,
        CASE
            WHEN effective_session_open IS NOT NULL
                AND timezone IS NOT NULL
                THEN trading_date + CAST(effective_session_open AS TIME)
        END AS effective_session_open_local_at,
        CASE
            WHEN effective_session_close IS NOT NULL
                AND timezone IS NOT NULL
                THEN trading_date + CAST(effective_session_close AS TIME)
        END AS effective_session_close_local_at
    FROM base
),

session_instants AS (
    SELECT
        *,
        CASE
            WHEN effective_session_open_local_at IS NOT NULL
                AND timezone IS NOT NULL
                THEN (effective_session_open_local_at AT TIME ZONE timezone) AT TIME ZONE 'UTC'
        END AS effective_session_open_utc_at,
        CASE
            WHEN effective_session_close_local_at IS NOT NULL
                AND timezone IS NOT NULL
                THEN (effective_session_close_local_at AT TIME ZONE timezone) AT TIME ZONE 'UTC'
        END AS effective_session_close_utc_at
    FROM session_times
),

final AS (
    SELECT
        exchange_trading_day_pk,
        exchange_calendar_pk,
        exchange_pk,
        data_provider,
        provider_exchange_code,
        mic,
        provider_code_kind,
        source_kind,
        exchange_catalog_name,
        provider_schedule_exchange_code,
        schedule_mapping_method,
        schedule_mapping_confidence,
        mic_mapping_method,
        mic_mapping_confidence,
        exchange_schedule_name,
        timezone,
        working_days,
        day_name,
        trading_day_status,
        closure_reason,
        is_calendar_known,
        is_calendar_coverage_known,
        calendar_coverage_status,
        is_working_day,
        is_full_holiday,
        is_early_close,
        is_trading_day,
        is_closed,
        has_pre_market,
        has_after_hours,
        has_lunch_break,
        full_holiday_name,
        early_close_name,
        session_open,
        session_close,
        effective_session_open,
        effective_session_close,
        effective_session_open_local_at,
        effective_session_close_local_at,
        effective_session_open_utc_at,
        effective_session_close_utc_at,
        CASE
            WHEN effective_session_open_utc_at IS NOT NULL
                AND effective_session_open_local_at IS NOT NULL
                THEN DATE_DIFF(
                        'minute',
                        effective_session_open_utc_at,
                        effective_session_open_local_at
                    )
        END AS utc_offset_minutes_at_session_open,
        CASE
            WHEN effective_session_close_utc_at IS NOT NULL
                AND effective_session_close_local_at IS NOT NULL
                THEN DATE_DIFF(
                        'minute',
                        effective_session_close_utc_at,
                        effective_session_close_local_at
                    )
        END AS utc_offset_minutes_at_session_close,
        pre_market_open,
        pre_market_close,
        after_hours_open,
        after_hours_close,
        lunch_break_start,
        lunch_break_end,
        early_close_time,
        trading_date,
        exchange_creation_date,
        calendar_coverage_start_date,
        calendar_coverage_end_date,
        calendar_start_date,
        calendar_start_date_source,
        schedule_snapshot_date,
        schedule_ingested_at
    FROM session_instants
)

SELECT * FROM final
