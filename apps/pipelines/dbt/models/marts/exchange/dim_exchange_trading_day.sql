WITH trading_day AS (
    SELECT *
    FROM {{ ref('int_exchange_trading_day') }}
),

exchange_calendar AS (
    SELECT *
    FROM {{ ref('int_exchange_calendar') }}
),

final AS (
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
            WHEN trading_day.is_full_holiday THEN 'closed_holiday'
            WHEN NOT trading_day.is_working_day THEN 'closed_non_working_day'
            WHEN trading_day.is_early_close THEN 'early_close'
            ELSE 'open'
        END AS trading_day_status,
        CASE
            WHEN NOT trading_day.is_calendar_known THEN 'unknown_calendar'
            WHEN trading_day.is_full_holiday THEN 'full_holiday'
            WHEN NOT trading_day.is_working_day THEN 'non_working_day'
        END AS closure_reason,
        trading_day.is_calendar_known,
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
        trading_day.calendar_start_date,
        trading_day.calendar_start_date_source,
        exchange_calendar.schedule_snapshot_date,
        exchange_calendar.schedule_ingested_at
    FROM trading_day
    LEFT JOIN exchange_calendar
        ON trading_day.data_provider = exchange_calendar.data_provider
        AND trading_day.provider_schedule_exchange_code = exchange_calendar.provider_schedule_exchange_code
)

SELECT * FROM final
