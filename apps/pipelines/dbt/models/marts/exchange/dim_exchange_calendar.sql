WITH exchange_calendar AS (
    SELECT *
    FROM {{ ref('int_exchange_calendar') }}
),

final AS (
    SELECT
        {{ surrogate_key(["data_provider", "provider_schedule_exchange_code"]) }} AS exchange_calendar_pk,
        data_provider,
        provider_schedule_exchange_code,
        linked_provider_exchange_codes,
        exchange_schedule_name,
        timezone,
        working_days,
        has_pre_market,
        has_after_hours,
        has_lunch_break,
        session_open,
        session_close,
        pre_market_open,
        pre_market_close,
        after_hours_open,
        after_hours_close,
        lunch_break_start,
        lunch_break_end,
        schedule_snapshot_date,
        schedule_ingested_at
    FROM exchange_calendar
)

SELECT * FROM final
