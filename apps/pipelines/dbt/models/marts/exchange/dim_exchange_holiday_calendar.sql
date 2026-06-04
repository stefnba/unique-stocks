WITH exchange_holiday_calendar AS (
    SELECT *
    FROM {{ ref('int_exchange_holiday_calendar') }}
),

final AS (
    SELECT
        {{ surrogate_key([
            "data_provider",
            "provider_schedule_exchange_code",
            "holiday_date",
            "holiday_name",
            "holiday_type",
            "early_close_time"
        ]) }} AS exchange_holiday_calendar_pk,
        data_provider,
        provider_schedule_exchange_code,
        linked_provider_exchange_codes,
        exchange_schedule_name,
        timezone,
        holiday_name,
        holiday_type,
        is_early_close,
        early_close_time,
        holiday_date,
        holiday_snapshot_date,
        holiday_ingested_at
    FROM exchange_holiday_calendar
)

SELECT * FROM final
