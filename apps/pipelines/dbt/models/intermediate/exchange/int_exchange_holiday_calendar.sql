WITH latest_schedule_snapshot AS (
    SELECT
        data_provider,
        provider_schedule_exchange_code,
        MAX(snapshot_date) AS latest_snapshot_date
    FROM {{ ref('stg_exchange_schedule') }}
    GROUP BY 1, 2
),

holiday AS (
    SELECT holiday_rows.*
    FROM {{ ref('stg_exchange_holiday') }} AS holiday_rows
    INNER JOIN latest_schedule_snapshot
        ON holiday_rows.data_provider = latest_schedule_snapshot.data_provider
        AND holiday_rows.provider_schedule_exchange_code = latest_schedule_snapshot.provider_schedule_exchange_code
        AND holiday_rows.snapshot_date = latest_schedule_snapshot.latest_snapshot_date
),

calendar AS (
    SELECT *
    FROM {{ ref('int_exchange_calendar') }}
)

SELECT
    holiday.data_provider
    || ':'
    || holiday.provider_schedule_exchange_code
    || ':'
    || CAST(holiday.holiday_date AS VARCHAR)
        AS exchange_holiday_calendar_id,
    holiday.data_provider,
    holiday.provider_schedule_exchange_code,
    calendar.linked_provider_exchange_codes,
    calendar.exchange_schedule_name,
    calendar.timezone,
    holiday.holiday_date,
    holiday.holiday_name,
    holiday.holiday_type,
    holiday.is_early_close,
    holiday.early_close_time,
    holiday.snapshot_date AS holiday_snapshot_date,
    holiday.row_hash AS holiday_row_hash,
    holiday.source_uri AS holiday_source_uri,
    holiday.ingested_at AS holiday_ingested_at
FROM holiday
LEFT JOIN calendar
    ON holiday.data_provider = calendar.data_provider
    AND holiday.provider_schedule_exchange_code = calendar.provider_schedule_exchange_code
