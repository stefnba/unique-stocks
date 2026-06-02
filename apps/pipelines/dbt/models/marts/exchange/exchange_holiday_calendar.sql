SELECT
    exchange_holiday_calendar_id,
    data_provider,
    provider_schedule_exchange_code,
    linked_provider_exchange_codes,
    exchange_schedule_name,
    timezone,
    holiday_date,
    holiday_name,
    holiday_type,
    is_early_close,
    early_close_time,
    holiday_snapshot_date,
    holiday_row_hash,
    holiday_source_uri,
    holiday_ingested_at
FROM {{ ref('int_exchange_holiday_calendar') }}
