WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'exchange_schedule') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_schedule_exchange_code AS VARCHAR))) AS provider_schedule_exchange_code,
        NULLIF(TRIM(CAST(source_data.name AS VARCHAR)), '') AS exchange_schedule_name,
        NULLIF(TRIM(CAST(source_data.timezone AS VARCHAR)), '') AS timezone,
        NULLIF(TRIM(CAST(source_data.session_open AS VARCHAR)), '') AS session_open,
        NULLIF(TRIM(CAST(source_data.session_close AS VARCHAR)), '') AS session_close,
        NULLIF(TRIM(CAST(source_data.working_days AS VARCHAR)), '') AS working_days,
        NULLIF(TRIM(CAST(source_data.pre_market_open AS VARCHAR)), '') AS pre_market_open,
        NULLIF(TRIM(CAST(source_data.pre_market_close AS VARCHAR)), '') AS pre_market_close,
        NULLIF(TRIM(CAST(source_data.after_hours_open AS VARCHAR)), '') AS after_hours_open,
        NULLIF(TRIM(CAST(source_data.after_hours_close AS VARCHAR)), '') AS after_hours_close,
        NULLIF(TRIM(CAST(source_data.lunch_break_start AS VARCHAR)), '') AS lunch_break_start,
        NULLIF(TRIM(CAST(source_data.lunch_break_end AS VARCHAR)), '') AS lunch_break_end,
        LOWER(TRIM(CAST(source_data.data_provider AS VARCHAR))) AS data_provider,
        CAST(source_data.row_hash AS VARCHAR) AS row_hash,
        CAST(source_data.source_uri AS VARCHAR) AS source_uri,
        CAST(source_data.ingested_at AS TIMESTAMPTZ) AS ingested_at
    FROM source AS source_data
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date, provider_schedule_exchange_code, data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT
    * EXCLUDE (row_number),
    pre_market_open IS NOT NULL OR pre_market_close IS NOT NULL AS has_pre_market,
    after_hours_open IS NOT NULL OR after_hours_close IS NOT NULL AS has_after_hours,
    lunch_break_start IS NOT NULL OR lunch_break_end IS NOT NULL AS has_lunch_break
FROM deduplicated
WHERE row_number = 1
