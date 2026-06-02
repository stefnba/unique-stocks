WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'exchange_holiday') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_schedule_exchange_code AS VARCHAR))) AS provider_schedule_exchange_code,
        CAST(source_data.holiday_date AS DATE) AS holiday_date,
        NULLIF(TRIM(CAST(source_data.holiday_name AS VARCHAR)), '') AS holiday_name,
        LOWER(NULLIF(TRIM(CAST(source_data.holiday_type AS VARCHAR)), '')) AS holiday_type,
        NULLIF(TRIM(CAST(source_data.early_close_time AS VARCHAR)), '') AS early_close_time,
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
            PARTITION BY snapshot_date, provider_schedule_exchange_code, holiday_date, data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT
    * EXCLUDE (row_number),
    holiday_type = 'earlyclose' OR early_close_time IS NOT NULL AS is_early_close
FROM deduplicated
WHERE row_number = 1
