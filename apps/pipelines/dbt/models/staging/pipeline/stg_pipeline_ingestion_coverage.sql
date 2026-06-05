{{ config(tags=['ingestion_control']) }}

WITH source AS (
    SELECT *
    FROM {{ source('pipeline', 'ingestion_coverage') }}
),

renamed AS (
    SELECT
        CAST(source_data.coverage_id AS VARCHAR) AS coverage_id,
        CAST(source_data.run_id AS VARCHAR) AS run_id,
        LOWER(TRIM(CAST(source_data.domain AS VARCHAR))) AS domain,
        LOWER(TRIM(CAST(source_data.provider AS VARCHAR))) AS data_provider,
        CAST(source_data.unit_type AS VARCHAR) AS unit_type,
        CAST(source_data.unit_key_hash AS VARCHAR) AS unit_key_hash,
        CAST(source_data.unit_key_json AS JSON) AS unit_key_json,
        LOWER(TRIM(CAST(source_data.status AS VARCHAR))) AS status,
        NULLIF(TRIM(CAST(source_data.reason AS VARCHAR)), '') AS reason,
        CAST(source_data.rows_raw AS INTEGER) AS rows_raw,
        CAST(source_data.rows_valid AS INTEGER) AS rows_valid,
        CAST(source_data.rows_rejected AS INTEGER) AS rows_rejected,
        NULLIF(TRIM(CAST(source_data.source_uri AS VARCHAR)), '') AS source_uri,
        CAST(source_data.recorded_at AS TIMESTAMPTZ) AS recorded_at
    FROM source AS source_data
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY domain, data_provider, unit_type, unit_key_hash, status
            ORDER BY recorded_at DESC, coverage_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
