WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_index_historical_component') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        CAST(source_data.provider_position AS BIGINT) AS provider_position,
        UPPER(TRIM(CAST(source_data.component_code AS VARCHAR))) AS component_code,
        NULLIF(TRIM(CAST(source_data.component_name AS VARCHAR)), '') AS component_name,
        CAST(source_data.start_date AS DATE) AS start_date,
        CAST(source_data.end_date AS DATE) AS end_date,
        CAST(source_data.is_active_now AS BOOLEAN) AS is_active_now,
        CAST(source_data.is_delisted AS BOOLEAN) AS is_delisted,
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
            PARTITION BY snapshot_date, ticker, component_code, start_date, data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
