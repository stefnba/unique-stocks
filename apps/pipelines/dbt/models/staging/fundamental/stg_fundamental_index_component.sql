WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_index_component') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        CAST(source_data.provider_position AS BIGINT) AS provider_position,
        UPPER(TRIM(CAST(source_data.component_code AS VARCHAR))) AS component_code,
        NULLIF(UPPER(TRIM(CAST(source_data.component_exchange AS VARCHAR))), '') AS component_exchange,
        NULLIF(UPPER(TRIM(CAST(source_data.component_ticker AS VARCHAR))), '') AS component_ticker,
        NULLIF(TRIM(CAST(source_data.component_name AS VARCHAR)), '') AS component_name,
        NULLIF(TRIM(CAST(source_data.sector AS VARCHAR)), '') AS sector,
        NULLIF(TRIM(CAST(source_data.industry AS VARCHAR)), '') AS industry,
        CAST(source_data.weight AS DECIMAL(38, 10)) AS weight,
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
            PARTITION BY snapshot_date, ticker, component_code, component_exchange, data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
