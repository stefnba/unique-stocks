WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_document') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        UPPER(TRIM(CAST(source_data.code AS VARCHAR))) AS code,
        NULLIF(TRIM(CAST(source_data.name AS VARCHAR)), '') AS instrument_name,
        NULLIF(TRIM(CAST(source_data.instrument_type AS VARCHAR)), '') AS instrument_type,
        LOWER(TRIM(CAST(source_data.instrument_family AS VARCHAR))) AS instrument_family,
        NULLIF(UPPER(TRIM(CAST(source_data.primary_ticker AS VARCHAR))), '') AS primary_ticker,
        NULLIF(TRIM(CAST(source_data.provider_listing_exchange_code AS VARCHAR)), '') AS provider_listing_exchange_code,
        CAST(source_data.provider_updated_at AS DATE) AS provider_updated_at,
        CAST(source_data.top_level_sections AS JSON) AS top_level_sections,
        CAST(source_data.payload_hash AS VARCHAR) AS payload_hash,
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
            PARTITION BY snapshot_date, ticker, data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
