WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'instrument') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.provider_instrument_code AS VARCHAR))) AS provider_instrument_code,
        NULLIF(TRIM(CAST(source_data.name AS VARCHAR)), '') AS instrument_name,
        NULLIF(TRIM(CAST(source_data.country AS VARCHAR)), '') AS country,
        NULLIF(TRIM(CAST(source_data.provider_listing_exchange_code AS VARCHAR)), '') AS provider_listing_exchange_code,
        NULLIF(UPPER(TRIM(CAST(source_data.currency AS VARCHAR))), '') AS currency,
        NULLIF(TRIM(CAST(source_data.asset_type AS VARCHAR)), '') AS asset_type,
        LOWER(NULLIF(TRIM(CAST(source_data.asset_type AS VARCHAR)), '')) AS normalized_asset_type,
        NULLIF(UPPER(TRIM(CAST(source_data.isin AS VARCHAR))), '') AS isin,
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
            PARTITION BY snapshot_date, provider_exchange_code, provider_instrument_code, data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
