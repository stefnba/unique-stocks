WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'exchange_catalog') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        NULLIF(TRIM(CAST(source_data.name AS VARCHAR)), '') AS exchange_catalog_name,
        NULLIF(UPPER(TRIM(CAST(source_data.operating_mic_codes AS VARCHAR))), '') AS operating_mic_codes,
        NULLIF(TRIM(CAST(source_data.country AS VARCHAR)), '') AS country,
        UPPER(TRIM(CAST(source_data.currency AS VARCHAR))) AS currency,
        UPPER(TRIM(CAST(source_data.country_iso2 AS VARCHAR))) AS country_iso2,
        UPPER(TRIM(CAST(source_data.country_iso3 AS VARCHAR))) AS country_iso3,
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
            PARTITION BY data_provider, provider_exchange_code
            ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
