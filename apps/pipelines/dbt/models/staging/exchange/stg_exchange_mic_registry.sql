WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'exchange_mic_registry') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.mic AS VARCHAR))) AS mic,
        UPPER(TRIM(CAST(source_data.operating_mic AS VARCHAR))) AS operating_mic,
        UPPER(TRIM(CAST(source_data.mic_type AS VARCHAR))) AS mic_type,
        NULLIF(TRIM(CAST(source_data.name AS VARCHAR)), '') AS mic_name,
        NULLIF(TRIM(CAST(source_data.legal_entity_name AS VARCHAR)), '') AS legal_entity_name,
        NULLIF(UPPER(TRIM(CAST(source_data.lei AS VARCHAR))), '') AS lei,
        NULLIF(UPPER(TRIM(CAST(source_data.market_category_code AS VARCHAR))), '') AS market_category_code,
        NULLIF(TRIM(CAST(source_data.acronym AS VARCHAR)), '') AS acronym,
        UPPER(TRIM(CAST(source_data.country_iso2 AS VARCHAR))) AS country_iso2,
        NULLIF(TRIM(CAST(source_data.city AS VARCHAR)), '') AS city,
        NULLIF(TRIM(CAST(source_data.website AS VARCHAR)), '') AS website,
        LOWER(TRIM(CAST(source_data.status AS VARCHAR))) AS status,
        CAST(source_data.creation_date AS DATE) AS creation_date,
        CAST(source_data.last_update_date AS DATE) AS last_update_date,
        CAST(source_data.last_validation_date AS DATE) AS last_validation_date,
        CAST(source_data.expiry_date AS DATE) AS expiry_date,
        NULLIF(TRIM(CAST(source_data.comments AS VARCHAR)), '') AS comments,
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
            PARTITION BY data_provider, mic
            ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT
    * EXCLUDE (row_number),
    mic_type = 'OPRT' AS is_operating_mic,
    status = 'active' AS is_active
FROM deduplicated
WHERE row_number = 1
