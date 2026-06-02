WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_index_identity') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        UPPER(TRIM(CAST(source_data.code AS VARCHAR))) AS code,
        NULLIF(TRIM(CAST(source_data.name AS VARCHAR)), '') AS index_name,
        NULLIF(TRIM(CAST(source_data.provider_listing_exchange_code AS VARCHAR)), '') AS provider_listing_exchange_code,
        NULLIF(UPPER(TRIM(CAST(source_data.currency_code AS VARCHAR))), '') AS currency_code,
        NULLIF(TRIM(CAST(source_data.currency_name AS VARCHAR)), '') AS currency_name,
        NULLIF(TRIM(CAST(source_data.country_name AS VARCHAR)), '') AS country_name,
        NULLIF(UPPER(TRIM(CAST(source_data.country_iso AS VARCHAR))), '') AS country_iso,
        NULLIF(UPPER(TRIM(CAST(source_data.open_figi AS VARCHAR))), '') AS open_figi,
        CAST(source_data.market_cap AS DECIMAL(38, 10)) AS market_cap,
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
