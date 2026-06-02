WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_etf_holding') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        UPPER(TRIM(CAST(source_data.holding_symbol AS VARCHAR))) AS holding_symbol,
        NULLIF(UPPER(TRIM(CAST(source_data.holding_code AS VARCHAR))), '') AS holding_code,
        NULLIF(UPPER(TRIM(CAST(source_data.holding_exchange AS VARCHAR))), '') AS holding_exchange,
        NULLIF(TRIM(CAST(source_data.holding_name AS VARCHAR)), '') AS holding_name,
        NULLIF(TRIM(CAST(source_data.sector AS VARCHAR)), '') AS sector,
        NULLIF(TRIM(CAST(source_data.industry AS VARCHAR)), '') AS industry,
        NULLIF(TRIM(CAST(source_data.country AS VARCHAR)), '') AS country,
        NULLIF(TRIM(CAST(source_data.region AS VARCHAR)), '') AS region,
        CAST(source_data.assets_percent AS DECIMAL(38, 10)) AS assets_percent,
        CAST(source_data.is_top_10 AS BOOLEAN) AS is_top_10,
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
            PARTITION BY snapshot_date, ticker, holding_symbol, data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
