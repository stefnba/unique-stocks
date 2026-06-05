WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_stock_holder') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.provider_instrument_code AS VARCHAR))) AS provider_instrument_code,
        LOWER(TRIM(CAST(source_data.holder_type AS VARCHAR))) AS holder_type,
        CAST(source_data.provider_position AS BIGINT) AS provider_position,
        NULLIF(TRIM(CAST(source_data.holder_name AS VARCHAR)), '') AS holder_name,
        CAST(source_data.report_date AS DATE) AS report_date,
        CAST(source_data.total_shares_percent AS DECIMAL(38, 10)) AS total_shares_percent,
        CAST(source_data.total_assets_percent AS DECIMAL(38, 10)) AS total_assets_percent,
        CAST(source_data.current_shares AS DECIMAL(38, 10)) AS current_shares,
        CAST(source_data.shares_change AS DECIMAL(38, 10)) AS shares_change,
        CAST(source_data.shares_change_percent AS DECIMAL(38, 10)) AS shares_change_percent,
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
            PARTITION BY
                snapshot_date,
                provider_exchange_code,
                provider_instrument_code,
                holder_type,
                holder_name,
                report_date,
                data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
