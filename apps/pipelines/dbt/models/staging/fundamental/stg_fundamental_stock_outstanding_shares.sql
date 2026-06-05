WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_stock_outstanding_shares') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.provider_instrument_code AS VARCHAR))) AS provider_instrument_code,
        LOWER(TRIM(CAST(source_data.period_type AS VARCHAR))) AS period_type,
        NULLIF(TRIM(CAST(source_data.provider_period_label AS VARCHAR)), '') AS provider_period_label,
        CAST(source_data.period_end_date AS DATE) AS period_end_date,
        CAST(source_data.shares_mln AS DECIMAL(38, 10)) AS shares_mln,
        CAST(source_data.shares AS DECIMAL(38, 10)) AS shares,
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
                period_type,
                period_end_date,
                data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
