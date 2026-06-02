WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_stock_shares_stats') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        CAST(source_data.shares_outstanding AS DECIMAL(38, 10)) AS shares_outstanding,
        CAST(source_data.shares_float AS DECIMAL(38, 10)) AS shares_float,
        CAST(source_data.percent_insiders AS DECIMAL(38, 10)) AS percent_insiders,
        CAST(source_data.percent_institutions AS DECIMAL(38, 10)) AS percent_institutions,
        CAST(source_data.shares_short AS DECIMAL(38, 10)) AS shares_short,
        CAST(source_data.shares_short_prior_month AS DECIMAL(38, 10)) AS shares_short_prior_month,
        CAST(source_data.short_ratio AS DECIMAL(38, 10)) AS short_ratio,
        CAST(source_data.short_percent_outstanding AS DECIMAL(38, 10)) AS short_percent_outstanding,
        CAST(source_data.short_percent_float AS DECIMAL(38, 10)) AS short_percent_float,
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
