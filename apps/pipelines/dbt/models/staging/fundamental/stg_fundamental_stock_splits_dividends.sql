WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_stock_splits_dividends') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        CAST(source_data.forward_annual_dividend_rate AS DECIMAL(38, 10)) AS forward_annual_dividend_rate,
        CAST(source_data.forward_annual_dividend_yield AS DECIMAL(38, 10)) AS forward_annual_dividend_yield,
        CAST(source_data.payout_ratio AS DECIMAL(38, 10)) AS payout_ratio,
        CAST(source_data.dividend_date AS DATE) AS dividend_date,
        CAST(source_data.ex_dividend_date AS DATE) AS ex_dividend_date,
        NULLIF(TRIM(CAST(source_data.last_split_factor AS VARCHAR)), '') AS last_split_factor,
        CAST(source_data.last_split_date AS DATE) AS last_split_date,
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
