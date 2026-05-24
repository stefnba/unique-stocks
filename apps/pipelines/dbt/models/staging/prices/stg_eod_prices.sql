WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'eod_prices') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.exchange_code AS VARCHAR) AS exchange_code,
        CAST(source_data.ticker AS VARCHAR) AS ticker,
        CAST(source_data.bar_date AS DATE) AS bar_date,
        CAST(source_data.open AS DECIMAL(18, 6)) AS open_price,
        CAST(source_data.high AS DECIMAL(18, 6)) AS high_price,
        CAST(source_data.low AS DECIMAL(18, 6)) AS low_price,
        CAST(source_data.close AS DECIMAL(18, 6)) AS close_price,
        CAST(source_data.adjusted_close AS DECIMAL(18, 6)) AS adjusted_close_price,
        CAST(source_data.volume AS BIGINT) AS volume,
        CAST(source_data.provider AS VARCHAR) AS provider,
        CAST(source_data.row_hash AS VARCHAR) AS row_hash,
        CAST(source_data.ingested_at AS TIMESTAMPTZ) AS ingested_at
    FROM source AS source_data
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, bar_date, provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
