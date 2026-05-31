WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_stock_insider_transaction') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        CAST(source_data.provider_position AS BIGINT) AS provider_position,
        CAST(source_data.filing_date AS DATE) AS filing_date,
        NULLIF(TRIM(CAST(source_data.owner_cik AS VARCHAR)), '') AS owner_cik,
        NULLIF(TRIM(CAST(source_data.owner_name AS VARCHAR)), '') AS owner_name,
        CAST(source_data.transaction_date AS DATE) AS transaction_date,
        UPPER(TRIM(CAST(source_data.transaction_code AS VARCHAR))) AS transaction_code,
        CAST(source_data.transaction_amount AS DECIMAL(38, 10)) AS transaction_amount,
        CAST(source_data.transaction_price AS DECIMAL(38, 10)) AS transaction_price,
        UPPER(TRIM(CAST(source_data.transaction_acquired_disposed AS VARCHAR))) AS transaction_acquired_disposed,
        CAST(source_data.post_transaction_amount AS DECIMAL(38, 10)) AS post_transaction_amount,
        NULLIF(TRIM(CAST(source_data.sec_link AS VARCHAR)), '') AS sec_link,
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
                ticker,
                provider_position,
                owner_name,
                transaction_date,
                transaction_code,
                data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
