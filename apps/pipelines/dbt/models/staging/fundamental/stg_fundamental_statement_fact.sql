WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_statement_fact') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        LOWER(TRIM(CAST(source_data.statement_type AS VARCHAR))) AS statement_type,
        LOWER(TRIM(CAST(source_data.period_type AS VARCHAR))) AS period_type,
        CAST(source_data.period_end_date AS DATE) AS period_end_date,
        CAST(source_data.filing_date AS DATE) AS filing_date,
        NULLIF(TRIM(CAST(source_data.currency_symbol AS VARCHAR)), '') AS currency_symbol,
        NULLIF(TRIM(CAST(source_data.metric_name AS VARCHAR)), '') AS metric_name,
        CAST(source_data.metric_value AS DECIMAL(38, 10)) AS metric_value,
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
                statement_type,
                period_type,
                period_end_date,
                metric_name,
                data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
