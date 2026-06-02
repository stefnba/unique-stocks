WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_stock_earnings_fact') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        LOWER(TRIM(CAST(source_data.earnings_section AS VARCHAR))) AS earnings_section,
        NULLIF(LOWER(TRIM(CAST(source_data.period_type AS VARCHAR))), '') AS period_type,
        CAST(source_data.fiscal_period_end AS DATE) AS fiscal_period_end,
        CAST(source_data.report_date AS DATE) AS report_date,
        NULLIF(TRIM(CAST(source_data.before_after_market AS VARCHAR)), '') AS before_after_market,
        NULLIF(UPPER(TRIM(CAST(source_data.currency_code AS VARCHAR))), '') AS currency_code,
        NULLIF(TRIM(CAST(source_data.fiscal_quarter AS VARCHAR)), '') AS fiscal_quarter,
        NULLIF(TRIM(CAST(source_data.period_offset AS VARCHAR)), '') AS period_offset,
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
                earnings_section,
                period_type,
                fiscal_period_end,
                metric_name,
                data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
