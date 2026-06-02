WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_fund_metric_fact') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.ticker AS VARCHAR))) AS ticker,
        LOWER(TRIM(CAST(source_data.instrument_family AS VARCHAR))) AS instrument_family,
        NULLIF(TRIM(CAST(source_data.metric_group AS VARCHAR)), '') AS metric_group,
        NULLIF(TRIM(CAST(source_data.metric_category AS VARCHAR)), '') AS metric_category,
        NULLIF(TRIM(CAST(source_data.metric_name AS VARCHAR)), '') AS metric_name,
        CAST(source_data.metric_value AS DECIMAL(38, 10)) AS metric_value,
        CAST(source_data.metric_date AS DATE) AS metric_date,
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
                instrument_family,
                metric_group,
                metric_category,
                metric_name,
                data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
